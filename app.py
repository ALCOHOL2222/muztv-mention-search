import argparse
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

BASE = "https://muz-tv.ru"
DB_PATH = os.path.join("data", "muztv_mentions.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

def ensure_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS site_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL UNIQUE,
        title TEXT,
        published_at TEXT,
        text TEXT,
        collected_at TEXT NOT NULL
    )
    """)
    conn.commit()
    return conn

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().replace("ё", "е")).strip()

def fetch(url: str, session: requests.Session, timeout: int = 30) -> str:
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_links_from_archive(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        if href.startswith("/"):
            href = urljoin(BASE, href)

        href = href.split("#")[0].split("?")[0].rstrip("/")

        if not href.startswith("https://muz-tv.ru/news/"):
            continue

        if href == "https://muz-tv.ru/news":
            continue

        if re.fullmatch(r"https://muz-tv\.ru/news/\d+", href):
            continue

        if re.fullmatch(r"https://muz-tv\.ru/news/page/\d+", href):
            continue

        links.add(href)

    return sorted(links)

def parse_article(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True)

    if not title:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = og["content"].strip()

    published_at = None

    meta_pub = soup.find("meta", attrs={"property": "article:published_time"})
    if meta_pub and meta_pub.get("content"):
        try:
            published_at = dtparser.parse(meta_pub["content"])
        except Exception:
            published_at = None

    if published_at is None:
        t = soup.find("time")
        if t and t.get("datetime"):
            try:
                published_at = dtparser.parse(t["datetime"])
            except Exception:
                published_at = None

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()

    return {
        "url": url,
        "title": title,
        "published_at": published_at.isoformat() if published_at else None,
        "text": text,
    }

def save_article(conn, article: dict) -> int:
    cur = conn.execute("""
    INSERT OR IGNORE INTO site_articles (url, title, published_at, text, collected_at)
    VALUES (?, ?, ?, ?, ?)
    """, (
        article["url"],
        article["title"],
        article["published_at"],
        article["text"],
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    return 1 if cur.rowcount == 1 else 0

def get_coverage(conn):
    row = conn.execute("""
        SELECT COUNT(*), MIN(published_at), MAX(published_at)
        FROM site_articles
        WHERE published_at IS NOT NULL
    """).fetchone()
    return {
        "count": int(row[0] or 0),
        "min_published_at": row[1],
        "max_published_at": row[2],
    }

def run_collect(max_pages: int, date_from_raw: str | None = None):
    conn = ensure_db()
    session = requests.Session()
    seen_links = set()
    processed_links = 0
    inserted_rows = 0

    cutoff = None
    if date_from_raw:
        cutoff = datetime.fromisoformat(date_from_raw).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        print(f"Собираю архив до даты: {cutoff.isoformat()}")

    for i in range(1, max_pages + 1):
        page_url = "https://muz-tv.ru/news/" if i == 1 else f"https://muz-tv.ru/news/{i}/"
        try:
            html = fetch(page_url, session)
        except Exception as e:
            print(f"ОШИБКА archive {page_url}: {e}")
            continue

        links = extract_links_from_archive(html)
        print(f"{page_url} -> {len(links)} ссылок")

        if not links:
            continue

        page_oldest_pub = None
        page_new_links = 0

        for link in links:
            if link in seen_links:
                continue
            seen_links.add(link)
            page_new_links += 1

            try:
                article_html = fetch(link, session)
                article = parse_article(link, article_html)
                processed_links += 1

                if article["published_at"]:
                    try:
                        pub_dt = dtparser.parse(article["published_at"]).astimezone(timezone.utc)
                        if page_oldest_pub is None or pub_dt < page_oldest_pub:
                            page_oldest_pub = pub_dt
                    except Exception:
                        pass

                inserted_rows += save_article(conn, article)

                if processed_links % 25 == 0:
                    print(f"Обработано статей: {processed_links}")

                time.sleep(0.15)
            except Exception as e:
                print(f"ОШИБКА article {link}: {e}")

        if page_oldest_pub is not None:
            print(f"Самая старая дата на странице: {page_oldest_pub.isoformat()}")

        if cutoff and page_oldest_pub and page_oldest_pub < cutoff:
            print("Достигли нужной глубины по дате. Останавливаю сбор.")
            break

        if page_new_links == 0:
            print("На странице не было новых ссылок.")

        time.sleep(0.2)

    coverage = get_coverage(conn)
    print("Готово.")
    print(f"Всего обработано ссылок в этом запуске: {processed_links}")
    print(f"Новых статей добавлено: {inserted_rows}")
    print(f"Статей в базе всего: {coverage['count']}")
    print(f"Покрытие базы: {coverage['min_published_at']} -> {coverage['max_published_at']}")

def run_search(artist_name: str, aliases_raw: str, date_from_raw: str, date_to_raw: str):
    conn = ensure_db()

    aliases = [artist_name] + [x.strip() for x in aliases_raw.split(",") if x.strip()]
    aliases_norm = [norm(x) for x in aliases if x.strip()]

    date_from = datetime.fromisoformat(date_from_raw).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    date_to = datetime.fromisoformat(date_to_raw).replace(
        hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc
    )

    df = pd.read_sql_query("""
        SELECT url, title, published_at, text
        FROM site_articles
        WHERE published_at IS NOT NULL
    """, conn)

    if df.empty:
        print("В базе пока нет статей.")
        return

    matched_rows = []

    for _, row in df.iterrows():
        try:
            pub = dtparser.parse(str(row["published_at"])).astimezone(timezone.utc)
        except Exception:
            continue

        if pub < date_from or pub > date_to:
            continue

        haystack = norm(f'{row["title"] or ""} {row["text"] or ""}')

        matched = None
        for alias, alias_norm in zip(aliases, aliases_norm):
            if alias_norm and alias_norm in haystack:
                matched = alias
                break

        if not matched:
            continue

        matched_rows.append({
            "source": "muztv_site",
            "artist_name": artist_name,
            "matched_alias": matched,
            "title": row["title"],
            "url": row["url"],
            "published_at": pub.isoformat(),
            "mentions_count": 1,
        })

    out_df = pd.DataFrame(matched_rows)
    os.makedirs("output", exist_ok=True)

    if out_df.empty:
        print("Совпадений не найдено.")
        empty_df = pd.DataFrame(columns=[
            "source", "artist_name", "matched_alias", "title", "url", "published_at", "mentions_count"
        ])
        empty_df.to_excel("output/muztv_site_mentions.xlsx", index=False)
        empty_df.to_csv("output/muztv_site_mentions.csv", index=False, encoding="utf-8-sig")
        return

    out_df = out_df.sort_values(by=["published_at", "title"], ascending=[False, True])
    out_df.to_excel("output/muztv_site_mentions.xlsx", index=False)
    out_df.to_csv("output/muztv_site_mentions.csv", index=False, encoding="utf-8-sig")

    links_df = out_df[["source", "artist_name", "title", "published_at", "url"]].copy()
    links_df.to_excel("output/muztv_site_links.xlsx", index=False)
    links_df.to_csv("output/muztv_site_links.csv", index=False, encoding="utf-8-sig")

    print(f"Найдено материалов: {len(out_df)}")
    print("Файлы сохранены в папку output/")

def run_stats():
    conn = ensure_db()
    coverage = get_coverage(conn)
    print(f"ARTICLES_IN_DB = {coverage['count']}")
    print(f"MIN_PUBLISHED_AT = {coverage['min_published_at']}")
    print(f"MAX_PUBLISHED_AT = {coverage['max_published_at']}")

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--pages", type=int, default=100)
    collect_parser.add_argument("--date-from", default=None)

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("--artist", required=True)
    search_parser.add_argument("--aliases", default="")
    search_parser.add_argument("--date-from", required=True)
    search_parser.add_argument("--date-to", required=True)

    subparsers.add_parser("stats")

    args = parser.parse_args()

    if args.command == "collect":
        run_collect(args.pages, args.date_from)
    elif args.command == "search":
        run_search(args.artist, args.aliases, args.date_from, args.date_to)
    elif args.command == "stats":
        run_stats()

if __name__ == "__main__":
    main()
