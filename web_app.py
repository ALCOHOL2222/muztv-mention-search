import io
import re
import sqlite3
from datetime import datetime, time, timezone

import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

DB_PATH = "data/muztv_mentions.db"

st.set_page_config(page_title="MUZ-TV Mention Search", page_icon="🎵", layout="wide")

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().replace("ё", "е")).strip()

@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data(ttl=30)
def load_articles():
    conn = get_conn()
    return pd.read_sql_query("""
        SELECT url, title, published_at, text
        FROM site_articles
        WHERE published_at IS NOT NULL
        ORDER BY published_at DESC
    """, conn)

@st.cache_data(ttl=30)
def load_coverage():
    conn = get_conn()
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

def search_articles(artist_name: str, aliases_raw: str, date_from, date_to):
    df = load_articles()

    if df.empty:
        return pd.DataFrame(columns=[
            "source", "artist_name", "matched_alias", "title", "url", "published_at", "mentions_count"
        ])

    aliases = [artist_name] + [x.strip() for x in aliases_raw.split(",") if x.strip()]
    aliases_norm = [norm(x) for x in aliases if x.strip()]

    date_from_dt = datetime.combine(date_from, time.min).replace(tzinfo=timezone.utc)
    date_to_dt = datetime.combine(date_to, time.max).replace(tzinfo=timezone.utc)

    matched_rows = []

    for _, row in df.iterrows():
        try:
            pub = dtparser.parse(str(row["published_at"])).astimezone(timezone.utc)
        except Exception:
            continue

        if pub < date_from_dt or pub > date_to_dt:
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

    if out_df.empty:
        return pd.DataFrame(columns=[
            "source", "artist_name", "matched_alias", "title", "url", "published_at", "mentions_count"
        ])

    return out_df.sort_values(by=["published_at", "title"], ascending=[False, True]).reset_index(drop=True)

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")
    return output.getvalue()

coverage = load_coverage()

st.title("🎵 MUZ-TV Mention Search")
st.caption("Поиск по уже собранной базе материалов сайта MUZ-TV")

with st.sidebar:
    st.subheader("Статус базы")
    st.metric("Статей в базе", coverage["count"])
    st.write("Покрытие базы:")
    st.write(f"от: {coverage['min_published_at'] or '—'}")
    st.write(f"до: {coverage['max_published_at'] or '—'}")
    st.info("Если нужный период не попадает в покрытие базы, сначала нужно дозагрузить архив.")

default_from = datetime(2025, 1, 1).date()
default_to = datetime.now().date()

with st.form("search_form"):
    c1, c2 = st.columns(2)

    with c1:
        artist_name = st.text_input("Имя артиста", placeholder="Например: Дима Билан")
        aliases = st.text_input("Алиасы через запятую", placeholder="Билан,Dima Bilan,Bilan")

    with c2:
        date_from = st.date_input("Дата с", value=default_from)
        date_to = st.date_input("Дата по", value=default_to)

    submitted = st.form_submit_button("Найти материалы")

if submitted:
    if not artist_name.strip():
        st.error("Введите имя артиста.")
    elif date_from > date_to:
        st.error("Дата 'с' не может быть позже даты 'по'.")
    else:
        cov_min = None
        try:
            if coverage["min_published_at"]:
                cov_min = dtparser.parse(coverage["min_published_at"]).date()
        except Exception:
            pass

        if cov_min and date_from < cov_min:
            st.warning(f"Внимание: база сейчас покрывает период только с {cov_min}. Для более ранних дат сначала нужно дозагрузить архив.")

        with st.spinner("Ищу материалы..."):
            results = search_articles(artist_name.strip(), aliases.strip(), date_from, date_to)

        st.subheader("Результат")
        st.metric("Найдено материалов", len(results))

        if results.empty:
            st.warning("Совпадений не найдено.")
        else:
            st.dataframe(
                results,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "url": st.column_config.LinkColumn("Ссылка"),
                },
            )

            csv_bytes = df_to_csv_bytes(results)
            xlsx_bytes = df_to_excel_bytes(results)

            d1, d2 = st.columns(2)
            with d1:
                st.download_button(
                    "Скачать CSV",
                    data=csv_bytes,
                    file_name="muztv_site_mentions.csv",
                    mime="text/csv"
                )
            with d2:
                st.download_button(
                    "Скачать XLSX",
                    data=xlsx_bytes,
                    file_name="muztv_site_mentions.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
