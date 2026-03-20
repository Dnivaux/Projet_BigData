import os

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DATAMART_BASE_PATH = os.getenv("DATAMART_BASE_PATH", "./data/datamart")

st.set_page_config(page_title="Fashion Medallion Dashboard", layout="wide")
st.title("Fashion Recommendations - Datamarts Dashboard")


@st.cache_data
def read_datamart(name: str) -> pd.DataFrame:
    path = f"{DATAMART_BASE_PATH}/{name}/**/*.parquet"
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    finally:
        con.close()


col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Top articles hebdo")
    try:
        df_top = read_datamart("dm_top_weekly_articles")
        chart = (
            alt.Chart(df_top.head(500))
            .mark_bar()
            .encode(
                x=alt.X("units_sold:Q", title="Unites vendues"),
                y=alt.Y("article_id:N", sort="-x", title="Article"),
                color=alt.Color("department_name:N", title="Departement"),
                tooltip=["week_start", "article_id", "units_sold", "department_name"],
            )
            .properties(height=420)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as exc:
        st.error(f"Impossible de charger dm_top_weekly_articles: {exc}")

with col2:
    st.subheader("Revenus mensuels par canal")
    try:
        df_channel = read_datamart("dm_sales_channel_monthly")
        chart = (
            alt.Chart(df_channel)
            .mark_line(point=True)
            .encode(
                x=alt.X("month_start:T", title="Mois"),
                y=alt.Y("revenue:Q", title="Revenu"),
                color=alt.Color("sales_channel_id:N", title="Canal"),
                tooltip=["month_start", "sales_channel_id", "revenue", "units_sold"],
            )
            .properties(height=420)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as exc:
        st.error(f"Impossible de charger dm_sales_channel_monthly: {exc}")

with col3:
    st.subheader("Segments RFM")
    try:
        df_rfm = read_datamart("dm_customer_rfm")
        seg = df_rfm["rfm_segment"].value_counts().reset_index()
        seg.columns = ["rfm_segment", "customers"]
        chart = (
            alt.Chart(seg.head(30))
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("customers:Q"),
                color=alt.Color("rfm_segment:N"),
                tooltip=["rfm_segment", "customers"],
            )
            .properties(height=420)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as exc:
        st.error(f"Impossible de charger dm_customer_rfm: {exc}")
