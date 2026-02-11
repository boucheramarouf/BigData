import os
import math
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

st.set_page_config(page_title="Apple Platform Analytics", layout="wide")

# --- Postgres config (dans Docker Compose => host = datamart-postgres)
PG_HOST = os.getenv("PG_HOST", "datamart-postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "apple_datamarts")
PG_USER = os.getenv("PG_USER", "datamart")
PG_PASS = os.getenv("PG_PASSWORD", "datamart")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(DB_URL, pool_pre_ping=True)

def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

@st.cache_data(ttl=60)
def load_years_categories():
    df = q(f"""
        SELECT DISTINCT year_event, category
        FROM {PG_SCHEMA}.dm_product_pricing_strategy
        ORDER BY 1,2
    """)
    years = sorted([int(x) for x in df["year_event"].dropna().unique().tolist()])
    cats = sorted([str(x) for x in df["category"].dropna().unique().tolist()])
    return years, cats

st.title("📊 Apple Platform Analytics (Datamarts Postgres)")

years, cats = load_years_categories()

# --- Sidebar filters
st.sidebar.header("Filtres")
year_opt = ["(Toutes)"] + [str(y) for y in years]
cat_opt = ["(Toutes)"] + cats

year_sel = st.sidebar.selectbox("Année (year_event)", options=year_opt, index=0)
cat_sel = st.sidebar.selectbox("Catégorie", options=cat_opt, index=0)

where = []
params = {}

if year_sel != "(Toutes)":
    where.append("year_event = :year_event")
    params["year_event"] = int(year_sel)

if cat_sel != "(Toutes)":
    where.append("category = :category")
    params["category"] = cat_sel

where_sql = ("WHERE " + " AND ".join(where)) if where else ""

# ---------- KPI
c1, c2, c3, c4 = st.columns(4)

kpi_pricing = q(
    f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.dm_product_pricing_strategy {where_sql}",
    params
)["n"][0]

kpi_corr = q(
    f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.dm_product_stock_correlation_yearly {where_sql}",
    params
)["n"][0]

kpi_top = q(
    f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.dm_top_products {where_sql}",
    params
)["n"][0]

kpi_monthly = q(
    f"SELECT COUNT(*) AS n FROM {PG_SCHEMA}.dm_stock_performance_monthly"
)["n"][0]

c1.metric("Rows pricing", int(kpi_pricing))
c2.metric("Rows correlation", int(kpi_corr))
c3.metric("Rows top products", int(kpi_top))
c4.metric("Rows stock monthly", int(kpi_monthly))

st.divider()

# ---------- Graph 1: Bar avg_price by category (par année)
st.subheader("1) 💰 Prix moyen par catégorie (année)")

df1 = q(f"""
    SELECT year_event, category, avg_price, median_price, products_count
    FROM {PG_SCHEMA}.dm_product_pricing_strategy
    {where_sql}
    ORDER BY year_event, avg_price DESC
""", params)

if df1.empty:
    st.warning("Aucune donnée pour ces filtres.")
else:
    # si pas d'année sélectionnée, on affiche la dernière année pour un graphe lisible
    if year_sel == "(Toutes)":
        latest_year = int(df1["year_event"].max())
        df1_plot = df1[df1["year_event"] == latest_year].copy()
        st.caption(f"Année affichée par défaut : {latest_year} (car filtre année non sélectionné)")
    else:
        df1_plot = df1.copy()

    fig1 = px.bar(
        df1_plot,
        x="category",
        y="avg_price",
        hover_data=["median_price", "products_count"],
        title="Avg price par catégorie"
    )
    st.plotly_chart(fig1, use_container_width=True)

# ---------- Graph 2: Time series stock monthly
st.subheader("2) 📈 Performance boursière mensuelle (AAPL)")

df2 = q(f"""
    SELECT year_event, month_event, avg_close, sum_volume, avg_volatility_7d
    FROM {PG_SCHEMA}.dm_stock_performance_monthly
    ORDER BY year_event, month_event
""")

if not df2.empty:
    df2["date"] = pd.to_datetime(
        df2["year_event"].astype(int).astype(str) + "-" +
        df2["month_event"].astype(int).astype(str).str.zfill(2) + "-01"
    )
    fig2 = px.line(
        df2.sort_values("date"),
        x="date",
        y="avg_close",
        hover_data=["sum_volume", "avg_volatility_7d"],
        title="Avg Close (mensuel)"
    )
    st.plotly_chart(fig2, use_container_width=True)

# ---------- Graph 3: Scatter correlation
st.subheader("3) 🔎 Corrélation prix produits vs bourse (annuel)")

df3 = q(f"""
    SELECT year_event, category, avg_price, avg_close_year, avg_volatility_7d_year
    FROM {PG_SCHEMA}.dm_product_stock_correlation_yearly
    {where_sql}
    ORDER BY year_event, avg_price DESC
""", params)

if df3.empty:
    st.warning("Aucune donnée corrélation pour ces filtres.")
else:
    fig3 = px.scatter(
        df3,
        x="avg_close_year",
        y="avg_price",
        color="category",
        hover_data=["year_event", "avg_volatility_7d_year"],
        title="Avg price vs Avg close (yearly)"
    )
    st.plotly_chart(fig3, use_container_width=True)

# ---------- Table Top products
st.subheader("🏆 Top produits (aperçu)")

df4 = q(f"""
    SELECT product_id, category, model_name, release_year, price, price_tier, rating, review_count
    FROM {PG_SCHEMA}.dm_top_products
    {where_sql}
    ORDER BY price DESC
    LIMIT 200
""", params)

st.dataframe(df4, use_container_width=True, height=350)
