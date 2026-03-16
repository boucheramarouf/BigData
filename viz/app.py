import os
import numpy as np
import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Apple Strategy Intelligence",
    page_icon="🍎",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS PREMIUM / EXECUTIVE LOOK
# ============================================================
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
            max-width: 1500px;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b1020 0%, #111827 100%);
            border-right: 1px solid rgba(255,255,255,0.06);
        }

        [data-testid="stSidebar"] * {
            color: #f3f4f6 !important;
        }

        .hero-card {
            padding: 1.4rem 1.6rem;
            border-radius: 20px;
            background:
                radial-gradient(circle at top right, rgba(255,255,255,0.10), transparent 25%),
                linear-gradient(135deg, #0f172a 0%, #111827 50%, #1f2937 100%);
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 12px 30px rgba(0,0,0,0.18);
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 2rem;
            font-weight: 800;
            color: #ffffff;
            margin-bottom: 0.3rem;
            letter-spacing: -0.02em;
        }

        .hero-subtitle {
            font-size: 1rem;
            color: #d1d5db;
            margin-bottom: 0.8rem;
        }

        .section-title {
            font-size: 1.1rem;
            font-weight: 700;
            margin-top: 0.4rem;
            margin-bottom: 0.8rem;
        }

        .insight-box {
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid rgba(15,23,42,0.08);
            border-left: 5px solid #111827;
            border-radius: 16px;
            padding: 1rem 1rem;
            margin-bottom: 0.8rem;
            box-shadow: 0 6px 18px rgba(15,23,42,0.05);
        }

        .insight-title {
            font-weight: 700;
            font-size: 0.98rem;
            margin-bottom: 0.2rem;
            color: #0f172a;
        }

        .insight-text {
            color: #334155;
            font-size: 0.94rem;
            line-height: 1.5;
        }

        .tiny-muted {
            color: #64748b;
            font-size: 0.85rem;
        }

        .chip {
            display: inline-block;
            padding: 0.35rem 0.7rem;
            border-radius: 999px;
            background: rgba(255,255,255,0.08);
            color: #f8fafc;
            font-size: 0.84rem;
            margin-right: 0.35rem;
            margin-top: 0.3rem;
            border: 1px solid rgba(255,255,255,0.08);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# DB CONFIG
# ============================================================
PG_HOST = os.getenv("PG_HOST", "datamart-postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "apple_datamarts")
PG_USER = os.getenv("PG_USER", "datamart")
PG_PASS = os.getenv("PG_PASSWORD", "datamart")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


engine = get_engine()


def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=120)
def load_years_categories():
    df = q(
        f"""
        SELECT DISTINCT year_event, category
        FROM {PG_SCHEMA}.dm_product_pricing_strategy
        ORDER BY 1, 2
        """
    )
    years = sorted([int(x) for x in df["year_event"].dropna().unique().tolist()])
    cats = sorted([str(x) for x in df["category"].dropna().unique().tolist()])
    return years, cats


# ============================================================
# PLOTLY THEME
# ============================================================
PALETTE = {
    "bg": "#ffffff",
    "text": "#0f172a",
    "muted": "#64748b",
    "grid": "rgba(15,23,42,0.08)",
    "line": "#111827",
    "accent": "#2563eb",
    "accent_2": "#7c3aed",
    "accent_3": "#14b8a6",
    "accent_4": "#f59e0b",
    "danger": "#dc2626",
    "success": "#16a34a",
}

CATEGORY_COLORS = {
    "iPhone": "#111827",
    "iPad": "#2563eb",
    "MacBook": "#7c3aed",
    "iMac": "#14b8a6",
    "Apple Watch": "#f59e0b",
    "AirPods": "#ec4899",
}

TIER_ORDER = ["Budget", "Mid-range", "Premium", "Luxury"]


def apply_fig_style(fig, title=None):
    fig.update_layout(
        title=title,
        paper_bgcolor=PALETTE["bg"],
        plot_bgcolor=PALETTE["bg"],
        font=dict(color=PALETTE["text"]),
        margin=dict(l=30, r=30, t=70, b=30),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(0,0,0,0)"
        ),
    )
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        linecolor=PALETTE["grid"],
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=PALETTE["grid"],
        zeroline=False,
    )
    return fig


def format_currency(x):
    if pd.isna(x):
        return "—"
    return f"${x:,.0f}"


def format_pct(x):
    if pd.isna(x):
        return "—"
    return f"{x:.1f}%"


def safe_corr(df: pd.DataFrame, x_col: str, y_col: str):
    if df.empty:
        return np.nan
    clean = df[[x_col, y_col]].dropna()
    if len(clean) < 2:
        return np.nan
    return clean[x_col].corr(clean[y_col])


# ============================================================
# SIDEBAR
# ============================================================
years, cats = load_years_categories()

st.sidebar.markdown("## 🍎 Apple Strategy Intelligence")
st.sidebar.markdown(
    """
    <div class="tiny-muted">
    Executive dashboard for analyzing the relationship between Apple product pricing,
    premiumization strategy and AAPL market performance.
    </div>
    """,
    unsafe_allow_html=True,
)

year_opt = ["Toutes"] + [str(y) for y in years]
cat_opt = ["Toutes"] + cats

selected_year = st.sidebar.selectbox("Année", options=year_opt, index=0)
selected_cat = st.sidebar.selectbox("Catégorie", options=cat_opt, index=0)

st.sidebar.markdown("---")
show_raw = st.sidebar.toggle("Afficher explorer data", value=False)
show_annotations = st.sidebar.toggle("Afficher insights automatiques", value=True)

where = []
where_top = []
params = {}

if selected_year != "Toutes":
    where.append("year_event = :year_event")
    where_top.append("release_year = :year_event")
    params["year_event"] = int(selected_year)

if selected_cat != "Toutes":
    where.append("category = :category")
    where_top.append("category = :category")
    params["category"] = selected_cat

where_sql = f"WHERE {' AND '.join(where)}" if where else ""
where_sql_top = f"WHERE {' AND '.join(where_top)}" if where_top else ""

# ============================================================
# DATA LOAD
# ============================================================
@st.cache_data(ttl=120)
def load_pricing(where_sql: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    return q(
        f"""
        SELECT year_event, category, avg_price, median_price, premium_ratio, products_count
        FROM {PG_SCHEMA}.dm_product_pricing_strategy
        {where_sql}
        ORDER BY year_event, category
        """,
        params_local,
    )


@st.cache_data(ttl=120)
def load_stock_monthly():
    return q(
        f"""
        SELECT year_event, month_event, avg_close, sum_volume, avg_volatility_7d
        FROM {PG_SCHEMA}.dm_stock_performance_monthly
        ORDER BY year_event, month_event
        """
    )


@st.cache_data(ttl=120)
def load_corr(where_sql: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    return q(
        f"""
        SELECT year_event, category, avg_price, avg_close_year, avg_volatility_7d_year
        FROM {PG_SCHEMA}.dm_product_stock_correlation_yearly
        {where_sql}
        ORDER BY year_event, category
        """,
        params_local,
    )


@st.cache_data(ttl=120)
def load_top(where_sql_top: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    return q(
        f"""
        SELECT product_id, category, model_name, release_year, price, price_tier, rating, review_count
        FROM {PG_SCHEMA}.dm_top_products
        {where_sql_top}
        ORDER BY price DESC
        LIMIT 200
        """,
        params_local,
    )


pricing = load_pricing(where_sql, tuple(params.items()))
stock_monthly = load_stock_monthly()
corr_df = load_corr(where_sql, tuple(params.items()))
top_df = load_top(where_sql_top, tuple(params.items()))

if not stock_monthly.empty:
    stock_monthly["date"] = pd.to_datetime(
        stock_monthly["year_event"].astype(int).astype(str)
        + "-"
        + stock_monthly["month_event"].astype(int).astype(str).str.zfill(2)
        + "-01"
    )

# ============================================================
# DERIVED DATA
# ============================================================
latest_year = None if pricing.empty else int(pricing["year_event"].max())
previous_year = None if pricing.empty else (
    int(sorted(pricing["year_event"].unique())[-2]) if pricing["year_event"].nunique() > 1 else None
)

current_pricing = pricing[pricing["year_event"] == latest_year].copy() if latest_year is not None else pd.DataFrame()
prev_pricing = pricing[pricing["year_event"] == previous_year].copy() if previous_year is not None else pd.DataFrame()

avg_price_kpi = current_pricing["avg_price"].mean() if not current_pricing.empty else np.nan
premium_ratio_kpi = current_pricing["premium_ratio"].mean() if "premium_ratio" in current_pricing.columns and not current_pricing.empty else np.nan

if latest_year is not None and not corr_df.empty:
    current_corr_slice = corr_df[corr_df["year_event"] == latest_year].copy()
else:
    current_corr_slice = pd.DataFrame()

corr_kpi = safe_corr(corr_df, "avg_price", "avg_close_year")
stock_kpi = stock_monthly["avg_close"].iloc[-1] if not stock_monthly.empty else np.nan

prev_avg_price = prev_pricing["avg_price"].mean() if not prev_pricing.empty else np.nan
avg_price_delta_pct = (
    ((avg_price_kpi - prev_avg_price) / prev_avg_price) * 100
    if pd.notna(avg_price_kpi) and pd.notna(prev_avg_price) and prev_avg_price != 0
    else np.nan
)

# mix pricing tiers from top products
tier_mix = pd.DataFrame()
if not top_df.empty and "price_tier" in top_df.columns:
    tier_mix = top_df.copy()
    tier_mix["price_tier"] = pd.Categorical(tier_mix["price_tier"], categories=TIER_ORDER, ordered=True)
    tier_mix = (
        tier_mix.groupby(["category", "price_tier"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["category", "price_tier"])
    )

# ============================================================
# HERO
# ============================================================
st.markdown(
    f"""
    <div class="hero-card">
        <div class="hero-title">Apple Strategy Intelligence Dashboard</div>
        <div class="hero-subtitle">
            Visual analysis of Apple pricing strategy, premiumization and stock-market behavior.
            Built for executive storytelling and recruiter-grade presentation.
        </div>
        <span class="chip">Medallion Architecture</span>
        <span class="chip">PostgreSQL Datamarts</span>
        <span class="chip">Streamlit + Plotly</span>
        <span class="chip">Business Storytelling</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# KPI ROW
# ============================================================
k1, k2, k3, k4 = st.columns(4, gap="large")

with k1:
    st.metric(
        "Average product price",
        format_currency(avg_price_kpi),
        None if pd.isna(avg_price_delta_pct) else f"{avg_price_delta_pct:+.1f}% vs prev year",
        border=True,
    )

with k2:
    st.metric(
        "Premium ratio",
        format_pct(premium_ratio_kpi),
        border=True,
        help="Part moyenne des produits premium sur le périmètre sélectionné.",
    )

with k3:
    st.metric(
        "AAPL monthly avg close",
        format_currency(stock_kpi),
        border=True,
    )

with k4:
    corr_label = "—" if pd.isna(corr_kpi) else f"{corr_kpi:.2f}"
    corr_delta = None
    if pd.notna(corr_kpi):
        corr_delta = "Positive linkage" if corr_kpi >= 0.5 else "Moderate linkage" if corr_kpi >= 0.2 else "Weak linkage"
    st.metric(
        "Pricing / stock correlation",
        corr_label,
        corr_delta,
        border=True,
    )

st.markdown("")

# ============================================================
# AUTO INSIGHTS
# ============================================================
if show_annotations:
    c_left, c_right = st.columns([1.5, 1], gap="large")

    with c_left:
        st.markdown('<div class="section-title">Executive summary</div>', unsafe_allow_html=True)

        dominant_category = None
        if not current_pricing.empty:
            top_cat_row = current_pricing.sort_values("avg_price", ascending=False).iloc[0]
            dominant_category = f"{top_cat_row['category']} ({format_currency(top_cat_row['avg_price'])})"

        insight_1 = (
            f"En {latest_year}, le prix moyen observé est de {format_currency(avg_price_kpi)} "
            f"avec un ratio premium de {format_pct(premium_ratio_kpi)}."
            if latest_year is not None and pd.notna(avg_price_kpi)
            else "Données insuffisantes pour calculer le résumé exécutif."
        )
        insight_2 = (
            f"La catégorie la plus haut de gamme sur le périmètre courant est {dominant_category}."
            if dominant_category
            else "Impossible d’identifier la catégorie la plus premium."
        )
        insight_3 = (
            f"La corrélation globale estimée entre le pricing et le niveau boursier est de {corr_kpi:.2f}, "
            f"ce qui suggère une relation {'forte' if corr_kpi >= 0.5 else 'modérée' if corr_kpi >= 0.2 else 'faible'}."
            if pd.notna(corr_kpi)
            else "La corrélation n’est pas calculable sur ce périmètre."
        )

        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">What matters for the business</div>
                <div class="insight-text">{insight_1}<br><br>{insight_2}<br><br>{insight_3}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c_right:
        st.markdown('<div class="section-title">Recruiter takeaway</div>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="insight-box">
                <div class="insight-title">Why this dashboard looks senior</div>
                <div class="insight-text">
                    It does not just display charts. It frames a business question,
                    highlights KPIs, quantifies the premiumization strategy,
                    and links product signals to market performance in a clear executive narrative.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "Overview",
        "Pricing Strategy",
        "Stock & Correlation",
        "Top Products",
    ]
)

# ============================================================
# TAB 1 - OVERVIEW
# ============================================================
with tab1:
    left, right = st.columns([1.4, 1], gap="large")

    with left:
        st.markdown("### Price evolution by category")
        if pricing.empty:
            st.warning("Aucune donnée disponible pour les filtres sélectionnés.")
        else:
            fig = go.Figure()
            for cat in pricing["category"].dropna().unique():
                d = pricing[pricing["category"] == cat].sort_values("year_event")
                fig.add_trace(
                    go.Scatter(
                        x=d["year_event"],
                        y=d["avg_price"],
                        mode="lines+markers",
                        name=cat,
                        line=dict(width=3, color=CATEGORY_COLORS.get(cat, PALETTE["accent"])),
                        marker=dict(size=8),
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "Year: %{x}<br>"
                            "Avg price: $%{y:,.0f}<br>"
                            "Premium ratio: %{customdata[0]:.1f}%<br>"
                            "Products: %{customdata[1]:,.0f}<extra></extra>"
                        ),
                        customdata=d[["premium_ratio", "products_count"]].to_numpy(),
                    )
                )
            apply_fig_style(fig, "Pricing trajectory by product category")
            fig.update_xaxes(dtick=1)
            fig.update_yaxes(title="Average price ($)")
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("### Pricing heatmap")
        if pricing.empty:
            st.info("Pas de données.")
        else:
            heat = pricing.pivot_table(
                index="category",
                columns="year_event",
                values="avg_price",
                aggfunc="mean"
            )
            fig_h = px.imshow(
                heat,
                aspect="auto",
                text_auto=".0f",
                color_continuous_scale="Blues",
            )
            apply_fig_style(fig_h, "Average price intensity")
            fig_h.update_traces(
                hovertemplate="Category: %{y}<br>Year: %{x}<br>Avg price: $%{z:,.0f}<extra></extra>"
            )
            st.plotly_chart(fig_h, use_container_width=True)

# ============================================================
# TAB 2 - PRICING STRATEGY
# ============================================================
with tab2:
    left, right = st.columns([1.2, 1], gap="large")

    with left:
        st.markdown("### Premiumization vs average price")
        if pricing.empty:
            st.warning("Aucune donnée.")
        else:
            annual = (
                pricing.groupby("year_event", as_index=False)
                .agg(avg_price=("avg_price", "mean"), premium_ratio=("premium_ratio", "mean"))
                .sort_values("year_event")
            )

            fig_combo = make_subplots(specs=[[{"secondary_y": True}]])
            fig_combo.add_trace(
                go.Bar(
                    x=annual["year_event"],
                    y=annual["premium_ratio"],
                    name="Premium ratio",
                    marker_color=PALETTE["accent"],
                    hovertemplate="Year: %{x}<br>Premium ratio: %{y:.1f}%<extra></extra>",
                ),
                secondary_y=False,
            )
            fig_combo.add_trace(
                go.Scatter(
                    x=annual["year_event"],
                    y=annual["avg_price"],
                    name="Average price",
                    mode="lines+markers",
                    line=dict(width=3, color=PALETTE["line"]),
                    hovertemplate="Year: %{x}<br>Average price: $%{y:,.0f}<extra></extra>",
                ),
                secondary_y=True,
            )
            apply_fig_style(fig_combo, "Premiumization trend and pricing level")
            fig_combo.update_yaxes(title_text="Premium ratio (%)", secondary_y=False)
            fig_combo.update_yaxes(title_text="Average price ($)", secondary_y=True)
            fig_combo.update_xaxes(dtick=1)
            st.plotly_chart(fig_combo, use_container_width=True)

    with right:
        st.markdown("### Product mix by price tier")
        if tier_mix.empty:
            st.info("Aucun price_tier disponible dans dm_top_products.")
        else:
            fig_mix = px.bar(
                tier_mix,
                x="category",
                y="count",
                color="price_tier",
                barmode="stack",
                category_orders={"price_tier": TIER_ORDER},
                color_discrete_map={
                    "Budget": "#cbd5e1",
                    "Mid-range": "#93c5fd",
                    "Premium": "#2563eb",
                    "Luxury": "#111827",
                },
            )
            apply_fig_style(fig_mix, "Distribution of visible products by price tier")
            fig_mix.update_traces(
                hovertemplate="Category: %{x}<br>Tier: %{fullData.name}<br>Count: %{y}<extra></extra>"
            )
            st.plotly_chart(fig_mix, use_container_width=True)

# ============================================================
# TAB 3 - STOCK & CORRELATION
# ============================================================
with tab3:
    top_left, top_right = st.columns([1.2, 1], gap="large")

    with top_left:
        st.markdown("### AAPL stock momentum")
        if stock_monthly.empty:
            st.warning("Aucune donnée boursière.")
        else:
            fig_stock = go.Figure()

            fig_stock.add_trace(
                go.Scatter(
                    x=stock_monthly["date"],
                    y=stock_monthly["avg_close"],
                    mode="lines",
                    name="Avg close",
                    line=dict(width=3, color=PALETTE["line"]),
                    hovertemplate=(
                        "Month: %{x|%b %Y}<br>"
                        "Avg close: $%{y:,.2f}<br>"
                        "Volume: %{customdata[0]:,.0f}<br>"
                        "Volatility 7d: %{customdata[1]:.2f}<extra></extra>"
                    ),
                    customdata=stock_monthly[["sum_volume", "avg_volatility_7d"]].to_numpy(),
                )
            )

            apply_fig_style(fig_stock, "Monthly stock performance (AAPL)")
            fig_stock.update_yaxes(title="Average close ($)")
            st.plotly_chart(fig_stock, use_container_width=True)

    with top_right:
        st.markdown("### Pricing vs stock level")
        if corr_df.empty:
            st.warning("Aucune donnée de corrélation.")
        else:
            fig_scatter = px.scatter(
                corr_df,
                x="avg_close_year",
                y="avg_price",
                color="category",
                size_max=18,
                color_discrete_map=CATEGORY_COLORS,
            )
            fig_scatter.update_traces(
                marker=dict(size=14, opacity=0.82, line=dict(width=1, color="white")),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Avg close year: $%{x:,.2f}<br>"
                    "Avg product price: $%{y:,.0f}<br>"
                    "Year: %{customdata[0]}<br>"
                    "Volatility: %{customdata[1]:.2f}<extra></extra>"
                ),
                customdata=corr_df[["year_event", "avg_volatility_7d_year"]].to_numpy(),
            )
            apply_fig_style(fig_scatter, "Relationship between pricing and market valuation")
            fig_scatter.update_xaxes(title="Average yearly close ($)")
            fig_scatter.update_yaxes(title="Average product price ($)")
            st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("### Multi-metric yearly storyline")
    if pricing.empty or corr_df.empty:
        st.info("Données insuffisantes pour le storyline annuel.")
    else:
        annual_price = (
            pricing.groupby("year_event", as_index=False)
            .agg(avg_price=("avg_price", "mean"), premium_ratio=("premium_ratio", "mean"))
        )
        annual_stock = (
            corr_df.groupby("year_event", as_index=False)
            .agg(avg_close_year=("avg_close_year", "mean"))
        )
        annual_story = annual_price.merge(annual_stock, on="year_event", how="left").sort_values("year_event")

        fig_story = make_subplots(specs=[[{"secondary_y": True}]])
        fig_story.add_trace(
            go.Bar(
                x=annual_story["year_event"],
                y=annual_story["premium_ratio"],
                name="Premium ratio",
                marker_color=PALETTE["accent_3"],
                hovertemplate="Year: %{x}<br>Premium ratio: %{y:.1f}%<extra></extra>",
            ),
            secondary_y=False,
        )
        fig_story.add_trace(
            go.Scatter(
                x=annual_story["year_event"],
                y=annual_story["avg_price"],
                name="Average price",
                mode="lines+markers",
                line=dict(width=3, color=PALETTE["accent_2"]),
                hovertemplate="Year: %{x}<br>Average price: $%{y:,.0f}<extra></extra>",
            ),
            secondary_y=True,
        )
        fig_story.add_trace(
            go.Scatter(
                x=annual_story["year_event"],
                y=annual_story["avg_close_year"],
                name="Avg close year",
                mode="lines+markers",
                line=dict(width=3, dash="dot", color=PALETTE["line"]),
                hovertemplate="Year: %{x}<br>Avg close year: $%{y:,.2f}<extra></extra>",
            ),
            secondary_y=True,
        )
        apply_fig_style(fig_story, "Premium mix, pricing and stock market storyline")
        fig_story.update_yaxes(title_text="Premium ratio (%)", secondary_y=False)
        fig_story.update_yaxes(title_text="Price / Stock ($)", secondary_y=True)
        fig_story.update_xaxes(dtick=1)
        st.plotly_chart(fig_story, use_container_width=True)

# ============================================================
# TAB 4 - TOP PRODUCTS
# ============================================================
with tab4:
    left, right = st.columns([1.1, 1], gap="large")

    with left:
        st.markdown("### Most expensive visible products")
        if top_df.empty:
            st.warning("Aucun top produit sur ce filtre.")
        else:
            top10 = top_df.nlargest(10, "price").copy()
            top10["label"] = top10["model_name"].fillna(top10["product_id"].astype(str))

            fig_top = px.bar(
                top10.sort_values("price"),
                x="price",
                y="label",
                orientation="h",
                color="category",
                color_discrete_map=CATEGORY_COLORS,
            )
            fig_top.update_traces(
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Price: $%{x:,.0f}<br>"
                    "Category: %{customdata[0]}<br>"
                    "Tier: %{customdata[1]}<br>"
                    "Rating: %{customdata[2]:.2f}<extra></extra>"
                ),
                customdata=top10[["category", "price_tier", "rating"]].to_numpy(),
            )
            apply_fig_style(fig_top, "Top 10 highest-priced products")
            fig_top.update_xaxes(title="Price ($)")
            fig_top.update_yaxes(title="")
            st.plotly_chart(fig_top, use_container_width=True)

    with right:
        st.markdown("### Product quality snapshot")
        if top_df.empty or "rating" not in top_df.columns:
            st.info("Pas de ratings disponibles.")
        else:
            rating_view = top_df.groupby("category", as_index=False).agg(
                avg_rating=("rating", "mean"),
                avg_reviews=("review_count", "mean"),
                avg_price=("price", "mean"),
            )
            fig_rating = px.scatter(
                rating_view,
                x="avg_price",
                y="avg_rating",
                size="avg_reviews",
                color="category",
                color_discrete_map=CATEGORY_COLORS,
            )
            fig_rating.update_traces(
                marker=dict(opacity=0.85, line=dict(width=1, color="white")),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Avg price: $%{x:,.0f}<br>"
                    "Avg rating: %{y:.2f}<br>"
                    "Avg reviews: %{marker.size:,.0f}<extra></extra>"
                ),
            )
            apply_fig_style(fig_rating, "Value perception by category")
            fig_rating.update_xaxes(title="Average price ($)")
            fig_rating.update_yaxes(title="Average rating")
            st.plotly_chart(fig_rating, use_container_width=True)

    st.markdown("### Product explorer")
    if top_df.empty:
        st.info("Pas de données.")
    else:
        display_df = top_df.copy()
        display_df["price"] = display_df["price"].map(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
        display_df["rating"] = display_df["rating"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        st.dataframe(display_df, use_container_width=True, height=380, hide_index=True)

# ============================================================
# OPTIONAL RAW EXPLORER
# ============================================================
if show_raw:
    st.markdown("---")
    st.markdown("## Data explorer")

    raw_tab1, raw_tab2, raw_tab3 = st.tabs(["Pricing", "Correlation", "Top products"])

    with raw_tab1:
        st.dataframe(pricing, use_container_width=True, height=300, hide_index=True)

    with raw_tab2:
        st.dataframe(corr_df, use_container_width=True, height=300, hide_index=True)

    with raw_tab3:
        st.dataframe(top_df, use_container_width=True, height=300, hide_index=True)