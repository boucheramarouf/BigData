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
# PREMIUM CSS
# ============================================================
st.markdown(
    """
    <style>
        :root {
            --bg: #f5f7fb;
            --card: rgba(255,255,255,0.82);
            --card-border: rgba(15,23,42,0.08);
            --text: #0f172a;
            --muted: #64748b;
            --dark: #0b1020;
            --blue: #2563eb;
            --violet: #7c3aed;
            --teal: #14b8a6;
            --amber: #f59e0b;
            --rose: #ec4899;
            --shadow: 0 10px 30px rgba(2, 8, 23, 0.08);
        }

        html, body, [class*="css"] {
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(37,99,235,0.06), transparent 22%),
                radial-gradient(circle at top right, rgba(124,58,237,0.05), transparent 18%),
                linear-gradient(180deg, #f8fafc 0%, #f5f7fb 100%);
        }

        .block-container {
            padding-top: 1rem;
            padding-bottom: 2rem;
            max-width: 1550px;
        }

        [data-testid="stSidebar"] {
            background:
                radial-gradient(circle at top, rgba(255,255,255,0.08), transparent 20%),
                linear-gradient(180deg, #0b1020 0%, #0f172a 55%, #111827 100%);
            border-right: 1px solid rgba(255,255,255,0.06);
        }

        [data-testid="stSidebar"] * {
            color: #f8fafc !important;
        }

        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stToggle label {
            font-weight: 600;
        }

        .hero {
            padding: 1.65rem 1.7rem;
            border-radius: 24px;
            background:
                radial-gradient(circle at top right, rgba(255,255,255,0.14), transparent 25%),
                radial-gradient(circle at bottom left, rgba(37,99,235,0.12), transparent 30%),
                linear-gradient(135deg, #0b1020 0%, #111827 60%, #1e293b 100%);
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 18px 40px rgba(2,8,23,0.18);
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 2.15rem;
            font-weight: 850;
            color: white;
            line-height: 1.08;
            letter-spacing: -0.03em;
            margin-bottom: 0.45rem;
        }

        .hero-subtitle {
            font-size: 1rem;
            color: #dbe4f0;
            max-width: 1000px;
            line-height: 1.6;
            margin-bottom: 0.9rem;
        }

        .hero-chips {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
        }

        .hero-chip {
            display: inline-block;
            padding: 0.38rem 0.75rem;
            border-radius: 999px;
            font-size: 0.82rem;
            font-weight: 600;
            color: #f8fafc;
            background: rgba(255,255,255,0.08);
            border: 1px solid rgba(255,255,255,0.08);
            backdrop-filter: blur(8px);
        }

        .glass-card {
            background: var(--card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--card-border);
            border-radius: 22px;
            box-shadow: var(--shadow);
            padding: 1rem 1.05rem;
        }

        .kpi-card {
            background:
                linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(255,255,255,0.86) 100%);
            border: 1px solid rgba(15,23,42,0.08);
            border-radius: 22px;
            padding: 1rem 1rem 0.95rem 1rem;
            box-shadow: 0 12px 26px rgba(2,8,23,0.07);
            min-height: 132px;
        }

        .kpi-label {
            font-size: 0.86rem;
            font-weight: 700;
            color: #64748b;
            letter-spacing: 0.01em;
            margin-bottom: 0.6rem;
        }

        .kpi-value {
            font-size: 2rem;
            font-weight: 850;
            color: #0f172a;
            line-height: 1;
            letter-spacing: -0.03em;
            margin-bottom: 0.4rem;
        }

        .kpi-delta {
            font-size: 0.92rem;
            font-weight: 700;
        }

        .kpi-subtle {
            font-size: 0.82rem;
            color: #64748b;
            margin-top: 0.35rem;
            line-height: 1.45;
        }

        .section-kicker {
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #64748b;
            margin-bottom: 0.25rem;
        }

        .section-title {
            font-size: 1.15rem;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 0.9rem;
        }

        .insight-box {
            background:
                linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(248,250,252,0.92) 100%);
            border: 1px solid rgba(15,23,42,0.08);
            border-left: 6px solid #111827;
            border-radius: 18px;
            padding: 1rem 1rem;
            box-shadow: 0 10px 26px rgba(2,8,23,0.05);
            margin-bottom: 0.8rem;
        }

        .insight-title {
            font-weight: 800;
            font-size: 0.98rem;
            color: #0f172a;
            margin-bottom: 0.3rem;
        }

        .insight-text {
            color: #334155;
            font-size: 0.94rem;
            line-height: 1.6;
        }

        .mini-badge {
            display: inline-block;
            padding: 0.28rem 0.6rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 700;
            margin-right: 0.35rem;
            margin-top: 0.25rem;
            color: #0f172a;
            background: rgba(15,23,42,0.05);
        }

        .recruiter-box {
            padding: 1rem 1rem;
            border-radius: 18px;
            background:
                linear-gradient(135deg, rgba(37,99,235,0.09), rgba(124,58,237,0.08));
            border: 1px solid rgba(37,99,235,0.10);
            box-shadow: 0 10px 26px rgba(2,8,23,0.05);
        }

        .recruiter-title {
            font-size: 1rem;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 0.3rem;
        }

        .recruiter-text {
            font-size: 0.94rem;
            color: #334155;
            line-height: 1.6;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(15,23,42,0.06);
            box-shadow: 0 10px 24px rgba(2,8,23,0.05);
        }

        .foot-note {
            color: #64748b;
            font-size: 0.86rem;
            margin-top: 0.4rem;
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


def safe_to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^\d\.\-]", "", regex=True)
                .replace({"": np.nan, "nan": np.nan, "None": np.nan})
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fmt_currency(x):
    if pd.isna(x):
        return "—"
    return f"${x:,.0f}"


def fmt_pct(x):
    if pd.isna(x):
        return "—"
    return f"{x:.1f}%"


def fmt_num(x, digits=2):
    if pd.isna(x):
        return "—"
    return f"{x:.{digits}f}"


def safe_corr(df: pd.DataFrame, x_col: str, y_col: str):
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return np.nan
    clean = df[[x_col, y_col]].dropna()
    if len(clean) < 2:
        return np.nan
    return clean[x_col].corr(clean[y_col])


def delta_color(x):
    if pd.isna(x):
        return "#64748b"
    return "#16a34a" if x >= 0 else "#dc2626"


def corr_label(c):
    if pd.isna(c):
        return "Not enough data"
    if c >= 0.7:
        return "Strong positive linkage"
    if c >= 0.5:
        return "Positive linkage"
    if c >= 0.2:
        return "Moderate linkage"
    if c >= 0:
        return "Weak positive linkage"
    if c > -0.2:
        return "Weak negative linkage"
    return "Negative linkage"


def render_kpi_card(label, value, delta=None, subtle=None):
    delta_html = ""
    if delta is not None and delta != "":
        color = "#16a34a" if str(delta).startswith("+") else "#dc2626" if str(delta).startswith("-") else "#334155"
        delta_html = f'<div class="kpi-delta" style="color:{color};">{delta}</div>'
    subtle_html = f'<div class="kpi-subtle">{subtle}</div>' if subtle else ""
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            {delta_html}
            {subtle_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_header(kicker, title):
    st.markdown(
        f"""
        <div class="section-kicker">{kicker}</div>
        <div class="section-title">{title}</div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# PLOTLY THEME
# ============================================================
PALETTE = {
    "bg": "#ffffff",
    "text": "#0f172a",
    "muted": "#64748b",
    "grid": "rgba(15,23,42,0.08)",
    "line": "#111827",
    "blue": "#2563eb",
    "violet": "#7c3aed",
    "teal": "#14b8a6",
    "amber": "#f59e0b",
    "rose": "#ec4899",
    "green": "#16a34a",
    "red": "#dc2626",
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
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
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
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=PALETTE["grid"])
    fig.update_yaxes(showgrid=True, gridcolor=PALETTE["grid"], zeroline=False)
    return fig


# ============================================================
# DATA LOAD
# ============================================================
@st.cache_data(ttl=120)
def load_years_categories():
    df = q(
        f"""
        SELECT DISTINCT year_event, category
        FROM {PG_SCHEMA}.dm_product_pricing_strategy
        ORDER BY 1,2
        """
    )
    if df.empty:
        return [], []
    df = safe_to_numeric(df, ["year_event"])
    years = sorted([int(x) for x in df["year_event"].dropna().unique().tolist()]) if "year_event" in df.columns else []
    cats = sorted([str(x) for x in df["category"].dropna().unique().tolist()]) if "category" in df.columns else []
    return years, cats


@st.cache_data(ttl=120)
def load_pricing(where_sql: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    df = q(
        f"""
        SELECT year_event, category, avg_price, median_price, premium_ratio, products_count
        FROM {PG_SCHEMA}.dm_product_pricing_strategy
        {where_sql}
        ORDER BY year_event, category
        """,
        params_local,
    )
    return safe_to_numeric(df, ["year_event", "avg_price", "median_price", "premium_ratio", "products_count"])


@st.cache_data(ttl=120)
def load_stock_monthly():
    df = q(
        f"""
        SELECT year_event, month_event, avg_close, sum_volume, avg_volatility_7d
        FROM {PG_SCHEMA}.dm_stock_performance_monthly
        ORDER BY year_event, month_event
        """
    )
    df = safe_to_numeric(df, ["year_event", "month_event", "avg_close", "sum_volume", "avg_volatility_7d"])
    if not df.empty and {"year_event", "month_event"}.issubset(df.columns):
        df = df.dropna(subset=["year_event", "month_event"]).copy()
        df["year_event"] = df["year_event"].astype(int)
        df["month_event"] = df["month_event"].astype(int)
        df["date"] = pd.to_datetime(
            df["year_event"].astype(str) + "-" + df["month_event"].astype(str).str.zfill(2) + "-01",
            errors="coerce"
        )
        df = df.dropna(subset=["date"]).copy()
    return df


@st.cache_data(ttl=120)
def load_corr(where_sql: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    df = q(
        f"""
        SELECT year_event, category, avg_price, avg_close_year, avg_volatility_7d_year
        FROM {PG_SCHEMA}.dm_product_stock_correlation_yearly
        {where_sql}
        ORDER BY year_event, category
        """,
        params_local,
    )
    return safe_to_numeric(df, ["year_event", "avg_price", "avg_close_year", "avg_volatility_7d_year"])


@st.cache_data(ttl=120)
def load_top(where_sql_top: str, params_tuple: tuple):
    params_local = dict(params_tuple)
    df = q(
        f"""
        SELECT product_id, category, model_name, release_year, price, price_tier, rating, review_count
        FROM {PG_SCHEMA}.dm_top_products
        {where_sql_top}
        ORDER BY price DESC
        LIMIT 200
        """,
        params_local,
    )
    return safe_to_numeric(df, ["release_year", "price", "rating", "review_count"])


# ============================================================
# SIDEBAR
# ============================================================
years, cats = load_years_categories()

st.sidebar.markdown("## 🍎 Apple Strategy Intelligence")
st.sidebar.caption("Premium data storytelling for product strategy & market behavior")

year_opt = ["Toutes"] + [str(y) for y in years]
cat_opt = ["Toutes"] + cats

selected_year = st.sidebar.selectbox("Année", options=year_opt, index=0)
selected_cat = st.sidebar.selectbox("Catégorie", options=cat_opt, index=0)

st.sidebar.markdown("---")
show_raw = st.sidebar.toggle("Afficher les tables brutes", value=False)
show_annotations = st.sidebar.toggle("Afficher les insights automatiques", value=True)
focus_latest = st.sidebar.toggle("Mettre l'accent sur la dernière année", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### Ce que regarde un recruteur")
st.sidebar.markdown(
    """
    - Storytelling clair  
    - KPI business compréhensibles  
    - Corrélation défendable  
    - Dashboard propre et cohérent  
    - Maîtrise visuelle + technique
    """
)

# ============================================================
# FILTERS
# ============================================================
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
# LOAD DATA
# ============================================================
pricing = load_pricing(where_sql, tuple(params.items()))
stock_monthly = load_stock_monthly()
corr_df = load_corr(where_sql, tuple(params.items()))
top_df = load_top(where_sql_top, tuple(params.items()))

# ============================================================
# DERIVED DATA
# ============================================================
latest_year = None if pricing.empty else int(pricing["year_event"].dropna().max())
year_values = sorted(pricing["year_event"].dropna().unique().tolist()) if not pricing.empty else []
previous_year = int(year_values[-2]) if len(year_values) > 1 else None

current_pricing = pricing[pricing["year_event"] == latest_year].copy() if latest_year is not None else pd.DataFrame()
prev_pricing = pricing[pricing["year_event"] == previous_year].copy() if previous_year is not None else pd.DataFrame()

avg_price_kpi = current_pricing["avg_price"].mean() if not current_pricing.empty else np.nan
premium_ratio_kpi = current_pricing["premium_ratio"].mean() if not current_pricing.empty else np.nan
stock_kpi = stock_monthly["avg_close"].iloc[-1] if not stock_monthly.empty else np.nan
corr_kpi = safe_corr(corr_df, "avg_price", "avg_close_year")

prev_avg_price = prev_pricing["avg_price"].mean() if not prev_pricing.empty else np.nan
avg_price_delta_pct = (
    ((avg_price_kpi - prev_avg_price) / prev_avg_price) * 100
    if pd.notna(avg_price_kpi) and pd.notna(prev_avg_price) and prev_avg_price != 0
    else np.nan
)

if latest_year is not None and not corr_df.empty:
    corr_latest = corr_df[corr_df["year_event"] == latest_year].copy()
else:
    corr_latest = pd.DataFrame()

best_category = None
if not current_pricing.empty:
    tmp = current_pricing.dropna(subset=["avg_price"]).sort_values("avg_price", ascending=False)
    if not tmp.empty:
        best_category = tmp.iloc[0]["category"]

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

annual_pricing = pd.DataFrame()
if not pricing.empty:
    annual_pricing = (
        pricing.groupby("year_event", as_index=False)
        .agg(
            avg_price=("avg_price", "mean"),
            premium_ratio=("premium_ratio", "mean"),
            products_count=("products_count", "sum")
        )
        .sort_values("year_event")
    )

annual_stock = pd.DataFrame()
if not corr_df.empty:
    annual_stock = (
        corr_df.groupby("year_event", as_index=False)
        .agg(
            avg_close_year=("avg_close_year", "mean"),
            avg_volatility=("avg_volatility_7d_year", "mean")
        )
        .sort_values("year_event")
    )

annual_story = annual_pricing.merge(annual_stock, on="year_event", how="left") if not annual_pricing.empty else pd.DataFrame()

# ============================================================
# AUTO INSIGHTS
# ============================================================
insights = []

if latest_year is not None and pd.notna(avg_price_kpi):
    insights.append(f"In {latest_year}, the average product price reaches {fmt_currency(avg_price_kpi)}.")

if pd.notna(premium_ratio_kpi):
    insights.append(f"The premium ratio sits at {fmt_pct(premium_ratio_kpi)}, supporting the premiumization narrative.")

if best_category:
    insights.append(f"{best_category} appears as the most premium category on the current scope.")

if pd.notna(corr_kpi):
    insights.append(f"The estimated pricing-to-stock correlation is {corr_kpi:.2f}, indicating {corr_label(corr_kpi).lower()}.")

if not annual_story.empty and len(annual_story) >= 2:
    last_row = annual_story.iloc[-1]
    first_row = annual_story.iloc[0]
    if pd.notna(first_row["avg_price"]) and first_row["avg_price"] != 0 and pd.notna(last_row["avg_price"]):
        growth = ((last_row["avg_price"] - first_row["avg_price"]) / first_row["avg_price"]) * 100
        insights.append(f"Average pricing has moved by {growth:+.1f}% between {int(first_row['year_event'])} and {int(last_row['year_event'])}.")

hero_summary = " ".join(insights[:3]) if insights else "Interactive analysis of Apple pricing strategy and stock performance."

# ============================================================
# HERO
# ============================================================
st.markdown(
    f"""
    <div class="hero">
        <div class="hero-title">Apple Strategy Intelligence</div>
        <div class="hero-subtitle">
            {hero_summary}
        </div>
        <div class="hero-chips">
            <span class="hero-chip">Medallion Architecture</span>
            <span class="hero-chip">PostgreSQL Datamarts</span>
            <span class="hero-chip">Streamlit + Plotly</span>
            <span class="hero-chip">Executive Storytelling</span>
            <span class="hero-chip">Recruiter-ready Visuals</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# KPI CARDS
# ============================================================
c1, c2, c3, c4 = st.columns(4, gap="large")

with c1:
    render_kpi_card(
        "Average Product Price",
        fmt_currency(avg_price_kpi),
        None if pd.isna(avg_price_delta_pct) else f"{avg_price_delta_pct:+.1f}% vs previous year",
        "Average of category pricing on the current perimeter."
    )

with c2:
    render_kpi_card(
        "Premium Ratio",
        fmt_pct(premium_ratio_kpi),
        None,
        "Share of premium products in the selected scope."
    )

with c3:
    render_kpi_card(
        "AAPL Monthly Avg Close",
        fmt_currency(stock_kpi),
        None,
        "Latest available monthly market level from the stock datamart."
    )

with c4:
    render_kpi_card(
        "Pricing / Stock Correlation",
        "—" if pd.isna(corr_kpi) else f"{corr_kpi:.2f}",
        corr_label(corr_kpi),
        "Pearson correlation between pricing level and yearly market level."
    )

st.markdown("")

# ============================================================
# EXECUTIVE SUMMARY
# ============================================================
if show_annotations:
    left, right = st.columns([1.5, 1], gap="large")

    with left:
        section_header("Executive View", "What stands out immediately")
        dominant_text = f"{best_category} leads the premium spectrum." if best_category else "No dominant category identified."
        corr_text = f"Correlation is {corr_kpi:.2f}, which suggests {corr_label(corr_kpi).lower()}." if pd.notna(corr_kpi) else "Correlation cannot be estimated on this scope."

        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">Business interpretation</div>
                <div class="insight-text">
                    Apple’s product strategy appears increasingly premium-oriented, with a current average pricing level of
                    <b>{fmt_currency(avg_price_kpi)}</b> and a premium ratio of <b>{fmt_pct(premium_ratio_kpi)}</b>.<br><br>
                    {dominant_text}<br><br>
                    {corr_text}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if insights:
            badges = "".join([f'<span class="mini-badge">{x}</span>' for x in insights[:4]])
            st.markdown(badges, unsafe_allow_html=True)

    with right:
        section_header("Recruiter Angle", "Why this looks stronger")
        st.markdown(
            """
            <div class="recruiter-box">
                <div class="recruiter-title">This dashboard feels senior because:</div>
                <div class="recruiter-text">
                    It frames a business hypothesis, prioritizes KPI clarity, uses premium visual hierarchy,
                    and turns raw datamarts into an interpretable strategic narrative.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(
    ["Overview", "Pricing Strategy", "Market Impact", "Top Products"]
)

# ============================================================
# TAB 1 - OVERVIEW
# ============================================================
with tab1:
    col_l, col_r = st.columns([1.35, 1], gap="large")

    with col_l:
        section_header("Trajectory", "Price evolution by category")

        if pricing.empty:
            st.warning("Aucune donnée disponible pour les filtres sélectionnés.")
        else:
            fig = go.Figure()

            latest_annotations = []
            for cat in pricing["category"].dropna().unique():
                d = pricing[pricing["category"] == cat].sort_values("year_event")
                if d.empty:
                    continue

                fig.add_trace(
                    go.Scatter(
                        x=d["year_event"],
                        y=d["avg_price"],
                        mode="lines+markers",
                        name=cat,
                        line=dict(width=3, color=CATEGORY_COLORS.get(cat, PALETTE["blue"])),
                        marker=dict(size=8),
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "Year: %{x}<br>"
                            "Avg price: $%{y:,.0f}<br>"
                            "Premium ratio: %{customdata[0]:.1f}%<br>"
                            "Products: %{customdata[1]:,.0f}<extra></extra>"
                        ),
                        customdata=d[["premium_ratio", "products_count"]].fillna(0).to_numpy(),
                    )
                )

                if focus_latest and not d.empty:
                    last = d.iloc[-1]
                    latest_annotations.append(
                        dict(
                            x=last["year_event"],
                            y=last["avg_price"],
                            text=str(cat),
                            showarrow=False,
                            xanchor="left",
                            yanchor="middle",
                            font=dict(size=11, color=CATEGORY_COLORS.get(cat, PALETTE["text"]))
                        )
                    )

            apply_fig_style(fig, "Pricing trajectory by product category")
            fig.update_xaxes(dtick=1)
            fig.update_yaxes(title="Average price ($)")
            if latest_annotations:
                fig.update_layout(annotations=latest_annotations)
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        section_header("Intensity", "Pricing heatmap")
        if pricing.empty:
            st.info("Pas de données.")
        else:
            heat = pricing.pivot_table(
                index="category",
                columns="year_event",
                values="avg_price",
                aggfunc="mean"
            )
            if heat.empty:
                st.info("Pas assez de données pour afficher la heatmap.")
            else:
                fig_h = px.imshow(
                    heat,
                    aspect="auto",
                    text_auto=".0f",
                    color_continuous_scale="Blues",
                )
                apply_fig_style(fig_h, "Average price intensity by category & year")
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
        section_header("Premiumization", "Premium ratio vs average price")

        if annual_pricing.empty:
            st.warning("Aucune donnée.")
        else:
            fig_combo = make_subplots(specs=[[{"secondary_y": True}]])
            fig_combo.add_trace(
                go.Bar(
                    x=annual_pricing["year_event"],
                    y=annual_pricing["premium_ratio"],
                    name="Premium ratio",
                    marker_color=PALETTE["blue"],
                    hovertemplate="Year: %{x}<br>Premium ratio: %{y:.1f}%<extra></extra>",
                ),
                secondary_y=False,
            )
            fig_combo.add_trace(
                go.Scatter(
                    x=annual_pricing["year_event"],
                    y=annual_pricing["avg_price"],
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

            if show_annotations and len(annual_pricing) >= 2:
                first = annual_pricing.iloc[0]
                last = annual_pricing.iloc[-1]
                growth = (
                    ((last["avg_price"] - first["avg_price"]) / first["avg_price"]) * 100
                    if pd.notna(first["avg_price"]) and first["avg_price"] != 0 and pd.notna(last["avg_price"])
                    else np.nan
                )
                st.markdown(
                    f"""
                    <div class="foot-note">
                        Insight: average pricing moved from <b>{fmt_currency(first['avg_price'])}</b> in {int(first['year_event'])}
                        to <b>{fmt_currency(last['avg_price'])}</b> in {int(last['year_event'])},
                        a variation of <b>{fmt_pct(growth)}</b>.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    with right:
        section_header("Mix", "Product distribution by price tier")

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
            apply_fig_style(fig_mix, "Visible portfolio mix by price tier")
            fig_mix.update_traces(
                hovertemplate="Category: %{x}<br>Tier: %{fullData.name}<br>Count: %{y}<extra></extra>"
            )
            st.plotly_chart(fig_mix, use_container_width=True)

# ============================================================
# TAB 3 - MARKET IMPACT
# ============================================================
with tab3:
    top_left, top_right = st.columns([1.2, 1], gap="large")

    with top_left:
        section_header("Market momentum", "AAPL monthly stock evolution")
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
                    line=dict(width=3.5, color=PALETTE["line"]),
                    fill="tozeroy",
                    fillcolor="rgba(37,99,235,0.08)",
                    hovertemplate=(
                        "Month: %{x|%b %Y}<br>"
                        "Avg close: $%{y:,.2f}<br>"
                        "Volume: %{customdata[0]:,.0f}<br>"
                        "Volatility 7d: %{customdata[1]:.2f}<extra></extra>"
                    ),
                    customdata=stock_monthly[["sum_volume", "avg_volatility_7d"]].fillna(0).to_numpy(),
                )
            )
            apply_fig_style(fig_stock, "Monthly stock performance (AAPL)")
            fig_stock.update_yaxes(title="Average close ($)")

            if focus_latest and not stock_monthly.empty:
                peak_row = stock_monthly.loc[stock_monthly["avg_close"].idxmax()]
                fig_stock.add_annotation(
                    x=peak_row["date"],
                    y=peak_row["avg_close"],
                    text=f"Peak: ${peak_row['avg_close']:,.0f}",
                    showarrow=True,
                    arrowhead=2,
                    ax=40,
                    ay=-40,
                    bgcolor="white",
                    bordercolor="rgba(15,23,42,0.1)",
                )

            st.plotly_chart(fig_stock, use_container_width=True)

    with top_right:
        section_header("Linkage", "Pricing vs market valuation")
        if corr_df.empty:
            st.warning("Aucune donnée de corrélation.")
        else:
            plot_corr = corr_df.dropna(subset=["avg_close_year", "avg_price"]).copy()
            if plot_corr.empty:
                st.info("Pas assez de données.")
            else:
                fig_scatter = px.scatter(
                    plot_corr,
                    x="avg_close_year",
                    y="avg_price",
                    color="category",
                    size_max=20,
                    color_discrete_map=CATEGORY_COLORS,
                )
                fig_scatter.update_traces(
                    marker=dict(size=15, opacity=0.82, line=dict(width=1, color="white")),
                    hovertemplate=(
                        "<b>%{fullData.name}</b><br>"
                        "Avg close year: $%{x:,.2f}<br>"
                        "Avg product price: $%{y:,.0f}<br>"
                        "Year: %{customdata[0]}<br>"
                        "Volatility: %{customdata[1]:.2f}<extra></extra>"
                    ),
                    customdata=plot_corr[["year_event", "avg_volatility_7d_year"]].fillna(0).to_numpy(),
                )
                apply_fig_style(fig_scatter, "Relationship between pricing and market valuation")
                fig_scatter.update_xaxes(title="Average yearly close ($)")
                fig_scatter.update_yaxes(title="Average product price ($)")
                st.plotly_chart(fig_scatter, use_container_width=True)

    section_header("Storyline", "Premium mix, pricing and stock performance")
    if annual_story.empty:
        st.info("Données insuffisantes pour le storyline annuel.")
    else:
        fig_story = make_subplots(specs=[[{"secondary_y": True}]])
        fig_story.add_trace(
            go.Bar(
                x=annual_story["year_event"],
                y=annual_story["premium_ratio"],
                name="Premium ratio",
                marker_color=PALETTE["teal"],
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
                line=dict(width=3, color=PALETTE["violet"]),
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
        apply_fig_style(fig_story, "Multi-metric yearly storyline")
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
        section_header("Leaders", "Top 10 highest-priced products")

        if top_df.empty:
            st.warning("Aucun top produit sur ce filtre.")
        else:
            top10 = top_df.dropna(subset=["price"]).nlargest(10, "price").copy()
            top10["label"] = top10["model_name"].fillna(top10["product_id"].astype(str))

            if top10.empty:
                st.info("Pas assez de données.")
            else:
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
                    customdata=top10[["category", "price_tier", "rating"]].fillna("—").to_numpy(),
                )
                apply_fig_style(fig_top, "Most premium visible products")
                fig_top.update_xaxes(title="Price ($)")
                fig_top.update_yaxes(title="")
                st.plotly_chart(fig_top, use_container_width=True)

    with right:
        section_header("Perception", "Category value perception")

        if top_df.empty or "rating" not in top_df.columns:
            st.info("Pas de ratings disponibles.")
        else:
            plot_df = top_df.copy()
            plot_df = safe_to_numeric(plot_df, ["rating", "review_count", "price"])
            plot_df = plot_df.dropna(subset=["category", "rating", "price"])

            if plot_df.empty:
                st.info("Pas assez de données numériques valides pour afficher ce graphique.")
            else:
                rating_view = plot_df.groupby("category", as_index=False).agg(
                    avg_rating=("rating", "mean"),
                    avg_reviews=("review_count", "mean"),
                    avg_price=("price", "mean"),
                )
                rating_view["avg_reviews"] = rating_view["avg_reviews"].fillna(1)

                fig_rating = px.scatter(
                    rating_view,
                    x="avg_price",
                    y="avg_rating",
                    size="avg_reviews",
                    color="category",
                    color_discrete_map=CATEGORY_COLORS,
                )
                fig_rating.update_traces(
                    marker=dict(opacity=0.86, line=dict(width=1, color="white")),
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

    section_header("Explorer", "Detailed product table")
    if top_df.empty:
        st.info("Pas de données.")
    else:
        display_df = top_df.copy()
        if "price" in display_df.columns:
            display_df["price"] = display_df["price"].map(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
        if "rating" in display_df.columns:
            display_df["rating"] = display_df["rating"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        if "review_count" in display_df.columns:
            display_df["review_count"] = display_df["review_count"].map(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        if "release_year" in display_df.columns:
            display_df["release_year"] = display_df["release_year"].map(lambda x: int(x) if pd.notna(x) else "—")

        st.dataframe(display_df, use_container_width=True, height=390, hide_index=True)

# ============================================================
# RAW EXPLORER
# ============================================================
if show_raw:
    st.markdown("---")
    section_header("Debug / Transparency", "Raw data explorer")
    raw1, raw2, raw3 = st.tabs(["Pricing", "Correlation", "Top products"])

    with raw1:
        st.dataframe(pricing, use_container_width=True, height=300, hide_index=True)
    with raw2:
        st.dataframe(corr_df, use_container_width=True, height=300, hide_index=True)
    with raw3:
        st.dataframe(top_df, use_container_width=True, height=300, hide_index=True)