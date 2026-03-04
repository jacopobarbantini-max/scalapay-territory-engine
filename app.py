"""
app.py — Scalapay Territory Engine
Premium UI with Scalapay Design System
"""

import io
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from config import SCORING_WEIGHTS
from similarweb_client import ingest
from hubspot_client import enrich_with_hubspot
from enrichment import enrich_dataframe
from scoring import score_dataframe
from utils import get_logger

log = get_logger("app")

# ── PAGE CONFIG ─────────────────────────────────────────────
st.set_page_config(
    page_title="Scalapay Territory Engine",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── SCALAPAY DESIGN SYSTEM CSS ──────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');

    /* ─── GLOBAL ─── */
    .stApp {
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    h1, h2, h3, h4, h5, h6, p, span, div, label {
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }

    /* ─── SIDEBAR ─── */
    section[data-testid="stSidebar"] {
        background: #FDF5F3;
        border-right: 1px solid #F0D5CE;
    }
    section[data-testid="stSidebar"] .stMarkdown h3 {
        font-size: 0.8rem;
        font-weight: 600;
        color: #8C939A;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 4px;
    }

    /* ─── HEADER COMPONENT ─── */
    .te-header {
        padding: 24px 0 20px 0;
        border-bottom: 1px solid #F0D5CE;
        margin-bottom: 28px;
    }
    .te-logo-row {
        display: flex;
        align-items: center;
        gap: 14px;
        margin-bottom: 6px;
    }
    .te-logo-badge {
        background: linear-gradient(135deg, #EA5440 0%, #F27060 100%);
        color: white;
        font-weight: 700;
        font-size: 0.7rem;
        padding: 5px 10px;
        border-radius: 8px;
        letter-spacing: 0.03em;
    }
    .te-title {
        font-size: 1.75rem;
        font-weight: 700;
        color: #1A1D21;
        margin: 0;
        line-height: 1.2;
    }
    .te-subtitle {
        color: #8C939A;
        font-size: 0.92rem;
        margin: 4px 0 0 0;
        font-weight: 400;
    }

    /* ─── SIDEBAR SECTIONS ─── */
    .sidebar-section {
        background: #FFFFFF;
        border: 1px solid #F0D5CE;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 16px;
    }
    .sidebar-section-title {
        font-size: 0.72rem;
        font-weight: 600;
        color: #8C939A;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 12px;
    }
    .sidebar-divider {
        border: none;
        border-top: 1px solid #F0D5CE;
        margin: 20px 0;
    }

    /* ─── INTEGRATION HELP TOOLTIPS ─── */
    .integration-item {
        display: flex;
        align-items: flex-start;
        gap: 10px;
        padding: 10px 0;
    }
    .integration-icon {
        width: 32px;
        height: 32px;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1rem;
        flex-shrink: 0;
    }
    .integration-icon-hs { background: #FFF4E5; }
    .integration-icon-scrape { background: #EEF0FF; }
    .integration-icon-ads { background: #F0FFF4; }
    .integration-desc {
        font-size: 0.78rem;
        color: #6B7280;
        line-height: 1.4;
        margin-top: 2px;
    }

    /* ─── KPI METRIC CARDS ─── */
    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #F0D5CE;
        border-radius: 14px;
        padding: 22px 18px;
        text-align: center;
        transition: all 0.2s ease;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    .kpi-card:hover {
        border-color: #EA5440;
        box-shadow: 0 4px 12px rgba(234,84,64,0.10);
        transform: translateY(-1px);
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1A1D21;
        line-height: 1.2;
        margin-bottom: 4px;
    }
    .kpi-value-gold { color: #B45309; }
    .kpi-value-coral { color: #EA5440; }
    .kpi-value-green { color: #059669; }
    .kpi-label {
        color: #8C939A;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-weight: 500;
    }

    /* ─── TIER BADGES ─── */
    .tier-gold {
        background: #FEF3C7;
        color: #92400E;
        font-weight: 600;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.8rem;
    }
    .tier-silver {
        background: #F1F5F9;
        color: #475569;
        font-weight: 600;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.8rem;
    }
    .tier-bronze {
        background: #FED7AA;
        color: #9A3412;
        font-weight: 600;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.8rem;
    }

    /* ─── DATA TABLES ─── */
    div[data-testid="stDataFrame"] {
        border: 1px solid #F0D5CE;
        border-radius: 12px;
        overflow: hidden;
    }

    /* ─── BUTTONS ─── */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #EA5440 0%, #F06050 100%);
        border: none;
        border-radius: 10px;
        font-weight: 600;
        font-size: 0.9rem;
        padding: 10px 24px;
        transition: all 0.2s ease;
        box-shadow: 0 2px 8px rgba(234,84,64,0.25);
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 4px 16px rgba(234,84,64,0.35);
        transform: translateY(-1px);
    }
    .stButton > button[kind="secondary"] {
        border-radius: 10px;
        font-weight: 500;
        border: 1px solid #F0D5CE;
    }

    /* ─── TABS ─── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #F0D5CE;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        font-weight: 500;
        font-size: 0.85rem;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        border-bottom: 2px solid #EA5440 !important;
        color: #EA5440;
    }

    /* ─── PROGRESS BAR ─── */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #EA5440, #F27060);
        border-radius: 8px;
    }

    /* ─── PHASE HEADERS ─── */
    .phase-header {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 12px 0 6px 0;
    }
    .phase-number {
        background: #EA5440;
        color: white;
        width: 26px;
        height: 26px;
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.75rem;
        font-weight: 700;
        flex-shrink: 0;
    }
    .phase-text {
        font-size: 1rem;
        font-weight: 600;
        color: #1A1D21;
    }

    /* ─── LANDING STATE ─── */
    .landing-container {
        text-align: center;
        padding: 80px 20px;
    }
    .landing-icon {
        font-size: 3.5rem;
        margin-bottom: 16px;
        opacity: 0.9;
    }
    .landing-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #1A1D21;
        margin-bottom: 8px;
    }
    .landing-desc {
        color: #8C939A;
        font-size: 0.9rem;
        max-width: 400px;
        margin: 0 auto;
        line-height: 1.5;
    }

    /* ─── FOOTER ─── */
    .te-footer {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 0;
        border-top: 1px solid #F0D5CE;
        margin-top: 32px;
    }
    .te-footer-left {
        color: #8C939A;
        font-size: 0.78rem;
    }
    .te-footer-logo {
        display: flex;
        align-items: center;
        gap: 6px;
        color: #8C939A;
        font-size: 0.75rem;
    }
    .te-footer-logo .scalapay-heart {
        color: #EA5440;
        font-size: 0.85rem;
    }

    /* ─── EXPANDER ─── */
    .streamlit-expanderHeader {
        font-weight: 500;
        font-size: 0.9rem;
    }

    /* ─── HIDE DEFAULT HEADER ─── */
    header[data-testid="stHeader"] {
        background: rgba(255,255,255,0.97);
        backdrop-filter: blur(8px);
    }

    /* ─── SLIDER STYLING ─── */
    .stSlider [data-baseweb="slider"] [role="slider"] {
        background-color: #EA5440;
    }
    .stSlider [data-baseweb="slider"] div[data-testid="stTickBar"] > div {
        background-color: #EA5440;
    }

    /* ─── CHECKBOX ─── */
    .stCheckbox label span[data-testid="stCheckbox"] {
        color: #1A1D21;
    }

    /* ─── ALERTS ─── */
    .stAlert {
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ──────────────────────────────────────────────────
st.markdown("""
<div class="te-header">
    <div class="te-logo-row">
        <span class="te-logo-badge">TERRITORY ENGINE</span>
    </div>
    <p class="te-title">Lead Scoring & Territory Builder</p>
    <p class="te-subtitle">Automated pipeline for IB (Iberia) / FR (France) sales expansion</p>
</div>
""", unsafe_allow_html=True)

# ── SIDEBAR — CONFIGURATION ────────────────────────────────
with st.sidebar:
    # Sidebar branding
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 8px; padding: 4px 0 16px 0;">
        <span style="color: #EA5440; font-size: 1.1rem;">♥</span>
        <span style="font-weight: 600; font-size: 0.95rem; color: #3A4045;">scalapay</span>
        <span style="color: #8C939A; font-size: 0.75rem; margin-left: auto;">Territory Engine</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    # ── DATA SOURCE ──
    st.markdown("### Data Source")

    data_mode = st.radio(
        "Input mode",
        ["CSV Upload", "API"],
        index=0,
        label_visibility="collapsed",
        help="Upload a Similarweb export file, or connect via API if you have a key.",
    )

    territory_labels = {"IB": "IB (Iberia: ES + PT)", "FR": "FR (France)"}
    countries = st.multiselect(
        "Target territories",
        ["IB", "FR"],
        default=["IB", "FR"],
        format_func=lambda x: territory_labels[x],
    )

    uploaded_files = {}
    if "CSV" in data_mode:
        st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
        st.markdown("### Upload Data")
        for c in countries:
            upload_label = "Iberia (ES+PT)" if c == "IB" else c
            f = st.file_uploader(
                f"Similarweb — {upload_label}",
                type=["csv", "xlsx", "xls"],
                key=f"sw_upload_{c}",
            )
            if f:
                uploaded_files[c] = f

        if not uploaded_files:
            use_sample = st.checkbox("Use sample data for demo", value=True)
        else:
            use_sample = False
    else:
        use_sample = False

    # ── INTEGRATIONS ──
    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
    st.markdown("### Integrations")

    enable_hubspot = st.checkbox(
        "HubSpot CRM",
        value=bool(os.getenv("HUBSPOT_API_KEY")),
        help="Cross-references every domain against your HubSpot CRM to find existing companies, active deals, and deal owners. Assigns a warmth label (Active Pipeline, In CRM No Deal, Net New, Lost 6m+ ago, Recently Lost, Existing Client) that feeds into the lead score.",
    )
    st.markdown('<p style="font-size:0.75rem; color:#8C939A; margin-top:-10px; margin-bottom:12px;">Matches leads to CRM records, deal stages, and owners. Determines lead warmth for scoring.</p>', unsafe_allow_html=True)

    enable_scraping = st.checkbox(
        "Competitor Detection",
        value=False,
        help="Scrapes merchant websites (homepage, product pages, checkout paths, JS bundles) to detect BNPL competitors like Klarna, Alma, Sequra, and Oney. Also checks structured data, sitemaps, and DNS records. Uses 9 detection layers for ~75% coverage. Enables the whitespace score component.",
    )
    st.markdown('<p style="font-size:0.75rem; color:#8C939A; margin-top:-10px; margin-bottom:12px;">Detects Klarna, Alma, Sequra & 11 more BNPL providers across 9 scraping layers. Feeds whitespace scoring.</p>', unsafe_allow_html=True)

    enable_ads_check = st.checkbox(
        "Ad Pixel Detection",
        value=False,
        help="Detects Meta (Facebook) Pixel and Google Ads conversion tags on merchant homepages. Merchants with active ad pixels are investing in customer acquisition — a signal they have budget and need checkout optimization. Requires competitor detection to be enabled.",
    )
    st.markdown('<p style="font-size:0.75rem; color:#8C939A; margin-top:-10px; margin-bottom:12px;">Finds Meta Pixel & Google Ads tags. Signals ad spend = budget for checkout optimization.</p>', unsafe_allow_html=True)

    # ── SCORING WEIGHTS ──
    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
    st.markdown("### Scoring Weights")
    st.markdown('<p style="font-size:0.75rem; color:#8C939A; margin-bottom:8px;">Adjust how each component contributes to the final priority score (0–100).</p>', unsafe_allow_html=True)

    w_tier = st.slider("Industry Tier", 0.0, 0.5, SCORING_WEIGHTS["tier"], 0.05,
        help="How well does BNPL convert in this vertical? Gold verticals (Apparel, Beauty, Pharma) have highest contribution margins. Mapped per country from Scalapay internal data.")
    w_pen = st.slider("Penetration / TTV", 0.0, 0.5, SCORING_WEIGHTS["penetration_ttv"], 0.05,
        help="Revenue opportunity. Combines the BNPL penetration rate for this vertical (from Scalapay data) with estimated annual transaction value based on traffic × conversion × AOV × Scalapay share.")
    w_growth = st.slider("Traffic Growth", 0.0, 0.5, SCORING_WEIGHTS["traffic_growth"], 0.05,
        help="Merchant momentum. Blends YoY growth (60% weight, structural) with MoM growth (40%, recent trend). Proxy for ad spend: growing merchants invest in acquisition and need checkout optimization.")
    w_warmth = st.slider("Lead Warmth", 0.0, 0.5, SCORING_WEIGHTS["lead_warmth"], 0.05,
        help="CRM status from HubSpot. Active Pipeline scores highest (10), then In CRM No Deal (7), Net New (5), Lost 6m+ ago (4), Recently Lost (3), Existing Client (2). Without HubSpot enabled, all leads default to Net New.")
    w_ws = st.slider("Whitespace", 0.0, 0.5, SCORING_WEIGHTS["whitespace"], 0.05,
        help="Greenfield opportunity. Scores 10/10 if zero real BNPL competitors are detected on the merchant site. PayPal BNPL alone still counts as whitespace (8/10). Requires competitor detection enabled.")

    # ── FILTERS ──
    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
    st.markdown("### Filters")
    min_traffic = st.number_input("Minimum monthly traffic", value=0, step=50_000)
    exclude_won = st.checkbox("Exclude Existing Clients", value=True)


# ── MAIN PIPELINE ───────────────────────────────────────────
def load_sample_data(country: str) -> pd.DataFrame:
    file_code = "es" if country == "IB" else country.lower()
    path = f"sample_data/similarweb_sample_{file_code}.csv"
    try:
        df = pd.read_csv(path)
        df["country"] = country.upper()
        from utils import clean_similarweb_df
        return clean_similarweb_df(df)
    except Exception as e:
        st.error(f"Could not load sample data for {country}: {e}")
        return pd.DataFrame()


def run_pipeline():
    # ── PHASE 1: DATA INGESTION ──
    st.markdown("""
    <div class="phase-header">
        <span class="phase-number">1</span>
        <span class="phase-text">Data Ingestion & CRM Cross-Check</span>
    </div>
    """, unsafe_allow_html=True)
    progress = st.progress(0, text=f"📂 Loading data — 0/{len(countries)} territories...")

    all_dfs = []
    for i, country in enumerate(countries):
        if use_sample:
            df = load_sample_data(country)
        elif country in uploaded_files:
            df = ingest(country, uploaded_file=uploaded_files[country])
        else:
            df = ingest(country)

        if not df.empty:
            all_dfs.append(df)

        progress.progress((i + 1) / len(countries), text=f"📂 Loading data — {i+1}/{len(countries)} territories loaded")

    if not all_dfs:
        st.error("No data to process. Upload CSVs or enable sample data.")
        return None

    df = pd.concat(all_dfs, ignore_index=True)
    total_leads = len(df)
    progress.progress(1.0, text=f"✓ Data loaded — {total_leads} leads across {', '.join(countries)}")

    traffic_col = "monthly_traffic" if "monthly_traffic" in df.columns else None
    if traffic_col:
        before = len(df)
        df = df[df[traffic_col] >= min_traffic].reset_index(drop=True)
        filtered = before - len(df)
        if filtered > 0:
            st.caption(f"Filtered out {filtered} leads below {min_traffic:,} monthly traffic")

    if enable_hubspot:
        total_leads = len(df)
        hs_progress = st.progress(0, text=f"🔍 HubSpot CRM — checking 0/{total_leads} domains...")

        def update_hs(current, total):
            pct = current / total if total > 0 else 1
            hs_progress.progress(pct, text=f"🔍 HubSpot CRM — {current}/{total} domains checked")

        df = enrich_with_hubspot(df, progress_callback=update_hs)
        hs_found = df["hs_exists"].sum() if "hs_exists" in df.columns else 0
        hs_progress.progress(1.0, text=f"✓ HubSpot CRM — {hs_found}/{total_leads} records matched")
    else:
        df["hs_exists"] = False
        df["hs_company_name"] = ""
        df["pipeline"] = ""
        df["deal_stage"] = ""
        df["deal_owner"] = ""
        df["hs_cross_country"] = False
        df["is_won"] = False
        df["is_in_pipeline"] = False
        df["lead_warmth"] = "Net New"

    progress.progress(1.0, text="Phase 1 complete")

    # ── PHASE 2: ENRICHMENT ──
    st.markdown("""
    <div class="phase-header">
        <span class="phase-number">2</span>
        <span class="phase-text">Competitor & Ad Enrichment</span>
    </div>
    """, unsafe_allow_html=True)

    if enable_scraping:
        total_leads = len(df)
        enrich_progress = st.progress(0, text=f"🌐 Competitor Detection — scanning 0/{total_leads} sites...")

        def update_enrich(current, total):
            pct = current / total if total > 0 else 1
            enrich_progress.progress(pct, text=f"🌐 Competitor Detection — {current}/{total} sites scanned")

        df = enrich_dataframe(df, enable_scraping=True, progress_callback=update_enrich)
        comps_found = (df["competitors_bnpl"] != "").sum()
        enrich_progress.progress(1.0, text=f"✓ Competitor Detection — BNPL found on {comps_found}/{total_leads} sites")
    else:
        df = enrich_dataframe(df, enable_scraping=False)
        st.info("Competitor detection disabled — enable in sidebar for whitespace scoring")

    # ── PHASE 3-5: SCORING ──
    st.markdown("""
    <div class="phase-header">
        <span class="phase-number">3</span>
        <span class="phase-text">Scoring & Tier Assignment</span>
    </div>
    """, unsafe_allow_html=True)
    score_progress = st.progress(0, text="🧮 Computing scores...")
    score_progress.progress(0.3, text=f"🧮 Assigning tiers to {len(df)} leads...")
    df = score_dataframe(df)
    score_progress.progress(0.8, text="🧮 Applying filters...")

    if exclude_won and "lead_warmth" in df.columns:
        df = df[df["lead_warmth"] != "Existing Client"].reset_index(drop=True)

    score_progress.progress(1.0, text=f"✓ Scoring complete — {len(df)} leads ready")
    return df


# ── GENERATE BUTTON ─────────────────────────────────────────
col_btn1, col_btn2, _ = st.columns([1, 1, 3])
with col_btn1:
    generate = st.button("Generate Territory List", type="primary", use_container_width=True)
with col_btn2:
    if "result_df" in st.session_state and st.session_state.result_df is not None:
        clear = st.button("Clear Results", use_container_width=True)
        if clear:
            st.session_state.result_df = None
            st.rerun()

if generate:
    result = run_pipeline()
    if result is not None:
        st.session_state.result_df = result

        try:
            import pathlib
            exports_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "exports"
            exports_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            excel_path = exports_dir / f"territory_{ts}.xlsx"
            result.to_excel(str(excel_path), index=False, engine="xlsxwriter")
            csv_path = exports_dir / f"territory_{ts}.csv"
            result.to_csv(str(csv_path), index=False)
            st.success(f"Auto-saved to `exports/territory_{ts}.xlsx`")
        except Exception as e:
            st.warning(f"Auto-save failed: {e}")

# ── RESULTS DISPLAY ─────────────────────────────────────────
if "result_df" in st.session_state and st.session_state.result_df is not None:
    df = st.session_state.result_df

    st.markdown('<hr style="border:none; border-top:1px solid #F0D5CE; margin: 28px 0;">', unsafe_allow_html=True)

    def render_kpi_cards(data):
        k1, k2, k3, k4, k5 = st.columns(5)
        n_leads = len(data)
        gold_n = (data["tier"] == "GOLD").sum() if "tier" in data.columns else 0
        avg_s = data["Sales_Priority_Score"].mean() if "Sales_Priority_Score" in data.columns and n_leads > 0 else 0
        ttv = data["est_ttv_annual_eur"].sum() if "est_ttv_annual_eur" in data.columns else 0
        ttv_str = f"€{ttv/1_000_000:.1f}M" if ttv >= 1_000_000 else f"€{ttv:,.0f}"
        ws_n = data["is_whitespace"].sum() if "is_whitespace" in data.columns else 0
        with k1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{n_leads}</div><div class="kpi-label">Total Leads</div></div>', unsafe_allow_html=True)
        with k2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value kpi-value-gold">{gold_n}</div><div class="kpi-label">Gold Tier</div></div>', unsafe_allow_html=True)
        with k3:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value kpi-value-coral">{avg_s:.1f}</div><div class="kpi-label">Avg Score</div></div>', unsafe_allow_html=True)
        with k4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value kpi-value-green">{ttv_str}</div><div class="kpi-label">Est. TTV</div></div>', unsafe_allow_html=True)
        with k5:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value kpi-value-coral">{ws_n}</div><div class="kpi-label">Whitespace</div></div>', unsafe_allow_html=True)

    # ── Tabs
    tab_full, tab_gold, tab_whitespace, tab_country = st.tabs([
        "Full List", "Gold Tier", "Whitespace", "By Country"
    ])

    display_cols = [
        "domain", "country", "tier", "scalapay_category", "segment", "Sales_Priority_Score",
        "lead_warmth", "est_ttv_annual_eur", "est_mr_annual_eur",
    ]
    for col in ["category", "industry", "monthly_traffic", "annual_revenue_bucket",
                 "employees_bucket", "yoy_growth", "mom_growth", "email",
                 "competitors_bnpl", "competitors_count", "psp_detected",
                 "pipeline", "deal_stage", "deal_owner", "is_won", "is_in_pipeline",
                 "hs_cross_country", "in_hubspot_sw", "is_whitespace", "has_only_paypal_bnpl",
                 "is_actionable", "bnpl_penetration_pct", "growth_score", "is_advertising_heavy",
                 "channel_type", "hq_country", "top_country"]:
        if col in df.columns:
            display_cols.append(col)

    display_cols = [c for c in display_cols if c in df.columns]

    with tab_full:
        render_kpi_cards(df)
        st.markdown("")
        st.dataframe(
            df[display_cols],
            use_container_width=True,
            height=500,
            column_config={
                "Sales_Priority_Score": st.column_config.ProgressColumn(
                    "Priority Score",
                    min_value=0,
                    max_value=100,
                    format="%.1f",
                ),
                "est_ttv_annual_eur": st.column_config.NumberColumn(
                    "Est. TTV (Annual €)",
                    format="€%d",
                ),
                "est_mr_annual_eur": st.column_config.NumberColumn(
                    "Est. MR (Annual €)",
                    format="€%d",
                ),
                "monthly_traffic": st.column_config.NumberColumn(
                    "Monthly Traffic",
                    format="%d",
                ),
            },
        )

    with tab_gold:
        gold_df = df[df["tier"] == "GOLD"] if "tier" in df.columns else df
        render_kpi_cards(gold_df)
        st.markdown("")
        st.dataframe(gold_df[display_cols], use_container_width=True, height=400)

    with tab_whitespace:
        if "is_whitespace" in df.columns:
            ws_df = df[df["is_whitespace"] == True]
            if ws_df.empty:
                st.info("No whitespace opportunities found. Enable competitor detection for whitespace scoring.")
            else:
                render_kpi_cards(ws_df)
                st.markdown("")
                st.dataframe(ws_df[display_cols], use_container_width=True, height=400)
        else:
            st.info("Enable competitor detection in the sidebar to see whitespace opportunities.")

    with tab_country:
        for country in countries:
            if "country" in df.columns:
                cdf = df[df["country"] == country]
                label = "🇪🇸🇵🇹 Iberia (ES + PT)" if country == "IB" else "🇫🇷 France"
                st.markdown(f"#### {label}")
                render_kpi_cards(cdf)
                st.markdown("")
                st.dataframe(cdf[display_cols], use_container_width=True, height=400)

    # ── EXCEL EXPORT ──
    st.markdown('<hr style="border:none; border-top:1px solid #F0D5CE; margin: 28px 0;">', unsafe_allow_html=True)

    def generate_excel(dataframe: pd.DataFrame) -> bytes:
        output = io.BytesIO()

        priority_cols = [
            "domain", "country", "tier", "scalapay_category", "segment", "Sales_Priority_Score",
            "pipeline", "deal_stage", "deal_owner", "is_won", "is_in_pipeline",
            "lead_warmth", "channel_type",
            "est_ttv_annual_eur", "est_mr_annual_eur",
            "industry", "annual_revenue_bucket", "monthly_traffic",
            "yoy_growth", "mom_growth",
            "competitors_bnpl", "psp_detected",
            "is_whitespace", "is_advertising_heavy",
            "hs_company_name", "hs_cross_country",
            "email", "hq_country", "top_country",
        ]
        ordered = [c for c in priority_cols if c in dataframe.columns]
        remaining = [c for c in dataframe.columns if c not in ordered]
        dataframe = dataframe[ordered + remaining]

        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            dataframe.to_excel(writer, sheet_name="Territory List", index=False)
            wb = writer.book
            ws = writer.sheets["Territory List"]

            header_fmt = wb.add_format({
                "bold": True, "bg_color": "#EA5440", "font_color": "#FFFFFF",
                "border": 1, "text_wrap": True, "valign": "vcenter",
                "font_name": "Arial", "font_size": 10,
            })
            money_fmt = wb.add_format({"num_format": '#,##0 €', "font_name": "Arial", "font_size": 10})
            number_fmt = wb.add_format({"num_format": "#,##0", "font_name": "Arial", "font_size": 10})
            pct_fmt = wb.add_format({"num_format": "0.0%", "font_name": "Arial", "font_size": 10})
            score_fmt = wb.add_format({"num_format": "0.0", "bold": True, "font_name": "Arial", "font_size": 10})
            text_fmt = wb.add_format({"font_name": "Arial", "font_size": 10})

            for col_num, col_name in enumerate(dataframe.columns):
                clean = col_name.replace("_", " ").replace("est ", "Est. ").title()
                for old, new in [("Ttv","TTV"),("Eur","(EUR)"),("Mr ","MR "),("Bnpl","BNPL"),
                                 ("Psp","PSP"),("Yoy","YoY"),("Mom","MoM"),("Hs ","HubSpot ")]:
                    clean = clean.replace(old, new)
                ws.write(0, col_num, clean, header_fmt)

            for i, col in enumerate(dataframe.columns):
                max_len = max(dataframe[col].astype(str).str.len().max(), len(col))
                width = min(max_len + 3, 35)
                if "ttv" in col or "mr_" in col:
                    ws.set_column(i, i, 18, money_fmt)
                elif col in ("monthly_traffic", "total_page_views", "avg_monthly_visits"):
                    ws.set_column(i, i, 16, number_fmt)
                elif col in ("yoy_growth", "mom_growth", "bnpl_penetration_pct"):
                    ws.set_column(i, i, 12, pct_fmt)
                elif col == "Sales_Priority_Score":
                    ws.set_column(i, i, 14, score_fmt)
                else:
                    ws.set_column(i, i, width, text_fmt)

            ws.freeze_panes(1, 2)
            ws.autofilter(0, 0, len(dataframe), len(dataframe.columns) - 1)

            if "Sales_Priority_Score" in dataframe.columns:
                si = dataframe.columns.get_loc("Sales_Priority_Score")
                ws.conditional_format(1, si, len(dataframe), si,
                    {"type": "3_color_scale", "min_color": "#fca5a5",
                     "mid_color": "#fde68a", "max_color": "#86efac"})

            if "tier" in dataframe.columns:
                ti = dataframe.columns.get_loc("tier")
                for val, bg, fg in [("GOLD","#fef3c7","#92400e"),("SILVER","#e2e8f0","#334155"),("BRONZE","#fed7aa","#9a3412")]:
                    fmt = wb.add_format({"bg_color": bg, "font_color": fg, "font_name": "Arial", "font_size": 10})
                    ws.conditional_format(1, ti, len(dataframe), ti,
                        {"type": "text", "criteria": "containing", "value": val, "format": fmt})

            def write_sub(data, name):
                data.to_excel(writer, sheet_name=name, index=False)
                sws = writer.sheets[name]
                for cn, cname in enumerate(data.columns):
                    sws.write(0, cn, cname.replace("_", " ").title(), header_fmt)
                sws.freeze_panes(1, 2)
                sws.autofilter(0, 0, len(data), len(data.columns) - 1)

            if "tier" in dataframe.columns:
                write_sub(dataframe[dataframe["tier"] == "GOLD"], "Gold Tier")

            if "is_whitespace" in dataframe.columns:
                wsd = dataframe[dataframe["is_whitespace"] == True]
                if not wsd.empty:
                    write_sub(wsd, "Whitespace")

            if "country" in dataframe.columns:
                for c in dataframe["country"].unique():
                    label = "Iberia" if c == "IB" else c
                    write_sub(dataframe[dataframe["country"] == c], f"Territory {label}"[:31])

        output.seek(0)
        return output.getvalue()

    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        excel_bytes = generate_excel(df)
        st.download_button(
            label="Download Excel Report",
            data=excel_bytes,
            file_name=f"scalapay_territory_list_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )
    with col_dl2:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name=f"scalapay_territory_list_{timestamp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # ── Score Breakdown
    with st.expander("Score Breakdown & Methodology"):
        st.markdown("""
        **Sales Priority Score** is a composite 0–100 score built from five weighted components:

        | Component | Max pts | What it measures |
        |-----------|---------|------------------|
        | **Industry Tier** | 25 | BNPL conversion fit by vertical and country. Gold = highest margin. |
        | **Penetration / TTV** | 20 | Revenue potential: BNPL penetration × estimated transaction value. |
        | **Traffic Growth** | 20 | YoY (60%) + MoM (40%) momentum. Proxy for merchant ad investment. |
        | **Lead Warmth** | 15 | CRM status from HubSpot: Active Pipeline → Net New → Lost. |
        | **Whitespace** | 20 | No BNPL competitor detected = greenfield opportunity. |

        Weights are adjustable in the sidebar. Formula: (raw points / 95) × 100.
        """)

else:
    # Landing state
    st.markdown("""
    <div class="landing-container">
        <div class="landing-icon">🎯</div>
        <p class="landing-title">Upload Similarweb data & hit Generate</p>
        <p class="landing-desc">
            Configure integrations and scoring weights in the sidebar,
            or check "Use sample data" for a quick demo.
        </p>
    </div>
    """, unsafe_allow_html=True)

# ── Footer ──
st.markdown("""
<div class="te-footer">
    <div class="te-footer-left">Territory Engine v2.0 — RevOps & Strategy</div>
    <div class="te-footer-logo">
        Built with <span class="scalapay-heart">♥</span> <strong>scalapay</strong>
    </div>
</div>
""", unsafe_allow_html=True)
