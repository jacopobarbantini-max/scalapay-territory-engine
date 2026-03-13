"""
app.py — Scalapay Territory Engine v4 (Streamlit)
IT/FR/IB tiering, real SW transactions, travel sub-categories, HubSpot bulk-fetch.
"""
import io, json, os, logging
from datetime import datetime
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

from config import SCORING_WEIGHTS, WHITESPACE_CATEGORIES
from similarweb_client import ingest
from hubspot_client import enrich_with_hubspot
from enrichment import enrich_dataframe
from scoring import score_dataframe
from utils import get_logger

# ── LOG CAPTURE ────────────────────────────────────────────
# Capture all app logs into a list for display
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        if "log_messages" not in st.session_state:
            st.session_state.log_messages = []
    def emit(self, record):
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        msg = f"[{ts}] {record.name} — {record.getMessage()}"
        if record.levelno >= logging.ERROR:
            msg = f"❌ {msg}"
        elif record.levelno >= logging.WARNING:
            msg = f"⚠️ {msg}"
        else:
            msg = f"✅ {msg}"
        st.session_state.log_messages.append(msg)

_log_handler = StreamlitLogHandler()
_log_handler.setLevel(logging.INFO)
for logger_name in ["app", "enrichment", "hubspot_client", "scoring", "utils", "similarweb_client", "similarweb_cookies"]:
    lg = logging.getLogger(logger_name)
    lg.addHandler(_log_handler)
    lg.setLevel(logging.INFO)

log = get_logger("app")
FLAGS = {"ES": "🇪🇸", "FR": "🇫🇷", "PT": "🇵🇹", "IT": "🇮🇹"}

st.set_page_config(page_title="Scalapay Territory Engine", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .stApp { background-color: #0a0a0f; }
    section[data-testid="stSidebar"] { background-color: #111118; }
    .main-header { background: linear-gradient(135deg, #6C3AED 0%, #2563EB 50%, #0EA5E9 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.4rem; font-weight: 800; margin-bottom: 0; }
    .sub-header { color: #94a3b8; font-size: 1.05rem; margin-top: -8px; margin-bottom: 24px; }
    .metric-card { background: linear-gradient(135deg, #1e1b4b 0%, #172554 100%); border: 1px solid #334155; border-radius: 12px; padding: 20px; text-align: center; }
    .metric-value { font-size: 2rem; font-weight: 700; color: #e2e8f0; }
    .metric-label { color: #94a3b8; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .tier-gold { color: #fbbf24; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">🎯 Scalapay Territory Engine</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Lead scoring & territory lists — IT / FR / IB · v4.0</p>', unsafe_allow_html=True)

# ── SIDEBAR ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Pipeline Configuration")

    st.markdown("---")
    st.markdown("**📊 Data Source**")
    data_mode = st.radio("Similarweb input", [
        "📁 Upload (XLSX/CSV)",
        "🔄 Reload Export (add CRM)",
        "🔌 Pro API (Similarweb)",
        "🧪 Demo (sample data)",
    ], index=0)
    use_sample = "Demo" in data_mode
    use_reload = "Reload" in data_mode
    use_pro_api = "Pro API" in data_mode

    # Initialize all file variables
    file_ib = file_fr = file_it = reload_file = None
    api_filters = None
    page_size = 100
    max_pages = 1
    pro_api_country = "IT"

    if use_reload:
        st.markdown("---")
        st.markdown("**📂 Upload Previous Export**")
        st.caption("Upload a scored Excel export to add HubSpot CRM enrichment on top. Keeps all existing scraping data.")
        reload_file = st.file_uploader("Previously scored .xlsx", type=["xlsx","xls"], key="reload")
    elif use_pro_api:
        st.markdown("---")
        st.markdown("**🔌 Pro API Settings**")
        pro_api_country = st.selectbox("Territory", ["IT", "FR", "ES", "IB"], index=0)
        page_size = st.number_input("Results per page", value=100, min_value=10, max_value=500, step=10)
        max_pages = st.number_input("Max pages", value=1, min_value=1, max_value=50, step=1)

        # ── Tag Filter ──────────────────────────────────────
        api_filters = None
        _tags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bnpl_tags_by_category.json")
        if os.path.exists(_tags_path):
            st.markdown("### Filtro Prodotti (Tags)")
            if "bnpl_tags_data" not in st.session_state:
                with open(_tags_path) as _f:
                    st.session_state["bnpl_tags_data"] = json.load(_f)
            _tags_data = st.session_state["bnpl_tags_data"]

            _all_categories = sorted(_tags_data.keys())
            selected_categories = st.multiselect("Categorie Scalapay", _all_categories, default=[])

            # Dynamic subcategories
            _available_subs = {}
            for cat in selected_categories:
                for sub in _tags_data.get(cat, {}):
                    _available_subs[sub] = cat
            selected_subcategories = []
            if _available_subs:
                selected_subcategories = st.multiselect(
                    "Sottocategorie",
                    sorted(_available_subs.keys()),
                    default=sorted(_available_subs.keys()),
                )

            # Collect tags from selected subcategories
            _selected_tags = set()
            for cat in selected_categories:
                subs = _tags_data.get(cat, {})
                for sub_name, sub_tags in subs.items():
                    if not selected_subcategories or sub_name in selected_subcategories:
                        _selected_tags.update(sub_tags)

            # Free-text search
            _free_text = st.text_input("Ricerca libera tag", "", help="Cerca tra tutti i ~280k tags Similarweb")
            if _free_text.strip():
                _all_tags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "similarweb_all_tags.json")
                if os.path.exists(_all_tags_path):
                    if "all_sw_tags" not in st.session_state:
                        with open(_all_tags_path) as _f2:
                            st.session_state["all_sw_tags"] = json.load(_f2)
                    _query = _free_text.strip().lower()
                    _matches = [t for t in st.session_state["all_sw_tags"] if _query in t.lower()]
                    if _matches:
                        _picked = st.multiselect(f"{len(_matches)} tags trovati", _matches[:200], default=[])
                        _selected_tags.update(_picked)

            _selected_tags = sorted(_selected_tags)
            if _selected_tags:
                st.info(f"{len(_selected_tags)} tags selezionati")
                with st.expander("Preview tags"):
                    st.write(", ".join(_selected_tags[:50]))
                    if len(_selected_tags) > 50:
                        st.caption(f"... e altri {len(_selected_tags) - 50}")
                api_filters = {"siteTags": _selected_tags}
    elif not use_sample:
        st.markdown("---")
        st.markdown("**📂 Upload Similarweb Exports**")
        file_ib = st.file_uploader("🇪🇸 Iberia (ES)", type=["csv","xlsx","xls"], key="sw_ib")
        file_fr = st.file_uploader("🇫🇷 France", type=["csv","xlsx","xls"], key="sw_fr")
        file_it = st.file_uploader("🇮🇹🇵🇹 Italy / Portugal (IT tiering)", type=["csv","xlsx","xls"], key="sw_it")

    st.markdown("---")
    st.markdown("**🔌 Integrations**")

    enable_hubspot = st.checkbox(
        "🔗 HubSpot CRM cross-check",
        value=bool(os.getenv("HUBSPOT_API_KEY")),
        help="Bulk-fetches ALL companies from HubSpot CRM (~200 API calls). Matches leads by domain + brand root for cross-country detection (e.g. zooplus.es → zooplus.it). Classifies warmth: Warm / Net New / Cold-Lost / Won. Requires HUBSPOT_API_KEY in .env.",
    )
    enable_scraping = st.checkbox(
        "🔍 Checkout / competitor scraping",
        value=False,
        help="Visits each merchant's homepage and scans for BNPL provider scripts (Klarna, Alma, Oney, etc.) and PSP integrations (Stripe, Adyen, Checkout.com). Detects competitors already active. Slower: ~5s per domain.",
    )
    enable_ads = st.checkbox(
        "📡 Ad pixel detection",
        value=False,
        help="Scans homepages for Meta Pixel (fbq) and Google Ads (gtag/AW-) tags. Merchants with active pixels are likely spending on acquisition = higher conversion intent. Requires scraping enabled.",
    )

    st.markdown("---")
    st.markdown("**📋 Filters**")
    min_traffic = st.number_input("Min monthly traffic", value=0, step=50_000)
    exclude_won = st.checkbox("Exclude 'Existing Won' deals", value=True)

    st.markdown("---")
    st.markdown("**🎚️ Score Weights**")
    st.caption("Each slider 0–100. Sum should equal 100%. Exceeding 100% will show a warning.")

    w_tier = st.slider("Tier — country-specific risk mapping (IT/FR/IB). Travel: 14 sub-categories.", 0, 100, 25, step=5, help="Gold/Silver/Bronze. Low risk = high CM.")
    w_pen = st.slider("Penetration / TTV — BNPL adoption × estimated merchant revenue.", 0, 100, 25, step=5, help="Higher penetration + bigger MR = higher score.")
    w_growth = st.slider("Growth — YoY (60%) + MoM (40%) traffic momentum.", 0, 100, 15, step=5, help=">50% YoY = max. Negative growth reduces score.")
    w_warmth = st.slider("Approachability — CRM status (Net New > Lost>6mo > Stale > Cold < Warm).", 0, 100, 20, step=5, help="Net New=max. Stale deal (no contact 45d)=re-approachable. Warm=colleague working it.")
    w_mkt = st.slider("Market Opportunity — BNPL competition at checkout.", 0, 100, 15, step=5, help="TOP=no BNPL. MEDIUM-HIGH=1 minor. MEDIUM=1 direct. LOW=3+ saturated.")

    w_sum = w_tier + w_pen + w_growth + w_warmth + w_mkt
    if w_sum > 100:
        st.error(f"⚠️ Weights sum: {w_sum}% — exceeds 100%! Reduce some weights.")
    elif w_sum == 100:
        st.success(f"✅ Weights sum: {w_sum}%")
    else:
        st.warning(f"⚡ Weights sum: {w_sum}% — below 100%, scores won't use full range.")


# ── PIPELINE ────────────────────────────────────────────────
def load_sample_data(country):
    path = f"sample_data/similarweb_sample_{country.lower()}.csv"
    try:
        df = pd.read_csv(path)
        df["country"] = country.upper()
        from utils import clean_similarweb_df
        return clean_similarweb_df(df)
    except Exception as e:
        st.error(f"Sample data error for {country}: {e}")
        return pd.DataFrame()


def show_scraping_report(df):
    """Show enrichment performance report — honest numbers."""
    st.markdown("### 📡 Enrichment Performance Report")

    total = len(df)
    if total == 0:
        return

    # Dedicated BNPL (excludes PayPal)
    has_bnpl = (df["competitors_bnpl"].astype(str).str.strip() != "").sum() if "competitors_bnpl" in df.columns else 0
    has_paypal = df["has_paypal"].sum() if "has_paypal" in df.columns else 0
    paypal_only = int(has_paypal) - has_bnpl  # PayPal but no dedicated BNPL
    if paypal_only < 0: paypal_only = 0
    no_bnpl = total - has_bnpl - paypal_only

    # PSP & pixels
    has_psp = (df["psp_detected"].astype(str).str.strip() != "").sum() if "psp_detected" in df.columns else 0
    has_meta = df["has_meta_pixel"].sum() if "has_meta_pixel" in df.columns else 0
    has_gads = df["has_google_ads"].sum() if "has_google_ads" in df.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("🎯 Dedicated BNPL", f"{has_bnpl}/{total}", f"{has_bnpl/total*100:.1f}% hit rate")
    with c2:
        st.metric("💳 PayPal Only", f"{paypal_only}", f"Not a real competitor")
    with c3:
        st.metric("📘 Meta Pixel", f"{int(has_meta)}/{total}", f"{has_meta/total*100:.1f}%")
    with c4:
        st.metric("📊 Google Ads", f"{int(has_gads)}/{total}", f"{has_gads/total*100:.1f}%")

    st.caption(f"🟢 {no_bnpl} merchants with NO dedicated BNPL at checkout (best opportunity)")

    # Competitor breakdown
    if has_bnpl > 0 and "competitors_bnpl" in df.columns:
        st.markdown("**Dedicated BNPL competitors found:**")
        all_comps = []
        for val in df["competitors_bnpl"].dropna():
            if str(val).strip():
                all_comps.extend([c.strip() for c in str(val).split(",") if c.strip()])
        if all_comps:
            from collections import Counter
            comp_counts = Counter(all_comps)
            comp_df = pd.DataFrame(comp_counts.most_common(15), columns=["Competitor", "Merchants"])
            st.dataframe(comp_df, use_container_width=True, height=200)

    # Market opportunity breakdown
    opp_counts = df["opportunity_level"].value_counts().to_dict() if "opportunity_level" in df.columns else {}
    if opp_counts:
        st.markdown("**Market Opportunity distribution:**")
        for level in ["TOP", "MEDIUM-HIGH", "MEDIUM", "LOW"]:
            n = opp_counts.get(level, 0)
            pct = n / total * 100
            bar = "█" * int(pct / 2)
            color = {"TOP": "🟢", "MEDIUM-HIGH": "🔵", "MEDIUM": "🟡", "LOW": "🔴"}.get(level, "⚪")
            st.caption(f"{color} **{level}**: {n} ({pct:.1f}%) {bar}")

    # Sites not reached
    if has_comp == 0 and has_psp == 0:
        st.warning("⚠️ No enrichment data found — scraping may not have been enabled for this export.")
    else:
        unreached = total - max(has_comp, has_psp, int(has_meta), int(has_gads))
        if unreached > 0:
            st.caption(f"ℹ️ ~{unreached} sites ({unreached/total*100:.0f}%) returned no detectable data (Cloudflare blocks, SPAs, or no BNPL/PSP present)")


def run_reload_pipeline():
    """Reload a previous export, add HubSpot CRM, re-score."""
    st.session_state.log_messages = []
    weights = {
        "tier": w_tier, "penetration": w_pen,
        "growth": w_growth, "warmth": w_warmth, "market_opportunity": w_mkt,
    }
    if w_sum > 100:
        st.error("Cannot generate: weights exceed 100%.")
        return None

    st.markdown("### 📥 Phase 1 — Loading Previous Export")
    try:
        xls = pd.ExcelFile(reload_file)
        # Read main sheet
        if "All Leads" in xls.sheet_names:
            df = pd.read_excel(reload_file, sheet_name="All Leads")
        elif "All" in xls.sheet_names:
            df = pd.read_excel(reload_file, sheet_name="All")
        else:
            df = pd.read_excel(reload_file, sheet_name=0)
        st.success(f"✅ Loaded {len(df)} leads from export ({len(df.columns)} columns)")
    except Exception as e:
        st.error(f"Failed to read export: {e}")
        return None

    # Show what enrichment data already exists
    has_scraping = "competitors_bnpl" in df.columns and (df["competitors_bnpl"].astype(str).str.strip() != "").any()
    has_hs = "hs_exists" in df.columns and df["hs_exists"].any()

    if has_scraping:
        st.info(f"✅ Export has scraping data — {(df['competitors_bnpl'].astype(str).str.strip() != '').sum()} merchants with competitor info")
    else:
        st.caption("ℹ️ No scraping data in export")

    if has_hs:
        st.info(f"✅ Export already has HubSpot data — {df['hs_exists'].sum()} matches")
    else:
        st.caption("ℹ️ No HubSpot data in export — will add now")

    # Show scraping report if data exists
    if has_scraping:
        show_scraping_report(df)

    # ── PHASE 2: HUBSPOT ─────────────────────────────
    st.markdown("### 🔗 Phase 2 — HubSpot CRM Cross-Check")
    if enable_hubspot:
        with st.spinner("🔄 HubSpot bulk fetch & matching..."):
            # Remove old HubSpot columns if they exist
            hs_cols = [c for c in df.columns if c.startswith("hs_") or c == "lead_warmth"]
            df = df.drop(columns=[c for c in hs_cols if c in df.columns], errors='ignore')

            df = enrich_with_hubspot(df)
            hs_found = df["hs_exists"].sum() if "hs_exists" in df.columns else 0
            st.success(f"✅ HubSpot: {hs_found} matches found")

            # Show warmth distribution
            if "lead_warmth" in df.columns:
                warmth = df["lead_warmth"].value_counts()
                st.markdown("**Approachability distribution:**")
                for status, count in warmth.items():
                    st.caption(f"  {status}: {count}")
    else:
        if "lead_warmth" not in df.columns:
            df["lead_warmth"] = "Net New"
        st.info("ℹ️ HubSpot disabled — keeping existing warmth data or defaulting to Net New")

    # ── PHASE 3: RE-SCORE ─────────────────────────────
    st.markdown("### 🧮 Phase 3 — Re-Scoring")
    with st.spinner("Re-computing scores with CRM data..."):
        # Remove old scores
        for col in ["Sales_Priority_Score", "warmth_score", "market_opportunity_score",
                     "tier_score", "penetration_score", "growth_score", "account_segment",
                     "opportunity_level", "n_competitors", "has_direct_competitor", "competitors_list"]:
            if col in df.columns:
                df = df.drop(columns=[col])
        df = score_dataframe(df, weights)

    if exclude_won and "lead_warmth" in df.columns:
        before = len(df)
        df = df[df["lead_warmth"] != "Existing Won"].reset_index(drop=True)
        excluded = before - len(df)
        if excluded > 0:
            st.caption(f"Excluded {excluded} 'Existing Won' merchants")

    st.success(f"✅ **{len(df)} leads re-scored** with CRM data")
    return df


def run_pipeline():
    # Clear previous logs
    st.session_state.log_messages = []
    # Build weights dict
    weights = {
        "tier": w_tier, "penetration": w_pen,
        "growth": w_growth, "warmth": w_warmth, "market_opportunity": w_mkt,
    }
    if w_sum > 100:
        st.error("Cannot generate: weights exceed 100%.")
        return None

    # ── PHASE 1: INGEST ─────────────────────────────
    st.markdown("### 📥 Phase 1 — Data Ingestion")
    progress = st.progress(0, text="Loading data...")
    all_dfs = []

    if use_sample:
        for country in ["ES", "FR"]:
            df = load_sample_data(country)
            if not df.empty:
                all_dfs.append(df)
                st.success(f"✅ {FLAGS.get(country,'')} {country}: {len(df)} demo leads")
    elif use_pro_api:
        with st.spinner(f"🔌 Fetching from Pro API ({pro_api_country})..."):
            df = ingest(
                pro_api_country,
                use_pro_api=True,
                page_size=page_size,
                max_pages=max_pages,
                filters=api_filters,
            )
            if not df.empty:
                all_dfs.append(df)
                st.success(f"✅ {FLAGS.get(pro_api_country,'')} {pro_api_country}: {len(df)} leads from Pro API")
            else:
                st.warning(f"⚠️ Pro API returned no results. Check cookies and filters.")
        progress.progress(1.0)
    else:
        uploads = []
        if file_ib: uploads.append((file_ib, "ES"))
        if file_fr: uploads.append((file_fr, "FR"))
        if file_it: uploads.append((file_it, "IT"))

        for i, (f, country) in enumerate(uploads):
            df = ingest(country, uploaded_file=f)
            if not df.empty:
                all_dfs.append(df)
                st.success(f"✅ {FLAGS.get(country,'')} {country}: {len(df)} leads loaded")
            else:
                st.warning(f"⚠️ {country}: No data in file")
            progress.progress((i + 1) / max(len(uploads), 1))

    if not all_dfs:
        st.error("No data to process. Upload files or enable demo mode.")
        return None

    df = pd.concat(all_dfs, ignore_index=True)
    st.info(f"📊 **{len(df)} total leads** across {', '.join(df['country'].unique())}")

    # Traffic filter
    if min_traffic > 0 and "monthly_traffic" in df.columns:
        before = len(df)
        df = df[df["monthly_traffic"] >= min_traffic].reset_index(drop=True)
        if before - len(df) > 0:
            st.caption(f"Filtered {before - len(df)} leads below {min_traffic:,} traffic")

    progress.progress(1.0, text="Phase 1 complete ✓")

    # ── PHASE 2: HUBSPOT ─────────────────────────────
    st.markdown("### 🔗 Phase 2 — HubSpot Cross-Check")
    if enable_hubspot:
        with st.spinner("🔄 HubSpot bulk fetch & matching..."):
            df = enrich_with_hubspot(df)
            hs_found = df["hs_exists"].sum() if "hs_exists" in df.columns else 0
            st.success(f"✅ HubSpot: {hs_found} matches (bulk mode, ~200 API calls)")
    else:
        df["hs_exists"] = False
        df["hs_company_name"] = ""
        df["hs_deal_stage"] = ""
        df["hs_deal_owner"] = ""
        df["hs_cross_country"] = False
        df["hs_it_deal_found"] = False
        df["lead_warmth"] = "Net New"
        st.info("ℹ️ HubSpot disabled — all leads classified as Net New")

    # ── PHASE 3: ENRICHMENT ──────────────────────────
    st.markdown("### 🔍 Phase 3 — Competitor & Ad Enrichment")
    if enable_scraping:
        ep = st.progress(0, text="Scraping merchant sites...")
        df = enrich_dataframe(df, enable_scraping=True,
                              progress_callback=lambda c, t: ep.progress(c / t, text=f"Enriching {c}/{t}..."))
        ep.progress(1.0, text="Enrichment complete ✓")
        comps = (df["competitors_bnpl"] != "").sum()
        st.success(f"✅ Competitor data for {comps} merchants")
        show_scraping_report(df)
    else:
        df = enrich_dataframe(df, enable_scraping=False)
        st.info("ℹ️ Scraping disabled — competitor & ad columns empty")

    # ── PHASE 4: SCORING ─────────────────────────────
    st.markdown("### 🧮 Phase 4 — Scoring & Tiering")
    with st.spinner("Computing scores with real SW transactions..."):
        df = score_dataframe(df, weights)

    if exclude_won and "lead_warmth" in df.columns:
        df = df[df["lead_warmth"] != "Existing Won"].reset_index(drop=True)

    # Stats
    sw_txn = (df.get("ttv_source", pd.Series(dtype=str)) == "SW").sum()
    cr_fb = (df.get("ttv_source", pd.Series(dtype=str)) == "CR").sum()
    st.success(f"✅ **{len(df)} scored leads** | TTV source: {sw_txn} from SW transactions, {cr_fb} from CR fallback")
    return df


# ── GENERATE BUTTON ─────────────────────────────────────────
col1, col2, _ = st.columns([1, 1, 3])
with col1:
    btn_label = "🔄 Reload + CRM" if use_reload else "🚀 Generate Territory List"
    can_run = (use_sample or use_pro_api or (use_reload and reload_file) or file_ib or file_fr or file_it) and w_sum <= 100
    generate = st.button(btn_label, type="primary", use_container_width=True, disabled=not can_run)
with col2:
    if "result_df" in st.session_state and st.session_state.result_df is not None:
        if st.button("🗑️ Clear Results", use_container_width=True):
            st.session_state.result_df = None
            st.rerun()

if generate:
    if use_reload:
        result = run_reload_pipeline()
    else:
        result = run_pipeline()
    if result is not None:
        st.session_state.result_df = result

# ── RESULTS ─────────────────────────────────────────────────
if "result_df" in st.session_state and st.session_state.result_df is not None:
    df = st.session_state.result_df

    st.markdown("---")
    st.markdown("### 📊 Results Dashboard")

    # KPIs
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{len(df)}</div><div class="metric-label">Total Leads</div></div>', unsafe_allow_html=True)
    with c2:
        gold = (df["tier"] == "GOLD").sum() if "tier" in df.columns else 0
        st.markdown(f'<div class="metric-card"><div class="metric-value tier-gold">{gold}</div><div class="metric-label">Gold Tier</div></div>', unsafe_allow_html=True)
    with c3:
        avg = df["Sales_Priority_Score"].mean() if "Sales_Priority_Score" in df.columns else 0
        st.markdown(f'<div class="metric-card"><div class="metric-value">{avg:.1f}</div><div class="metric-label">Avg Score</div></div>', unsafe_allow_html=True)
    with c4:
        ttv = df["est_ttv_annual_eur"].sum() if "est_ttv_annual_eur" in df.columns else 0
        ttv_d = f"€{ttv/1e6:.1f}M" if ttv >= 1e6 else f"€{ttv:,.0f}"
        st.markdown(f'<div class="metric-card"><div class="metric-value">{ttv_d}</div><div class="metric-label">Total Est. TTV</div></div>', unsafe_allow_html=True)
    with c5:
        top_opp = (df["opportunity_level"] == "TOP").sum() if "opportunity_level" in df.columns else 0
        st.markdown(f'<div class="metric-card"><div class="metric-value">{top_opp}</div><div class="metric-label">TOP Opportunity</div></div>', unsafe_allow_html=True)

    # Segments
    c1, c2, c3 = st.columns(3)
    with c1:
        n = (df["account_segment"] == "Strategic").sum() if "account_segment" in df.columns else 0
        st.metric("Strategic (>€5M TTV)", n)
    with c2:
        n = (df["account_segment"] == "Enterprise").sum() if "account_segment" in df.columns else 0
        st.metric("Enterprise (€500K–5M)", n)
    with c3:
        n = (df["account_segment"] == "Executive").sum() if "account_segment" in df.columns else 0
        st.metric("Executive (<€500K)", n)

    # TTV source info
    if "ttv_source" in df.columns:
        sw_n = (df["ttv_source"] == "SW").sum()
        cr_n = (df["ttv_source"] == "CR").sum()
        st.caption(f"TTV source: {sw_n} leads from real SW transactions, {cr_n} from CR fallback")

    # Enrichment report (if scraping data exists)
    has_scraping_data = "competitors_bnpl" in df.columns and (df["competitors_bnpl"].astype(str).str.strip() != "").any()
    if has_scraping_data:
        with st.expander("📡 Enrichment Performance Report", expanded=False):
            show_scraping_report(df)

    # Approachability breakdown (if CRM data exists)
    if "lead_warmth" in df.columns:
        warmth_counts = df["lead_warmth"].value_counts()
        if len(warmth_counts) > 1 or (len(warmth_counts) == 1 and warmth_counts.index[0] != "Net New"):
            with st.expander("🔗 CRM Approachability Breakdown", expanded=False):
                for status in ["Net New", "Lost >6 months", "Stale Deal", "In HubSpot (unknown)", "Lost <6 months", "Warm (active)", "Existing Won"]:
                    n = warmth_counts.get(status, 0)
                    if n > 0:
                        pct = n / len(df) * 100
                        st.caption(f"**{status}**: {n} ({pct:.1f}%)")

    st.markdown("")

    # Display columns
    dcols = ["domain", "country", "tier", "scalapay_category", "account_segment",
             "Sales_Priority_Score", "lead_warmth", "est_ttv_annual_eur", "est_mr_annual_eur"]
    extras = ["est_monthly_txns", "aov_used", "bnpl_pen_used", "ttv_source",
              "industry", "monthly_traffic", "yoy_growth", "mom_growth", "email",
              "competitors_bnpl", "psp_detected", "has_meta_pixel", "has_google_ads",
              "hs_deal_stage", "hs_deal_owner", "hs_cross_country", "hs_it_deal_found",
              "in_hubspot_sw", "opportunity_level", "competitors_list", "is_actionable", "growth_score",
              "hq_country", "top_country"]
    for c in extras:
        if c in df.columns:
            dcols.append(c)
    dcols = [c for c in dcols if c in df.columns]

    col_config = {
        "Sales_Priority_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.1f"),
        "est_ttv_annual_eur": st.column_config.NumberColumn("TTV Annual €", format="€%d"),
        "est_mr_annual_eur": st.column_config.NumberColumn("MR Annual €", format="€%d"),
        "monthly_traffic": st.column_config.NumberColumn("Traffic", format="%d"),
    }

    # Tabs
    tabs = st.tabs(["📋 Full List", "🥇 Gold Tier", "🎯 TOP Opportunity", "🌍 By Country"])

    with tabs[0]:
        st.dataframe(df[dcols], use_container_width=True, height=500, column_config=col_config)
    with tabs[1]:
        gold_df = df[df["tier"] == "GOLD"] if "tier" in df.columns else df
        st.dataframe(gold_df[dcols], use_container_width=True, height=400, column_config=col_config)
    with tabs[2]:
        if "opportunity_level" in df.columns:
            top_df = df[df["opportunity_level"] == "TOP"]
            if top_df.empty:
                st.info("No TOP opportunity leads found.")
            else:
                st.dataframe(top_df[dcols], use_container_width=True, height=400, column_config=col_config)
    with tabs[3]:
        for country in sorted(df["country"].unique()):
            cdf = df[df["country"] == country]
            st.markdown(f"#### {FLAGS.get(country, '🌍')} {country} — {len(cdf)} leads")
            st.dataframe(cdf[dcols].head(25), use_container_width=True, height=350, column_config=col_config)

    # ── EXPORT ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Export")

    def gen_excel(dataframe):
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as w:
            hdr = w.book.add_format({"bold": True, "bg_color": "#1e1b4b", "font_color": "#e2e8f0", "border": 1, "text_wrap": True})
            dataframe.to_excel(w, sheet_name="All Leads", index=False)
            ws = w.sheets["All Leads"]
            for i, col in enumerate(dataframe.columns):
                ws.write(0, i, col, hdr)
                ml = max(dataframe[col].astype(str).str.len().max(), len(col))
                ws.set_column(i, i, min(ml + 2, 30))
            if "Sales_Priority_Score" in dataframe.columns:
                si = dataframe.columns.get_loc("Sales_Priority_Score")
                ws.conditional_format(1, si, len(dataframe), si, {"type": "3_color_scale", "min_color": "#fca5a5", "mid_color": "#fde68a", "max_color": "#86efac"})
            # Extra sheets
            iberia = dataframe[dataframe["country"].isin(["ES", "PT"])]
            france = dataframe[dataframe["country"] == "FR"]
            italy = dataframe[dataframe["country"].isin(["IT"])]
            if not iberia.empty: iberia.to_excel(w, sheet_name="Iberia (ES+PT)", index=False)
            if not france.empty: france.to_excel(w, sheet_name="France", index=False)
            if not italy.empty: italy.to_excel(w, sheet_name="Italy+Portugal", index=False)
            for seg in ["Strategic", "Enterprise"]:
                sd = dataframe[dataframe["account_segment"] == seg]
                if not sd.empty: sd.to_excel(w, sheet_name=seg, index=False)
            if "tier" in dataframe.columns:
                dataframe[dataframe["tier"] == "GOLD"].to_excel(w, sheet_name="Gold Tier", index=False)
            if "opportunity_level" in dataframe.columns:
                top = dataframe[dataframe["opportunity_level"] == "TOP"]
                if not top.empty: top.to_excel(w, sheet_name="TOP Opportunity", index=False)
        out.seek(0)
        return out.getvalue()

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇️ Download Excel Report", gen_excel(df),
                           f"scalapay_territory_{ts}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           type="primary", use_container_width=True)
    with c2:
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(),
                           f"scalapay_territory_{ts}.csv", "text/csv", use_container_width=True)

    # Methodology
    with st.expander("🔬 Score Breakdown & Methodology"):
        st.markdown("""
        **Sales_Priority_Score** (0–100) = weighted composite:

        | Component | Default | What it measures |
        |-----------|---------|------------------|
        | **Tier** | 25% | Country-specific (IT/FR/IB matrix). Travel: 14 risk sub-categories |
        | **Account Size** | 20% | TTV-based: Strategic >€5M, Enterprise €500K–5M, Executive <€500K |
        | **Penetration/TTV** | 15% | BNPL adoption × estimated MR |
        | **Growth** | 15% | YoY (60%) + MoM (40%) traffic momentum |
        | **Warmth** | 10% | HubSpot deal stage |
        | **Competitor** | 10% | BNPL competitors on site |
        | **Market Opportunity** | 15% | BNPL checkout competition (TOP/MEDIUM-HIGH/MEDIUM/LOW) |

        **TTV Formula:**
        `Monthly Transactions (real from SW) × AOV (per category) × BNPL Penetration (per category × country) × 12`

        When SW transactions unavailable: `Traffic × CR (median per category) × AOV × Pen × 12`

        **Country tiering:** PT uses IT tiers. ES uses IB tiers.
        **Travel sub-categories:** OTA/Hotel = BRONZE (high default risk), Tour Operator = SILVER, Theme Parks/Ticketing/Wellness = GOLD (low risk).
        """)

else:
    st.markdown("---")
    st.markdown("""
    <div style="text-align:center;padding:60px 20px;color:#64748b;">
        <p style="font-size:3rem;margin-bottom:8px;">🎯</p>
        <p style="font-size:1.2rem;font-weight:600;color:#cbd5e1;">Upload Similarweb data & hit Generate</p>
        <p style="font-size:0.9rem;">IT, FR, IB (ES) supported · Upload XLSX or CSV · Country-specific tiering</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")
# ── LOG VIEWER ─────────────────────────────────────────────
hs_key = os.getenv("HUBSPOT_API_KEY", "")
hs_status = "🟢 Connected" if hs_key and len(hs_key) > 10 else "🔴 Not configured (.env missing)"
st.caption(f"Scalapay Territory Engine v4.0 — Strategy & RevOps · IT/FR/IB &nbsp;|&nbsp; HubSpot: {hs_status}")

if "log_messages" in st.session_state and st.session_state.log_messages:
    with st.expander(f"📋 Pipeline Logs ({len(st.session_state.log_messages)} entries)", expanded=False):
        c1, c2 = st.columns([4, 1])
        with c2:
            if st.button("🗑️ Clear logs", key="clear_logs"):
                st.session_state.log_messages = []
                st.rerun()
        log_text = "\n".join(st.session_state.log_messages[-200:])  # Last 200 entries
        st.code(log_text, language=None)
