"""
app.py — Scalapay Territory List Generator

Streamlit UI that orchestrates the full pipeline:
  Phase 1: Ingest (Similarweb CSV / API) + HubSpot cross-check
  Phase 2: Enrichment (competitor scraping, ad pixels)
  Phase 3: Business logic & metrics
  Phase 4: Tier assignment
  Phase 5: Final scoring & Excel export
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

# ── CUSTOM CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    .stApp {
        background-color: #0a0a0f;
    }
    section[data-testid="stSidebar"] {
        background-color: #111118;
    }
    .main-header {
        background: linear-gradient(135deg, #6C3AED 0%, #2563EB 50%, #0EA5E9 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.4rem;
        font-weight: 800;
        margin-bottom: 0;
    }
    .sub-header {
        color: #94a3b8;
        font-size: 1.05rem;
        margin-top: -8px;
        margin-bottom: 24px;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e1b4b 0%, #172554 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #e2e8f0;
    }
    .metric-label {
        color: #94a3b8;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .tier-gold { color: #fbbf24; font-weight: 700; }
    .tier-silver { color: #94a3b8; font-weight: 700; }
    .tier-bronze { color: #d97706; font-weight: 700; }
    div[data-testid="stDataFrame"] {
        border: 1px solid #1e293b;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ──────────────────────────────────────────────────
st.markdown('<p class="main-header">🎯 Scalapay Territory Engine</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Automated lead scoring & territory list generation for ES (Iberia) / FR Sales teams</p>', unsafe_allow_html=True)

# ── SIDEBAR — CONFIGURATION ────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Pipeline Configuration")

    st.markdown("---")
    st.markdown("**📊 Data Source**")
    data_mode = st.radio(
        "Similarweb input mode",
        ["📁 CSV Upload", "🌐 API (requires key)"],
        index=0,
        help="Upload a Similarweb export CSV, or use the API if you have a key.",
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
        st.markdown("---")
        st.markdown("**📂 Upload Similarweb Exports**")
        for c in countries:
            upload_label = "Iberia (ES+PT)" if c == "IB" else c
            f = st.file_uploader(
                f"Similarweb CSV/XLSX — {upload_label}",
                type=["csv", "xlsx", "xls"],
                key=f"sw_upload_{c}",
            )
            if f:
                uploaded_files[c] = f

        if not uploaded_files:
            use_sample = st.checkbox("🧪 Use sample data for demo", value=True)
        else:
            use_sample = False
    else:
        use_sample = False

    st.markdown("---")
    st.markdown("**🔌 Integrations**")
    enable_hubspot = st.checkbox(
        "HubSpot CRM cross-check",
        value=bool(os.getenv("HUBSPOT_API_KEY")),
        help="Requires HUBSPOT_API_KEY in .env",
    )
    enable_scraping = st.checkbox(
        "Checkout / competitor scraping",
        value=False,
        help="Scrapes merchant homepages — slower but richer data.",
    )
    enable_ads_check = st.checkbox(
        "Ad pixel detection",
        value=False,
        help="Detects Meta Pixel & Google Ads tags (requires scraping).",
    )

    st.markdown("---")
    st.markdown("**🎚️ Scoring Weights**")
    w_tier = st.slider("Tier weight", 0.0, 0.5, SCORING_WEIGHTS["tier"], 0.05,
        help="Industry fit for BNPL. How well does pay-later convert in this vertical? Gold (Apparel, Beauty, Electronics) scores highest, Bronze (Travel, Food) lowest.")
    w_pen = st.slider("Penetration/TTV weight", 0.0, 0.5, SCORING_WEIGHTS["penetration_ttv"], 0.05,
        help="Revenue opportunity. Combines BNPL penetration rate in the vertical with estimated Total Transaction Value (traffic x conversion x AOV x Scalapay share).")
    w_growth = st.slider("Growth weight", 0.0, 0.5, SCORING_WEIGHTS["traffic_growth"], 0.05,
        help="Merchant momentum. YoY + MoM traffic growth from Similarweb. Proxy for ad spend: merchants investing in acquisition have budget and need checkout optimization.")
    w_warmth = st.slider("Lead warmth weight", 0.0, 0.5, SCORING_WEIGHTS["lead_warmth"], 0.05,
        help="CRM status from HubSpot. Active Pipeline = 10, In CRM No Deal = 7, Net New = 5, Lost 6m+ ago = 4, Recently Lost = 3, Existing Client = 2. Without HubSpot, all leads = Net New.")
    w_ws = st.slider("Whitespace weight", 0.0, 0.5, SCORING_WEIGHTS["whitespace"], 0.05,
        help="Greenfield flag. True if no real BNPL competitor detected on merchant site. PayPal BNPL alone still counts as whitespace. Requires scraping enabled.")

    st.markdown("---")
    st.markdown("**📋 Filters**")
    min_traffic = st.number_input("Min monthly traffic", value=0, step=50_000)
    exclude_won = st.checkbox("Exclude Existing Clients", value=True)


# ── MAIN PIPELINE ───────────────────────────────────────────
def load_sample_data(country: str) -> pd.DataFrame:
    """Load built-in sample data."""
    # IB uses the ES sample (which already contains SP+PT merged data)
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
    """Execute the full territory list generation pipeline."""

    # ── PHASE 1: DATA INGESTION ─────────────────────────────
    st.markdown("### 📥 Phase 1 — Data Ingestion & CRM Cross-Check")
    progress = st.progress(0, text="Loading data...")

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
            st.success(f"✅ {country}: {len(df)} leads loaded")
        else:
            st.warning(f"⚠️ {country}: No data — upload a CSV or configure API key")

        progress.progress((i + 1) / len(countries), text=f"Loaded {country}")

    if not all_dfs:
        st.error("No data to process. Upload CSVs or enable sample data.")
        return None

    df = pd.concat(all_dfs, ignore_index=True)
    st.info(f"📊 **{len(df)} total leads** across {', '.join(countries)}")

    # Filter by minimum traffic
    traffic_col = "monthly_traffic" if "monthly_traffic" in df.columns else None
    if traffic_col:
        before = len(df)
        df = df[df[traffic_col] >= min_traffic].reset_index(drop=True)
        filtered = before - len(df)
        if filtered > 0:
            st.caption(f"Filtered out {filtered} leads below {min_traffic:,} monthly traffic")

    # ── HubSpot Cross-Check
    if enable_hubspot:
        with st.spinner("🔄 Checking HubSpot CRM..."):
            df = enrich_with_hubspot(df)
            hs_found = df["hs_exists"].sum() if "hs_exists" in df.columns else 0
            st.success(f"✅ HubSpot: {hs_found} existing records found")
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

    # ── PHASE 2: ENRICHMENT ─────────────────────────────────
    st.markdown("### 🔍 Phase 2 — Competitor & Ad Enrichment")

    if enable_scraping:
        enrich_progress = st.progress(0, text="Scraping merchant sites...")

        def update_enrich(current, total):
            enrich_progress.progress(
                current / total,
                text=f"Enriching {current}/{total} domains..."
            )

        df = enrich_dataframe(df, enable_scraping=True, progress_callback=update_enrich)
        enrich_progress.progress(1.0, text="Enrichment complete")
        comps_found = (df["competitors_bnpl"] != "").sum()
        st.success(f"✅ Competitor data found for {comps_found} merchants")
    else:
        df = enrich_dataframe(df, enable_scraping=False)
        st.info("ℹ️ Scraping disabled — competitor columns left empty (enable in sidebar)")

    # ── PHASE 3 + 4 + 5: SCORING ───────────────────────────
    st.markdown("### 🧮 Phase 3–5 — Scoring & Tier Assignment")
    with st.spinner("Computing scores..."):
        df = score_dataframe(df)

    # Exclude won deals if requested
    if exclude_won and "lead_warmth" in df.columns:
        df = df[df["lead_warmth"] != "Existing Client"].reset_index(drop=True)

    st.success(f"✅ **{len(df)} scored leads** ready for review")
    return df


# ── GENERATE BUTTON ─────────────────────────────────────────
col_btn1, col_btn2, _ = st.columns([1, 1, 3])
with col_btn1:
    generate = st.button("🚀 Generate Territory List", type="primary", use_container_width=True)
with col_btn2:
    if "result_df" in st.session_state and st.session_state.result_df is not None:
        clear = st.button("🗑️ Clear Results", use_container_width=True)
        if clear:
            st.session_state.result_df = None
            st.rerun()

if generate:
    result = run_pipeline()
    if result is not None:
        st.session_state.result_df = result

        # ── AUTO-SAVE to exports/ folder ────────────────────────
        try:
            import os, pathlib
            exports_dir = pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "exports"
            exports_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            # Save Excel
            excel_path = exports_dir / f"territory_{ts}.xlsx"
            result.to_excel(str(excel_path), index=False, engine="xlsxwriter")
            # Save CSV backup
            csv_path = exports_dir / f"territory_{ts}.csv"
            result.to_csv(str(csv_path), index=False)
            st.success(f"Auto-saved to `exports/territory_{ts}.xlsx` and `.csv`")
        except Exception as e:
            st.warning(f"Auto-save failed: {e}")

# ── RESULTS DISPLAY ─────────────────────────────────────────
if "result_df" in st.session_state and st.session_state.result_df is not None:
    df = st.session_state.result_df

    st.markdown("---")
    st.markdown("---")
    st.markdown("### 📊 Results Dashboard")

    def render_kpi_cards(data):
        """Render KPI summary cards for any data subset."""
        k1, k2, k3, k4, k5 = st.columns(5)
        n_leads = len(data)
        gold_n = (data["tier"] == "GOLD").sum() if "tier" in data.columns else 0
        avg_s = data["Sales_Priority_Score"].mean() if "Sales_Priority_Score" in data.columns and n_leads > 0 else 0
        ttv = data["est_ttv_annual_eur"].sum() if "est_ttv_annual_eur" in data.columns else 0
        ttv_str = f"€{ttv/1_000_000:.1f}M" if ttv >= 1_000_000 else f"€{ttv:,.0f}"
        ws_n = data["is_whitespace"].sum() if "is_whitespace" in data.columns else 0
        with k1:
            st.markdown(f'<div class="metric-card"><div class="metric-value">{n_leads}</div><div class="metric-label">Total Leads</div></div>', unsafe_allow_html=True)
        with k2:
            st.markdown(f'<div class="metric-card"><div class="metric-value tier-gold">{gold_n}</div><div class="metric-label">Gold Tier</div></div>', unsafe_allow_html=True)
        with k3:
            st.markdown(f'<div class="metric-card"><div class="metric-value">{avg_s:.1f}</div><div class="metric-label">Avg Score</div></div>', unsafe_allow_html=True)
        with k4:
            st.markdown(f'<div class="metric-card"><div class="metric-value">{ttv_str}</div><div class="metric-label">Total Est. TTV</div></div>', unsafe_allow_html=True)
        with k5:
            st.markdown(f'<div class="metric-card"><div class="metric-value">{ws_n}</div><div class="metric-label">Whitespace Opps</div></div>', unsafe_allow_html=True)

    # ── Tabs
    tab_full, tab_gold, tab_whitespace, tab_country = st.tabs([
        "📋 Full List", "🥇 Gold Tier", "🔵 Whitespace", "🌍 By Country"
    ])

    # Define display columns
    display_cols = [
        "domain", "country", "tier", "scalapay_category", "segment", "Sales_Priority_Score",
        "lead_warmth", "est_ttv_annual_eur", "est_mr_annual_eur",
    ]
    # Add optional columns if they exist
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
                st.info("No whitespace opportunities found with current data.")
            else:
                render_kpi_cards(ws_df)
                st.markdown("")
                st.dataframe(ws_df[display_cols], use_container_width=True, height=400)
        else:
            st.info("Whitespace column not available.")

    with tab_country:
        for country in countries:
            if "country" in df.columns:
                cdf = df[df["country"] == country]
                st.markdown(f"#### \U0001f1ea\U0001f1f8\U0001f1f5\U0001f1f9 Iberia (ES + PT)" if country == "IB" else f"#### \U0001f1eb\U0001f1f7 France")
                render_kpi_cards(cdf)
                st.markdown("")
                st.dataframe(cdf[display_cols], use_container_width=True, height=400)

    # ── EXCEL EXPORT ────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Export")

    def generate_excel(dataframe: pd.DataFrame) -> bytes:
        """Generate a formatted Excel file optimized for Google Sheets / sales."""
        output = io.BytesIO()

        # Reorder columns for sales readability
        priority_cols = [
            "domain", "country", "tier", "segment", "Sales_Priority_Score",
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
                "bold": True, "bg_color": "#1e1b4b", "font_color": "#e2e8f0",
                "border": 1, "text_wrap": True, "valign": "vcenter",
                "font_name": "Arial", "font_size": 10,
            })
            money_fmt = wb.add_format({"num_format": '#,##0 \u20ac', "font_name": "Arial", "font_size": 10})
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
            label="⬇️ Download Full Excel Report",
            data=excel_bytes,
            file_name=f"scalapay_territory_list_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )
    with col_dl2:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download CSV",
            data=csv_bytes,
            file_name=f"scalapay_territory_list_{timestamp}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # ── Score Breakdown (expandable)
    with st.expander("🔬 Score Breakdown & Methodology"):
        st.markdown("""
        **Sales_Priority_Score** is a composite 0–100 score built from:

        | Component | Max pts | What it measures |
        |-----------|---------|------------------|
        | **Tier** | 25 | Gold/Silver/Bronze category risk mapping |
        | **Penetration / TTV** | 20 | BNPL adoption potential × estimated revenue |
        | **Traffic Growth** | 15 | YoY (60%) + MoM (40%) e-commerce momentum |
        | **Lead Warmth** | 15 | CRM status: Warm > Net New > Cold/Lost |
        | **Competitor Intel** | 15 | Fewer active BNPL = bigger opportunity |
        | **Whitespace** | 10 | Under-penetrated vertical with no competitors |

        Weights are adjustable in the sidebar. Tier mapping follows the
        Scalapay Risk/Profitability framework (Gold → highest priority).
        """)

else:
    # Landing state
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; padding: 60px 20px; color: #64748b;">
        <p style="font-size: 3rem; margin-bottom: 8px;">🎯</p>
        <p style="font-size: 1.2rem; font-weight: 600; color: #cbd5e1;">
            Upload Similarweb data & hit Generate
        </p>
        <p style="font-size: 0.9rem;">
            Configure integrations in the sidebar, or use sample data for a quick demo.
        </p>
    </div>
    """, unsafe_allow_html=True)

# ── Footer
st.markdown("---")
st.caption("Scalapay Territory Engine v1.0 — RevOps & Strategy · Built for IB/FR Sales Expansion")
