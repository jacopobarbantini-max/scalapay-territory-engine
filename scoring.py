"""
scoring.py — Business logic & scoring engine.
Adapted for real Similarweb export columns.
"""

import numpy as np
import pandas as pd

from config import (
    AVG_ORDER_VALUE_EUR,
    BNPL_PENETRATION_PCT,
    BNPL_PENETRATION_BY_COUNTRY,
    DEFAULT_PENETRATION_PCT,
    REVENUE_TIERS,
    ACCOUNT_SIZE_SCORE,
    PAYPAL_BNPL_TAGS,
    SCALAPAY_SHARE_OF_CHECKOUT,
    TIER_MAP,
    TIER_BY_COUNTRY,
    SW_TO_SCALAPAY_CATEGORY,
    TIER_SCORE,
    TRAFFIC_TO_TRANSACTION_RATE,
    WARMTH_SCORES,
    WHITESPACE_CATEGORIES,
)
from utils import get_logger, safe_float

log = get_logger(__name__)


# -- MAP SIMILARWEB INDUSTRY -> SCALAPAY CATEGORY
def map_scalapay_category(industry):
    if not industry or not isinstance(industry, str):
        return "Other"
    industry = industry.strip()
    cat = SW_TO_SCALAPAY_CATEGORY.get(industry, "")
    if cat:
        return cat
    # Try macro match
    macro = industry.split("/")[0].strip()
    for sw_ind, c in SW_TO_SCALAPAY_CATEGORY.items():
        if sw_ind.startswith(macro + "/"):
            return c
    return "Other"


# -- TIER ASSIGNMENT (country-aware)
def assign_tier(industry, country=""):
    if not industry or not isinstance(industry, str):
        return "UNKNOWN"
    industry = industry.strip()

    # Step 1: Try country-specific tier via Scalapay category mapping
    scalapay_cat = SW_TO_SCALAPAY_CATEGORY.get(industry, "")
    if not scalapay_cat:
        # Try matching on macro category
        macro = industry.split("/")[0].strip()
        for sw_ind, cat in SW_TO_SCALAPAY_CATEGORY.items():
            if sw_ind.startswith(macro + "/"):
                scalapay_cat = cat
                break

    if scalapay_cat and country in TIER_BY_COUNTRY:
        tier = TIER_BY_COUNTRY[country].get(scalapay_cat)
        if tier:
            return tier

    # Step 2: Fallback to original TIER_MAP (Similarweb industry -> tier)
    if industry in TIER_MAP:
        return TIER_MAP[industry]
    macro = industry.split("/")[0].strip()
    for key, tier in TIER_MAP.items():
        if key.startswith(macro + "/"):
            return tier
    return "UNKNOWN"


# -- ACCOUNT SIZE SEGMENTATION
def assign_account_size(revenue_str):
    if not revenue_str or not isinstance(revenue_str, str) or revenue_str == "nan":
        return "Unknown"
    return REVENUE_TIERS.get(revenue_str.strip(), "Unknown")


# -- MR & TTV ESTIMATION
def estimate_mr_ttv(monthly_visits, industry=""):
    traffic = safe_float(monthly_visits)
    monthly_transactions = traffic * TRAFFIC_TO_TRANSACTION_RATE
    mr_monthly = monthly_transactions * AVG_ORDER_VALUE_EUR
    mr_annual = mr_monthly * 12
    expected_ttv_monthly = mr_monthly * SCALAPAY_SHARE_OF_CHECKOUT
    expected_ttv_annual = expected_ttv_monthly * 12
    return {
        "est_monthly_transactions": round(monthly_transactions),
        "est_mr_monthly_eur": round(mr_monthly),
        "est_mr_annual_eur": round(mr_annual),
        "est_ttv_monthly_eur": round(expected_ttv_monthly),
        "est_ttv_annual_eur": round(expected_ttv_annual),
    }


# -- PENETRATION VALUATION (country-aware)
def penetration_score(industry, mr_annual, country=""):
    if not industry or not isinstance(industry, str):
        industry = ""

    # Get Scalapay category for this industry
    scalapay_cat = SW_TO_SCALAPAY_CATEGORY.get(industry.strip(), "")
    if not scalapay_cat:
        macro = industry.split("/")[0].strip()
        for sw_ind, cat in SW_TO_SCALAPAY_CATEGORY.items():
            if sw_ind.startswith(macro + "/"):
                scalapay_cat = cat
                break

    # Look up penetration: country-specific first, then global fallback
    pen_pct = DEFAULT_PENETRATION_PCT
    if scalapay_cat:
        if country in BNPL_PENETRATION_BY_COUNTRY:
            pen_pct = BNPL_PENETRATION_BY_COUNTRY[country].get(scalapay_cat, DEFAULT_PENETRATION_PCT)
        elif scalapay_cat in BNPL_PENETRATION_PCT:
            pen_pct = BNPL_PENETRATION_PCT[scalapay_cat]

    is_massive_mr = mr_annual > 5_000_000
    is_actionable = pen_pct > 5.0 or is_massive_mr
    pen_norm = min(pen_pct / 20.0, 1.0)
    mr_norm = min(mr_annual / 50_000_000, 1.0)
    score = (pen_norm * 0.6 + mr_norm * 0.4) * 15
    return {
        "bnpl_penetration_pct": pen_pct,
        "is_actionable": is_actionable,
        "penetration_score": round(score, 1),
    }


# -- GROWTH SCORING
def growth_score(yoy, mom):
    yoy_val = safe_float(yoy)
    mom_val = safe_float(mom)
    # SW exports as decimals (0.15 = 15%)
    yoy_pct = yoy_val * 100 if abs(yoy_val) < 5 else yoy_val
    mom_pct = mom_val * 100 if abs(mom_val) < 5 else mom_val
    yoy_norm = np.clip(yoy_pct / 50.0, -0.5, 1.0)
    mom_norm = np.clip(mom_pct / 10.0, -0.5, 1.0)
    raw = (yoy_norm * 0.6 + mom_norm * 0.4) * 15
    return round(max(raw, 0), 1)


# -- WHITESPACE FLAG (purely competitor-based)
def is_whitespace(competitors_bnpl=""):
    """
    Whitespace = no real BNPL competitor on site.
    PayPal BNPL alone does NOT disqualify — it's a generic option, not a dedicated BNPL.
    """
    if not isinstance(competitors_bnpl, str) or not competitors_bnpl.strip():
        # No scraping done or no competitors found
        return {
            "is_whitespace": True,
            "competitors_count": 0,
            "has_only_paypal_bnpl": False,
            "whitespace_score": 10.0,
        }

    comps = [c.strip() for c in competitors_bnpl.split(",") if c.strip()]
    real_comps = [c for c in comps if c not in PAYPAL_BNPL_TAGS]
    has_only_paypal = len(comps) > 0 and len(real_comps) == 0

    if len(real_comps) == 0:
        # No real BNPL = whitespace (even if PayPal BNPL present)
        return {
            "is_whitespace": True,
            "competitors_count": len(comps),
            "has_only_paypal_bnpl": has_only_paypal,
            "whitespace_score": 10.0 if not has_only_paypal else 8.0,
        }
    else:
        return {
            "is_whitespace": False,
            "competitors_count": len(comps),
            "has_only_paypal_bnpl": False,
            "whitespace_score": 0.0,
        }


# -- COMPETITOR COUNT (informational — feeds whitespace, no separate score)
def competitor_count(competitors_bnpl):
    """Returns count of BNPL competitors found. For sales visibility."""
    if not isinstance(competitors_bnpl, str) or not competitors_bnpl.strip():
        return 0
    comps = [c.strip() for c in competitors_bnpl.split(",") if c.strip()]
    return len(comps)


# -- LEAD WARMTH FROM SW "In HubSpot" COLUMN
def classify_warmth_from_sw(in_hubspot, is_new=""):
    hs = str(in_hubspot).strip().lower()
    if hs == "yes":
        return "In CRM, No Deal"
    return "Net New"


# -- FINAL COMPOSITE SCORE
def compute_final_score(row):
    tier = safe_float(row.get("tier_score", 0))
    acct = safe_float(row.get("account_size_score", 0))
    pen = safe_float(row.get("penetration_score", 0))
    grw = safe_float(row.get("growth_score", 0))
    warmth = safe_float(row.get("warmth_score", 0))
    ws = safe_float(row.get("whitespace_score", 0))

    # 5 components (competitor intel removed as standalone — feeds whitespace)
    raw = (
        min(tier, 25) + min(acct, 20) + min(pen, 15)
        + min(grw, 15) + min(warmth, 10) + min(ws, 10)
    )
    score = (raw / 95) * 100
    return round(np.clip(score, 0, 100), 1)


# -- MAIN PIPELINE
def score_dataframe(df):
    """Run full scoring on Similarweb-format DataFrame."""

    # TIER (country-aware)
    df["scalapay_category"] = df["industry"].apply(map_scalapay_category)
    df["tier"] = df.apply(
        lambda r: assign_tier(r.get("industry", ""), r.get("country", "")),
        axis=1,
    )
    df["tier_score"] = df["tier"].map(TIER_SCORE).fillna(8)

    # ACCOUNT SIZE
    df["segment"] = df["annual_revenue_bucket"].astype(str).apply(assign_account_size)
    df["account_size_score"] = df["segment"].map(ACCOUNT_SIZE_SCORE).fillna(4)

    # MR & TTV
    mr_ttv = df.apply(
        lambda r: estimate_mr_ttv(r.get("monthly_traffic", 0), r.get("industry", "")),
        axis=1,
    )
    mr_df = pd.DataFrame(mr_ttv.tolist(), index=df.index)
    df = pd.concat([df, mr_df], axis=1)

    # PENETRATION (country-aware)
    pen = df.apply(
        lambda r: penetration_score(
            r.get("industry", ""),
            safe_float(r.get("est_mr_annual_eur", 0)),
            r.get("country", ""),
        ),
        axis=1,
    )
    pen_df = pd.DataFrame(pen.tolist(), index=df.index)
    df = pd.concat([df, pen_df], axis=1)

    # GROWTH
    df["growth_score"] = df.apply(
        lambda r: growth_score(r.get("YoY traffic change", 0), r.get("MoM traffic change", 0)),
        axis=1,
    )

    # WARMTH
    if "lead_warmth" not in df.columns:
        df["lead_warmth"] = df.apply(
            lambda r: classify_warmth_from_sw(r.get("in_hubspot_sw", ""), r.get("is_new", "")),
            axis=1,
        )
    df["warmth_score"] = df["lead_warmth"].map(WARMTH_SCORES).fillna(5)

    # COMPETITOR (informational — count + list for sales visibility)
    if "competitors_bnpl" not in df.columns:
        df["competitors_bnpl"] = ""
    df["competitors_count"] = df["competitors_bnpl"].apply(competitor_count)

    # WHITESPACE (based on real BNPL competitor presence, not industry categories)
    ws = df.apply(
        lambda r: is_whitespace(str(r.get("competitors_bnpl", ""))),
        axis=1,
    )
    ws_df = pd.DataFrame(ws.tolist(), index=df.index)
    for col in ws_df.columns:
        if col in df.columns:
            df = df.drop(columns=[col])
    df = pd.concat([df, ws_df], axis=1)

    # EXCLUDE gambling, adult, etc.
    df = df[df["tier"] != "EXCLUDE"].reset_index(drop=True)

    # FINAL SCORE
    df["Sales_Priority_Score"] = df.apply(compute_final_score, axis=1)
    df = df.sort_values("Sales_Priority_Score", ascending=False).reset_index(drop=True)

    return df
