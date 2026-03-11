"""
scoring.py v4 — Real SW transactions, category AOV, country penetration.
TTV = Monthly_Transactions(SW) × AOV(category) × Penetration(cat, country) × 12
Fallback: traffic × CR(category) when transactions missing.
"""
import numpy as np
import pandas as pd
from config import (
    BNPL_PENETRATION_PCT, DEFAULT_PENETRATION_PCT, CATEGORY_AOV, DEFAULT_AOV_EUR,
    TTV_SEGMENT_THRESHOLDS, ACCOUNT_SIZE_SCORE,
    get_tier, get_scalapay_category, get_penetration, TIER_SCORE,
    WARMTH_SCORES, WHITESPACE_CATEGORIES,
)
from utils import get_logger, safe_float
log = get_logger(__name__)

# Fallback CRs from Similarweb data (median by category)
CATEGORY_CR = {
    "Apparel & Fashion": 0.60, "Auto & Moto": 0.57, "Auto Repair Shops": 0.57,
    "B2B": 0.58, "B2B Goods & Trade Materials": 0.58, "Baby & Toddler": 0.66,
    "Cosmetics & Beauty": 1.20, "Dental": 0.59, "Education": 0.96,
    "Electronics": 0.51, "Electronics & Household appliance": 0.51,
    "Entertainment & Sports": 0.55, "Food & Beverage": 0.89,
    "General Retail": 1.06, "Generalist Marketplace": 0.74,
    "Glasses & Eyewear": 0.56, "Hobbies & Games": 0.65,
    "Home & Garden": 0.57, "Household Appliance": 0.58,
    "Jewelry & Watches": 0.56, "Learning & Classes": 0.96,
    "Luxury Goods": 0.56, "Medical": 1.26, "Other": 0.87,
    "Petcare": 1.07, "Pharma": 1.17, "Professional Services": 0.87,
    "Shoes & Accessories": 0.62, "Sport": 0.60, "Veterinarians": 1.07,
    "Wellness": 0.94,
    "Travel - OTA": 0.52, "Travel - Hotel & Accommodation": 1.10,
    "Travel - Tour Operator": 1.16, "Travel - Local & Urban Transport": 0.83,
    "Travel - Adventure & Group Travel": 0.62, "Travel - Theme Parks": 0.58,
    "Travel - Cruise": 0.58, "Travel - Car Rental": 0.58,
    "Travel - Entertainment & Experiences": 0.58, "Travel - Ticketing & Events": 0.58,
    "Travel - Ferry & Maritime": 0.83, "Travel - Wellness & Spa": 0.94,
    "Travel - Ski & Mountain": 0.58, "Travel - Other": 0.62,
}
DEFAULT_CR = 0.62


def assign_account_segment(ttv_annual):
    ttv = safe_float(ttv_annual)
    for threshold, segment in TTV_SEGMENT_THRESHOLDS:
        if ttv > threshold:
            return segment
    return "Executive"


def estimate_mr_ttv(row):
    """
    Primary: SW Monthly Transactions × AOV × Penetration × 12
    Fallback: Traffic × CR(cat) × AOV × Penetration × 12
    """
    cat = row.get("scalapay_category", "Other")
    country = row.get("country", "ES")
    aov = CATEGORY_AOV.get(cat, DEFAULT_AOV_EUR)
    pen_pct = get_penetration(cat, country) / 100.0

    sw_txns = safe_float(row.get("monthly_transactions_est", 0))
    traffic = safe_float(row.get("monthly_traffic", 0))

    if sw_txns > 0:
        monthly_txns = sw_txns
        source = "SW"
    else:
        cr = CATEGORY_CR.get(cat, DEFAULT_CR) / 100.0
        monthly_txns = traffic * cr
        source = "CR"

    mr_m = monthly_txns * aov
    mr_a = mr_m * 12
    ttv_m = mr_m * pen_pct
    ttv_a = ttv_m * 12
    return pd.Series({
        "est_monthly_txns": round(monthly_txns),
        "est_mr_monthly_eur": round(mr_m),
        "est_mr_annual_eur": round(mr_a),
        "est_ttv_monthly_eur": round(ttv_m),
        "est_ttv_annual_eur": round(ttv_a),
        "aov_used": aov,
        "bnpl_pen_used": round(pen_pct * 100, 1),
        "ttv_source": source,
    })


def penetration_score(industry, mr_annual, country="ES"):
    if not industry or not isinstance(industry, str):
        industry = ""
    sc = get_scalapay_category(industry)
    pen_pct = get_penetration(sc, country)
    is_actionable = pen_pct > 5.0 or mr_annual > 5_000_000
    pen_norm = min(pen_pct / 15.0, 1.0)
    mr_norm = min(mr_annual / 50_000_000, 1.0)
    score = (pen_norm * 0.6 + mr_norm * 0.4) * 15
    return {
        "bnpl_penetration_pct": pen_pct,
        "is_actionable": is_actionable,
        "penetration_score": round(score, 1),
    }


def growth_score(yoy, mom):
    yoy_val = safe_float(yoy)
    mom_val = safe_float(mom)
    yoy_norm = np.clip(yoy_val / 50.0, -0.5, 1.0)
    mom_norm = np.clip(mom_val / 10.0, -0.5, 1.0)
    raw = (yoy_norm * 0.6 + mom_norm * 0.4) * 15
    return round(max(raw, 0), 1)


def is_whitespace(industry, competitors_bnpl=""):
    sc = get_scalapay_category(industry) if isinstance(industry, str) else "Other"
    is_ws_cat = sc in WHITESPACE_CATEGORIES
    has_no_comp = not bool(competitors_bnpl.strip()) if isinstance(competitors_bnpl, str) else True
    is_ws = is_ws_cat and has_no_comp
    score = 10.0 if is_ws else (5.0 if is_ws_cat else 0.0)
    return {"is_whitespace": is_ws, "whitespace_category": is_ws_cat, "whitespace_score": score}


def competitor_score(competitors_bnpl):
    if not isinstance(competitors_bnpl, str) or not competitors_bnpl.strip():
        return 10.0
    n = len([c.strip() for c in competitors_bnpl.split(",") if c.strip()])
    if n == 0: return 10.0
    elif n == 1: return 7.0
    elif n == 2: return 4.0
    else: return 2.0


def classify_warmth_from_sw(in_hubspot, is_new=""):
    """Approachability from Similarweb flag. Full classification in hubspot_client.py."""
    if str(in_hubspot).strip().lower() == "yes":
        return "In HubSpot (unknown)"
    return "Net New"


# Direct competitors = Klarna, Alma, Sequra, Oney, Clearpay, Afterpay
_DIRECT_COMPETITORS = {"klarna", "alma", "sequra", "oney", "clearpay", "afterpay"}


def market_opportunity_score(industry, competitors_bnpl="", has_paypal=False):
    """
    Market Opportunity - 4 levels based on DEDICATED BNPL competition.
    PayPal is tracked separately (not a real BNPL competitor).

    TOP (15 pts):         No dedicated BNPL at checkout
    MEDIUM-HIGH (10 pts): 1 non-direct BNPL (Cofidis, Pledg, Heylight, etc.)
    MEDIUM (5 pts):       1 direct competitor (Klarna/Alma/Sequra/Oney) or 2+ any
    LOW (2 pts):          3+ players — saturated
    """
    comps = []
    if isinstance(competitors_bnpl, str) and competitors_bnpl.strip():
        comps = [c.strip().lower() for c in competitors_bnpl.split(",") if c.strip()]
    n_comps = len(comps)
    has_direct = any(c in _DIRECT_COMPETITORS for c in comps)

    if n_comps == 0:
        level, pts = "TOP", 15.0
    elif n_comps == 1 and not has_direct:
        level, pts = "MEDIUM-HIGH", 10.0
    elif n_comps <= 2:
        level, pts = "MEDIUM", 5.0
    else:
        level, pts = "LOW", 2.0

    return {
        "n_competitors": n_comps,
        "competitors_list": ", ".join(c.title() for c in comps) if comps else "None",
        "has_direct_competitor": has_direct,
        "has_paypal": has_paypal,
        "opportunity_level": level,
        "market_opportunity_score": pts,
    }


def compute_final_score(row, weights=None):
    """5 components. Account size excluded (territory routing only)."""
    if not weights:
        weights = {"tier": 25, "penetration": 25, "growth": 15,
                   "warmth": 20, "market_opportunity": 15}
    total_max = sum(weights.values())
    if total_max == 0: return 0
    tier = min(safe_float(row.get("tier_score", 0)), weights.get("tier", 30))
    pen = min(safe_float(row.get("penetration_score", 0)), weights.get("penetration", 20))
    grw = min(safe_float(row.get("growth_score", 0)), weights.get("growth", 20))
    warmth = min(safe_float(row.get("warmth_score", 0)), weights.get("warmth", 15))
    mkt = min(safe_float(row.get("market_opportunity_score", 0)), weights.get("market_opportunity", 15))
    raw = tier + pen + grw + warmth + mkt
    return round(np.clip((raw / total_max) * 100, 0, 100), 1)


def score_dataframe(df, weights=None):
    """Full scoring pipeline."""
    df["scalapay_category"] = df["industry"].apply(
        lambda x: get_scalapay_category(x) if isinstance(x, str) else "Other"
    )
    df["tier"] = df.apply(
        lambda r: get_tier(r.get("industry", ""), r.get("country", "ES")), axis=1
    )
    df["tier_score"] = df["tier"].map(TIER_SCORE).fillna(8)

    # MR & TTV (real transactions from SW)
    mr_ttv = df.apply(estimate_mr_ttv, axis=1)
    df = pd.concat([df, mr_ttv], axis=1)

    # Account segment by TTV — for territory assignment, NOT scoring
    df["account_segment"] = df["est_ttv_annual_eur"].apply(assign_account_segment)

    # Penetration
    pen = df.apply(
        lambda r: penetration_score(r.get("industry", ""), safe_float(r.get("est_mr_annual_eur", 0)), r.get("country", "ES")),
        axis=1,
    )
    df = pd.concat([df, pd.DataFrame(pen.tolist(), index=df.index)], axis=1)

    # Growth
    df["growth_score"] = df.apply(
        lambda r: growth_score(r.get("yoy_growth", 0), r.get("mom_growth", 0)), axis=1
    )

    # Approachability
    if "lead_warmth" not in df.columns:
        df["lead_warmth"] = df.apply(
            lambda r: classify_warmth_from_sw(r.get("in_hubspot_sw", ""), r.get("is_new", "")),
            axis=1,
        )
    df["warmth_score"] = df["lead_warmth"].map(WARMTH_SCORES).fillna(5)

    # Market Opportunity
    if "competitors_bnpl" not in df.columns:
        df["competitors_bnpl"] = ""
    mkt = df.apply(
        lambda r: market_opportunity_score(r.get("industry", ""), str(r.get("competitors_bnpl", "")), bool(r.get("has_paypal", False))),
        axis=1,
    )
    mkt_df = pd.DataFrame(mkt.tolist(), index=df.index)
    for col in mkt_df.columns:
        if col in df.columns:
            df = df.drop(columns=[col])
    df = pd.concat([df, mkt_df], axis=1)

    df = df[df["tier"] != "EXCLUDE"].reset_index(drop=True)
    df["Sales_Priority_Score"] = df.apply(lambda r: compute_final_score(r, weights), axis=1)
    df = df.sort_values("Sales_Priority_Score", ascending=False).reset_index(drop=True)
    return df
