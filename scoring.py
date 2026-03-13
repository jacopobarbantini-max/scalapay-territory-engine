"""
scoring.py v5 — TTV as TAM/SAM with competition adjustment.

TTV formula (bounce NOT applied — already embedded in CR):
  1. Transactions = SW real txns (preferred) OR Traffic × CR(category)
  2. Merchant Revenue (MR) = Txns × AOV(category)
  3. TAM = MR × BNPL Penetration(cat × country) × 12
  4. TAM_adj = TAM × AOV_Viability(aov)
  5. SAM = TAM_adj × Competition_Factor(competitors, country)
"""
import numpy as np
import pandas as pd
from config import (
    BNPL_PENETRATION_PCT, DEFAULT_PENETRATION_PCT, CATEGORY_AOV, DEFAULT_AOV_EUR,
    TTV_SEGMENT_THRESHOLDS, ACCOUNT_SIZE_SCORE,
    get_tier, get_scalapay_category, get_penetration, TIER_SCORE,
    WARMTH_SCORES, WHITESPACE_CATEGORIES,
    CATEGORY_CR, DEFAULT_CR, get_aov_viability, get_sam_factor,
)
from utils import get_logger, safe_float
log = get_logger(__name__)

_DIRECT_COMPETITORS = {"klarna", "alma", "sequra", "oney", "clearpay", "afterpay"}


def assign_account_segment(ttv_annual):
    ttv = safe_float(ttv_annual)
    for threshold, segment in TTV_SEGMENT_THRESHOLDS:
        if ttv > threshold: return segment
    return "Executive"


def estimate_mr_ttv(row):
    """
    TTV v5 final: 3% CR on avg_monthly_visits (L12M smoothed).
    
    Why 3%: Scalapay internal benchmark, calibrated on raw visits (bounce embedded),
    validated against HubSpot MR Estimated (exact match on FGM04 etc.).
    Why avg_monthly_visits: L12M average removes seasonality.
    SW Monthly Transactions are bucket midpoints (55K for 10K-100K range) — too coarse.
    """
    cat = row.get("scalapay_category", "Other")
    country = str(row.get("country", "ES")).upper()
    aov = CATEGORY_AOV.get(cat, DEFAULT_AOV_EUR)
    pen_pct = get_penetration(cat, country) / 100.0

    # Use avg_monthly_visits (L12M smoothed) × 3% CR
    avg_visits = safe_float(row.get("avg_monthly_visits", 0))
    traffic = safe_float(row.get("monthly_traffic", 0))

    # Prefer avg (L12M) for stability; fall back to monthly if avg missing
    base_traffic = avg_visits if avg_visits > 0 else traffic
    monthly_txns = base_traffic * 0.03  # Scalapay 3% benchmark

    # Merchant Revenue
    mr_m = monthly_txns * aov
    mr_a = mr_m * 12

    # TAM (100% Scalapay in empty market)
    tam_m = mr_m * pen_pct
    tam_a = tam_m * 12

    # AOV viability (HubSpot-backed)
    aov_viab = get_aov_viability(aov)
    tam_adj = tam_a * aov_viab

    # SAM (competition-adjusted, confidence-aware)
    comps = str(row.get("competitors_bnpl", ""))
    has_pp = bool(row.get("has_paypal", False))
    confidence = str(row.get("scraping_confidence", "NONE"))
    sam_factor = get_sam_factor(comps, country, has_pp)

    # If scraping failed (LOW confidence) and no competitors found,
    # the 1.00 factor is overly optimistic — use conservative category average
    if sam_factor >= 0.95 and confidence == "LOW":
        sam_factor = 0.75  # Conservative: assume some competition we couldn't see

    sam_a = tam_adj * sam_factor

    return pd.Series({
        "est_monthly_txns": round(monthly_txns),
        "est_mr_monthly_eur": round(mr_m),
        "est_mr_annual_eur": round(mr_a),
        "est_ttv_tam_annual": round(tam_a),
        "est_ttv_annual_eur": round(sam_a),
        "aov_used": aov,
        "aov_viability": round(aov_viab, 2),
        "sam_factor": round(sam_factor, 2),
        "bnpl_pen_used": round(pen_pct * 100, 1),
        "ttv_source": "L12M" if avg_visits > 0 else "Monthly",
    })


def penetration_score(industry, mr_annual, country="ES"):
    if not industry or not isinstance(industry, str): industry = ""
    sc = get_scalapay_category(industry)
    pen_pct = get_penetration(sc, country)
    is_actionable = pen_pct > 5.0 or mr_annual > 5_000_000
    pen_norm = min(pen_pct / 15.0, 1.0)
    mr_norm = min(mr_annual / 50_000_000, 1.0)
    score = (pen_norm * 0.6 + mr_norm * 0.4) * 15
    return {"bnpl_penetration_pct": pen_pct, "is_actionable": is_actionable, "penetration_score": round(score, 1)}


def growth_score(yoy, mom):
    yoy_val = safe_float(yoy); mom_val = safe_float(mom)
    yoy_norm = np.clip(yoy_val / 50.0, -0.5, 1.0)
    mom_norm = np.clip(mom_val / 10.0, -0.5, 1.0)
    raw = (yoy_norm * 0.6 + mom_norm * 0.4) * 15
    return round(max(raw, 0), 1)


def classify_warmth_from_sw(in_hubspot, is_new=""):
    if str(in_hubspot).strip().lower() == "yes": return "In HubSpot (unknown)"
    return "Net New"


def market_opportunity_score(industry, competitors_bnpl="", has_paypal=False):
    """Market Opportunity scoring — 4 levels. PayPal NOT counted as competitor."""
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
        weights = {"tier": 25, "penetration": 25, "growth": 15, "warmth": 20, "market_opportunity": 15}
    total_max = sum(weights.values())
    if total_max == 0: return 0
    tier = min(safe_float(row.get("tier_score", 0)), weights.get("tier", 30))
    pen = min(safe_float(row.get("penetration_score", 0)), weights.get("penetration", 20))
    grw = min(safe_float(row.get("growth_score", 0)), weights.get("growth", 20))
    warmth = min(safe_float(row.get("warmth_score", 0)), weights.get("warmth", 15))
    mkt = min(safe_float(row.get("market_opportunity_score", 0)), weights.get("market_opportunity", 15))
    raw = tier + pen + grw + warmth + mkt
    return round(np.clip((raw / total_max) * 100, 0, 100), 1)


# ═══════════════════════════════════════════════════════
# CROSS-COUNTRY ASSIGNMENT
# ═══════════════════════════════════════════════════════
_COUNTRY_MAP = {"france": "FR", "spain": "ES", "portugal": "PT", "italy": "IT",
                "fr": "FR", "es": "ES", "pt": "PT", "it": "IT"}

def assign_primary_country(row):
    """
    Assign merchant to the country where they generate most value.
    Uses top_country from SW, falls back to source_country.
    """
    source = str(row.get("country", "")).upper()
    top = str(row.get("top_country", "")).strip().lower()
    top_co = _COUNTRY_MAP.get(top, "")

    # If top_country maps to a valid territory AND differs from source
    if top_co and top_co != source and top_co in ("FR", "ES", "PT", "IT"):
        return top_co
    return source


def score_dataframe(df, weights=None):
    """Full scoring pipeline with v5 TTV."""
    # Category & tier
    df["scalapay_category"] = df["industry"].apply(
        lambda x: get_scalapay_category(x) if isinstance(x, str) else "Other")
    df["tier"] = df.apply(
        lambda r: get_tier(r.get("industry", ""), r.get("country", "ES")), axis=1)
    df["tier_score"] = df["tier"].map(TIER_SCORE).fillna(8)

    # Cross-country assignment
    if "top_country" in df.columns:
        df["primary_country"] = df.apply(assign_primary_country, axis=1)
        cross = df["primary_country"] != df["country"]
        if cross.sum() > 0:
            log.info(f"Cross-country: {cross.sum()} leads reassigned to primary market")
            df["cross_country_reassign"] = cross
            df["original_source_country"] = df["country"]
            df["country"] = df["primary_country"]
        df = df.drop(columns=["primary_country"], errors="ignore")

    # Market Opportunity (before TTV — needed for SAM factor)
    if "competitors_bnpl" not in df.columns:
        df["competitors_bnpl"] = ""
    mkt = df.apply(
        lambda r: market_opportunity_score(r.get("industry", ""), str(r.get("competitors_bnpl", "")), bool(r.get("has_paypal", False))),
        axis=1)
    mkt_df = pd.DataFrame(mkt.tolist(), index=df.index)
    for col in mkt_df.columns:
        if col in df.columns: df = df.drop(columns=[col])
    df = pd.concat([df, mkt_df], axis=1)

    # TTV v5 (uses competitors for SAM)
    mr_ttv = df.apply(estimate_mr_ttv, axis=1)
    # Drop old TTV columns if present (reload mode)
    for col in mr_ttv.columns:
        if col in df.columns: df = df.drop(columns=[col])
    df = pd.concat([df, mr_ttv], axis=1)

    # Account segment by SAM TTV
    df["account_segment"] = df["est_ttv_annual_eur"].apply(assign_account_segment)

    # Penetration score
    pen = df.apply(
        lambda r: penetration_score(r.get("industry", ""), safe_float(r.get("est_mr_annual_eur", 0)), r.get("country", "ES")),
        axis=1)
    pen_df = pd.DataFrame(pen.tolist(), index=df.index)
    for col in pen_df.columns:
        if col in df.columns: df = df.drop(columns=[col])
    df = pd.concat([df, pen_df], axis=1)

    # Growth
    df["growth_score"] = df.apply(
        lambda r: growth_score(r.get("yoy_growth", 0), r.get("mom_growth", 0)), axis=1)

    # Approachability
    if "lead_warmth" not in df.columns:
        df["lead_warmth"] = df.apply(
            lambda r: classify_warmth_from_sw(r.get("in_hubspot_sw", ""), r.get("is_new", "")), axis=1)
    df["warmth_score"] = df["lead_warmth"].map(WARMTH_SCORES).fillna(5)

    # Exclude non-viable
    df = df[df["tier"] != "EXCLUDE"].reset_index(drop=True)

    # Final score
    df["Sales_Priority_Score"] = df.apply(lambda r: compute_final_score(r, weights), axis=1)
    df = df.sort_values("Sales_Priority_Score", ascending=False).reset_index(drop=True)
    return df
