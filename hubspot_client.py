"""
hubspot_client.py — HubSpot CRM bulk-fetch + fuzzy domain matching.
v3: Fetches ALL companies once via pagination, then matches locally.
     Reduces API calls from ~18,000 to ~200.
"""
import os, re, time
from typing import Dict, Optional
import pandas as pd
import requests
from config import (
    HS_DEAL_STAGES_WON, HS_DEAL_STAGES_WARM, HS_DEAL_STAGES_LOST,
    CLOSED_LOST_REACTIVATION_MONTHS,
)
from utils import get_logger, normalise_domain
log = get_logger(__name__)

def _get_api_key():
    return os.getenv("HUBSPOT_API_KEY", "").strip() or None

def _extract_root(domain: str) -> str:
    """Extract brand root: 'zooplus.es' → 'zooplus', 'www.bershka.com' → 'bershka'."""
    d = normalise_domain(domain)
    parts = d.split(".")
    if len(parts) >= 2:
        brand = parts[-2]
        if brand in ("co", "com", "org", "net") and len(parts) >= 3:
            brand = parts[-3]
        return brand.lower()
    return d.lower()

# ── BULK FETCH ALL COMPANIES ────────────────────────────────
def _bulk_fetch_companies(api_key: str, progress_callback=None) -> Dict:
    """
    Paginate through ALL HubSpot companies. Returns dict:
      { normalised_domain: { name, country, company_id } }
    Also builds a root_brand → [domains] index for fuzzy matching.
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    domain_map = {}  # domain → company info
    after = None
    page = 0
    total_fetched = 0

    while True:
        url = "https://api.hubapi.com/crm/v3/objects/companies"
        params = {
            "limit": 100,
            "properties": "domain,name,country,hs_object_id",
        }
        if after:
            params["after"] = after

        for attempt in range(3):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 10))
                    log.warning(f"HS rate limit, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    time.sleep(10)
                    continue
                log.error(f"HS fetch page {page} error: {e}")
                return domain_map
            except Exception as e:
                log.error(f"HS fetch page {page} error: {e}")
                return domain_map
        else:
            log.error(f"HS fetch page {page} failed after 3 retries")
            break

        results = data.get("results", [])
        for company in results:
            props = company.get("properties", {})
            raw_domain = (props.get("domain") or "").strip()
            if not raw_domain:
                continue
            dom = normalise_domain(raw_domain)
            if not dom:
                continue
            domain_map[dom] = {
                "name": props.get("name", ""),
                "country": props.get("country", ""),
                "company_id": props.get("hs_object_id", company.get("id", "")),
            }
            total_fetched += 1

        page += 1
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")

        if progress_callback:
            progress_callback(total_fetched)

        if not after:
            break

        # Respect rate limits: small delay between pages
        time.sleep(0.15)

    log.info(f"HubSpot bulk fetch: {total_fetched} companies in {page} pages")
    return domain_map

# ── FETCH DEALS FOR A COMPANY ───────────────────────────────
def _fetch_deals_for_company(company_id: str, headers: dict) -> list:
    """Get associated deals for a company. Returns list of deal dicts."""
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}/associations/deals"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 429:
            time.sleep(10)
            resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return []
        assoc = resp.json().get("results", [])
        if not assoc:
            return []

        # Fetch deal details
        deals = []
        for a in assoc[:5]:  # Max 5 deals per company
            deal_id = a.get("id", "")
            if not deal_id:
                continue
            deal_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
            params = {"properties": "dealstage,dealname,hubspot_owner_id,amount,closedate,notes_last_contacted"}
            try:
                dr = requests.get(deal_url, params=params, headers=headers, timeout=10)
                if dr.status_code == 429:
                    time.sleep(5)
                    dr = requests.get(deal_url, params=params, headers=headers, timeout=10)
                if dr.status_code == 200:
                    deals.append(dr.json().get("properties", {}))
            except Exception:
                pass
            time.sleep(0.1)
        return deals
    except Exception as e:
        log.error(f"Deal fetch error for company {company_id}: {e}")
        return []

# ── CLASSIFY WARMTH ─────────────────────────────────────────
# Holiday windows for Southern Europe
_HOLIDAY_WINDOWS = [
    (7, 20, 8, 31),    # Summer: Jul 20 → Aug 31
    (12, 18, 1, 8),    # Christmas/NY: Dec 18 → Jan 8 (year wraps)
    (3, 28, 4, 8),     # Easter: late March → early April
]

def _effective_business_days(last_contact_dt, check_dt):
    """
    Calendar days minus actual holiday overlap.
    Precisely counts how many days in the gap fall inside holiday windows.
    Example: contacted Jul 15, checking Sep 5 = 52 cal days, 42 holiday = 10 effective.
    """
    from datetime import datetime
    cal_days = (check_dt - last_contact_dt).days
    holiday_days = 0
    for y in [last_contact_dt.year, last_contact_dt.year + 1]:
        for sm, sd, em, ed in _HOLIDAY_WINDOWS:
            if em < sm:  # year-wrapping (Christmas)
                h_start = datetime(y, sm, sd)
                h_end = datetime(y + 1, em, ed)
            else:
                h_start = datetime(y, sm, sd)
                h_end = datetime(y, em, ed)
            ov_start = max(last_contact_dt, h_start)
            ov_end = min(check_dt, h_end)
            if ov_start < ov_end:
                holiday_days += (ov_end - ov_start).days
    return max(cal_days - holiday_days, 0)


def classify_warmth(deals: list) -> str:
    """
    Approachability classification:
      Net New              → not in HubSpot
      Lost >6 months       → deal lost long ago, re-approachable
      Stale Deal           → warm/active deal BUT no contact beyond holiday-adjusted threshold
      In HubSpot (unknown) → exists but no clear deal status
      Lost <6 months       → deal lost recently, bad timing
      Warm (active)        → colleague contacted recently, not approachable
      Existing Won         → already a Scalapay merchant
    """
    if not deals:
        return "Net New"

    from datetime import datetime, timedelta
    now = datetime.utcnow()
    six_months_ago = now - timedelta(days=180)

    stages = [d.get("dealstage", "").lower() for d in deals]

    if any(s in HS_DEAL_STAGES_WON for s in stages):
        return "Existing Won"

    if any(s in HS_DEAL_STAGES_WARM for s in stages):
        for d in deals:
            if d.get("dealstage", "").lower() in HS_DEAL_STAGES_WARM:
                last_contact = d.get("notes_last_contacted", "")
                if last_contact:
                    try:
                        lc_dt = datetime.fromisoformat(last_contact.replace("Z", "+00:00")).replace(tzinfo=None)
                        eff_days = _effective_business_days(lc_dt, now)
                        if eff_days <= 45:
                            return "Warm (active)"
                        else:
                            return "Stale Deal"
                    except (ValueError, TypeError):
                        pass
        return "Warm (active)"

    if any(s in HS_DEAL_STAGES_LOST for s in stages):
        for d in deals:
            if d.get("dealstage", "").lower() in HS_DEAL_STAGES_LOST:
                close_str = d.get("closedate", "")
                if close_str:
                    try:
                        close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00")).replace(tzinfo=None)
                        if close_dt < six_months_ago:
                            return "Lost >6 months"
                    except (ValueError, TypeError):
                        pass
        return "Lost <6 months"

    return "In HubSpot (unknown)"

# ── MAIN ENRICHMENT FUNCTION ───────────────────────────────
def enrich_with_hubspot(df: pd.DataFrame, progress_callback=None) -> pd.DataFrame:
    """
    Enrich DataFrame with HubSpot data using bulk-fetch approach.
    1. Bulk-fetch all HS companies (paginated, ~200 API calls)
    2. Build domain + root-brand lookup dicts
    3. Match each Similarweb lead locally (zero API calls)
    4. For matched companies, fetch deals (rate-limited)
    """
    api_key = _get_api_key()

    # Init columns
    df["hs_exists"] = False
    df["hs_company_name"] = ""
    df["hs_deal_stage"] = ""
    df["hs_deal_owner"] = ""
    df["hs_cross_country"] = False
    df["hs_it_deal_found"] = False

    if not api_key:
        log.warning("No HUBSPOT_API_KEY — skipping CRM enrichment")
        df["lead_warmth"] = "Net New"
        return df

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # Step 1: Bulk fetch all companies
    log.info("Starting HubSpot bulk company fetch...")
    domain_map = _bulk_fetch_companies(api_key, progress_callback)
    if not domain_map:
        log.warning("No companies fetched from HubSpot")
        df["lead_warmth"] = "Net New"
        return df

    # Step 2: Build root-brand index for fuzzy matching
    root_index = {}  # brand_root → list of (domain, company_info)
    for dom, info in domain_map.items():
        root = _extract_root(dom)
        if root not in root_index:
            root_index[root] = []
        root_index[root].append((dom, info))

    log.info(f"Built lookup: {len(domain_map)} domains, {len(root_index)} brand roots")

    # Step 3: Match each lead
    matched_company_ids = set()
    match_results = {}  # idx → company_info

    for idx, row in df.iterrows():
        domain = normalise_domain(row.get("domain", ""))
        if not domain:
            continue

        # Try exact domain match
        if domain in domain_map:
            match_results[idx] = domain_map[domain]
            matched_company_ids.add(domain_map[domain]["company_id"])
            continue

        # Try root-brand match (zooplus.es → zooplus → matches zooplus.it)
        root = _extract_root(domain)
        if root in root_index:
            # Pick best match (prefer same TLD, then any)
            matches = root_index[root]
            if matches and matches[0][1]:
                match_results[idx] = matches[0][1]
                matched_company_ids.add(matches[0][1].get("company_id", ""))

            # Check if any match is from Italy
            for m_dom, m_info in matches:
                if m_info and m_info.get("country", "").lower() in ("italy", "it", "italia"):
                    df.at[idx, "hs_it_deal_found"] = True
                    df.at[idx, "hs_cross_country"] = True
                    break

    log.info(f"Matched {len(match_results)}/{len(df)} leads to HubSpot companies")

    # Step 4: Apply matches
    for idx, info in match_results.items():
        if not info:
            continue
        df.at[idx, "hs_exists"] = True
        df.at[idx, "hs_company_name"] = info.get("name", "")

    # Step 5: Fetch deals for matched companies (rate-limited)
    # Only fetch deals for top-scored or important matches to limit API calls
    company_deals_cache = {}
    companies_to_fetch = list(matched_company_ids)[:500]  # Cap at 500 deal lookups
    log.info(f"Fetching deals for {len(companies_to_fetch)} matched companies...")

    for i, cid in enumerate(companies_to_fetch):
        if i > 0 and i % 50 == 0:
            log.info(f"Deal fetch progress: {i}/{len(companies_to_fetch)}")
            time.sleep(2)  # Extra pause every 50

        deals = _fetch_deals_for_company(cid, headers)
        company_deals_cache[cid] = deals
        time.sleep(0.2)  # Rate limit: ~5 calls/sec

    # Step 6: Apply deal info + warmth classification
    for idx, info in match_results.items():
        cid = info.get("company_id", "")
        deals = company_deals_cache.get(cid, [])
        if deals:
            stages = [d.get("dealstage", "") for d in deals]
            df.at[idx, "hs_deal_stage"] = ", ".join(filter(None, stages))
            owners = [d.get("hubspot_owner_id", "") for d in deals]
            df.at[idx, "hs_deal_owner"] = ", ".join(filter(None, set(owners)))

    # Warmth classification
    df["lead_warmth"] = "Net New"
    for idx, info in match_results.items():
        cid = info.get("company_id", "")
        deals = company_deals_cache.get(cid, [])
        df.at[idx, "lead_warmth"] = classify_warmth(deals)

    # Stats
    warmth_counts = df["lead_warmth"].value_counts()
    log.info(f"Warmth distribution:\n{warmth_counts.to_string()}")

    return df
