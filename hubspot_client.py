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

def _safe_num(val):
    """Convert HubSpot property to float, return 0.0 if invalid."""
    if val is None or val == "" or val == "nan":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

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
            "properties": "domain,name,country,hs_object_id,category,subcategory,"
                          "scalapay__aov,scalapay__cr,aov___tot,aov__fr_,aov__es_,aov__it_,"
                          "fr___monthly_visits,es___monthly_visits,it___monthly_visits,"
                          "hs_parent_company_id,hs_num_child_companies",
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
                "hs_category": props.get("category", ""),
                "hs_subcategory": props.get("subcategory", ""),
                "hs_aov_benchmark": props.get("scalapay__aov", ""),
                "hs_cr_benchmark": props.get("scalapay__cr", ""),
                "hs_aov_tot": props.get("aov___tot", ""),
                "hs_aov_fr": props.get("aov__fr_", ""),
                "hs_aov_es": props.get("aov__es_", ""),
                "hs_aov_it": props.get("aov__it_", ""),
                "hs_visits_fr": props.get("fr___monthly_visits", ""),
                "hs_visits_es": props.get("es___monthly_visits", ""),
                "hs_visits_it": props.get("it___monthly_visits", ""),
                "hs_parent_company_id": props.get("hs_parent_company_id", ""),
                "hs_num_child_companies": props.get("hs_num_child_companies", ""),
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
    Approachability classification with stage-aware stale detection.
    
    Key insight: a deal stuck 45d in "Discovery" is dead.
    A deal stuck 45d in "Contract Negotiation" might just be waiting for legal.
    
    Thresholds (holiday-adjusted):
      Early stages (SQL, Discovery, Target):        30 days → Stale (dead)
      Mid stages (Business Meeting, Negotiation):    45 days → Stale (check with AE)
      Late stages (KYC, Contract, Onboarding):       60 days → Stale (possibly waiting)
    """
    if not deals:
        return "Net New"

    from datetime import datetime, timedelta
    from config import (HS_DEAL_STAGES_WON, HS_DEAL_STAGES_LOST, HS_DEAL_STAGES_WARM,
                        HS_STALE_THRESHOLDS, HS_EARLY_STAGES, HS_LATE_STAGES)
    now = datetime.utcnow()
    six_months_ago = now - timedelta(days=180)

    stages = [d.get("dealstage", "").lower() for d in deals]

    # Won = already a merchant
    if any(s in HS_DEAL_STAGES_WON for s in stages):
        return "Existing Won"

    # Active deal stages — check stale with stage-aware threshold
    if any(s in HS_DEAL_STAGES_WARM for s in stages):
        for d in deals:
            stage = d.get("dealstage", "").lower()
            if stage not in HS_DEAL_STAGES_WARM:
                continue
            last_contact = d.get("notes_last_contacted", "")
            if last_contact:
                try:
                    lc_dt = datetime.fromisoformat(last_contact.replace("Z", "+00:00")).replace(tzinfo=None)
                    eff_days = _effective_business_days(lc_dt, now)

                    # Determine threshold based on deal stage
                    if stage in HS_EARLY_STAGES:
                        threshold = HS_STALE_THRESHOLDS["early"]   # 30d
                    elif stage in HS_LATE_STAGES:
                        threshold = HS_STALE_THRESHOLDS["late"]    # 60d
                    else:
                        threshold = HS_STALE_THRESHOLDS["mid"]     # 45d

                    if eff_days <= threshold:
                        return "Warm (active)"
                    else:
                        # Stage-aware stale label
                        if stage in HS_EARLY_STAGES:
                            return "Stale Deal"    # Dead — re-approachable
                        elif stage in HS_LATE_STAGES:
                            return "Warm"           # Probably waiting, check with AE
                        else:
                            return "Stale Deal"
                except (ValueError, TypeError):
                    pass
        return "Warm (active)"  # Has warm stage but no contact date → assume active

    # Lost deals — check timing
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
        # AOV cascade: real per-merchant > benchmark > config fallback
        df.at[idx, "hs_aov_tot"] = _safe_num(info.get("hs_aov_tot", ""))
        df.at[idx, "hs_aov_fr"] = _safe_num(info.get("hs_aov_fr", ""))
        df.at[idx, "hs_aov_es"] = _safe_num(info.get("hs_aov_es", ""))
        df.at[idx, "hs_aov_it"] = _safe_num(info.get("hs_aov_it", ""))
        df.at[idx, "hs_aov_benchmark"] = _safe_num(info.get("hs_aov_benchmark", ""))
        df.at[idx, "hs_category"] = info.get("hs_category", "")
        df.at[idx, "hs_subcategory"] = info.get("hs_subcategory", "")
        # Cross-country traffic from HubSpot
        df.at[idx, "hs_visits_fr"] = _safe_num(info.get("hs_visits_fr", ""))
        df.at[idx, "hs_visits_es"] = _safe_num(info.get("hs_visits_es", ""))
        df.at[idx, "hs_visits_it"] = _safe_num(info.get("hs_visits_it", ""))

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

    # Step 7: Holding group detection
    # Build company_id → name reverse lookup
    cid_to_name = {}
    for dom, info in domain_map.items():
        cid = info.get("company_id", "")
        if cid:
            cid_to_name[str(cid)] = info.get("name", dom)

    df["hs_holding_group"] = ""
    holding_count = 0
    for idx, info in match_results.items():
        parent_id = str(info.get("hs_parent_company_id", "")).strip()
        n_children = info.get("hs_num_child_companies", "")
        try:
            n_children = int(float(n_children)) if n_children else 0
        except (ValueError, TypeError):
            n_children = 0

        if parent_id and parent_id != "0" and parent_id != "":
            # This company has a parent → it's part of a group
            parent_name = cid_to_name.get(parent_id, f"Group #{parent_id}")
            df.at[idx, "hs_holding_group"] = parent_name
            holding_count += 1
        elif n_children and n_children > 0:
            # This IS a parent company with children
            df.at[idx, "hs_holding_group"] = f"{info.get('name', '')} (parent, {n_children} brands)"
            holding_count += 1

    if holding_count > 0:
        log.info(f"Holding groups: {holding_count} leads linked to parent/child companies")

    # Stats
    warmth_counts = df["lead_warmth"].value_counts()
    log.info(f"Warmth distribution:\n{warmth_counts.to_string()}")

    return df


def extract_non_sw_leads(df, api_key=None):
    """
    Find HubSpot companies NOT in the Similarweb list.
    Returns a DataFrame of re-approachable leads with HubSpot data.
    Excludes: Won deals, active deals (< 45d contact).
    """
    if not api_key:
        api_key = os.getenv("HUBSPOT_API_KEY", "")
    if not api_key:
        log.info("No HubSpot key — skipping non-SW extraction")
        return pd.DataFrame()

    # Get all domains from SW list
    sw_domains = set()
    for d in df["domain"].dropna():
        sw_domains.add(str(d).strip().lower())
        root = _extract_root(str(d).strip().lower())
        sw_domains.add(root)

    # Fetch all HubSpot companies (already cached from bulk fetch)
    all_companies = _bulk_fetch_companies(api_key)
    log.info(f"Non-SW check: {len(all_companies)} HS companies vs {len(sw_domains)} SW domains")

    # Find companies NOT in SW
    non_sw = []
    for domain, info in all_companies.items():
        if not info:
            continue
        d_clean = str(domain).strip().lower()
        root = _extract_root(d_clean)
        if d_clean not in sw_domains and root not in sw_domains:
            non_sw.append({
                "domain": d_clean,
                "hs_company_name": info.get("name", ""),
                "hs_country": info.get("country", ""),
                "hs_company_id": info.get("company_id", ""),
            })

    if not non_sw:
        log.info("No non-SW leads found in HubSpot")
        return pd.DataFrame()

    non_sw_df = pd.DataFrame(non_sw)
    log.info(f"Found {len(non_sw_df)} HubSpot companies not in Similarweb list")

    # Fetch deals for these companies to classify warmth
    headers = {"Authorization": f"Bearer {api_key}"}
    for i, (idx, row) in enumerate(non_sw_df.iterrows()):
        cid = row.get("hs_company_id", "")
        if not cid:
            continue
        try:
            deals = _fetch_deals_for_company(cid, headers)
            warmth = classify_warmth(deals)
            non_sw_df.at[idx, "lead_warmth"] = warmth
            if deals:
                stages = [d.get("dealstage", "") for d in deals]
                non_sw_df.at[idx, "hs_deal_stage"] = ", ".join(filter(None, stages))
        except Exception:
            non_sw_df.at[idx, "lead_warmth"] = "In HubSpot (unknown)"
        if i > 0 and i % 50 == 0:
            log.info(f"Non-SW deals: {i}/{len(non_sw_df)}")
            time.sleep(1)

    if "lead_warmth" not in non_sw_df.columns:
        non_sw_df["lead_warmth"] = "In HubSpot (unknown)"

    # Filter: exclude Won and recently active (Warm)
    non_sw_df = non_sw_df[~non_sw_df["lead_warmth"].isin(["Existing Won", "Warm (active)"])].reset_index(drop=True)
    non_sw_df["source"] = "HubSpot (not in SW)"

    log.info(f"Non-SW re-approachable leads: {len(non_sw_df)}")
    if not non_sw_df.empty:
        warmth = non_sw_df["lead_warmth"].value_counts()
        log.info(f"Non-SW warmth:\n{warmth.to_string()}")

    return non_sw_df
