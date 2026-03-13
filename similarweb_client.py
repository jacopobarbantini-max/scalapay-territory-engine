"""
similarweb_client.py — Similarweb data ingestion.

Triple mode:
  • Pro API mode — calls Similarweb Pro API with cookie auth (siteTags filter).
  • API mode     — calls Similarweb Digital Data API if SIMILARWEB_API_KEY is set.
  • CSV mode     — accepts an uploaded DataFrame (from Streamlit file_uploader).
"""

import os
import time
from typing import Dict, List, Optional

import pandas as pd
import requests

from utils import (
    get_logger,
    clean_similarweb_df,
    normalise_domain,
    parse_revenue_bucket,
    parse_employees_bucket,
    parse_transactions_bucket,
)

log = get_logger(__name__)

from similarweb_cookies import load_cookies, HEADERS, BASE_URL

SW_BASE = "https://api.similarweb.com/v1"

COUNTRY_CODES: Dict[str, object] = {
    "ES": 724,
    "PT": 620,
    "FR": 250,
    "IT": 380,
    "IB": [724, 620],
}

_CODE_TO_TERRITORY: Dict[int, str] = {
    724: "IB",
    620: "IB",
    250: "FR",
    380: "IT",
}

RATE_LIMIT_SECONDS = 2
SITE_TAGS_BATCH_SIZE = 200


# ---------------------------------------------------------------------------
# Pro API helper functions
# ---------------------------------------------------------------------------


def _resolve_country_codes(countries: List[str]) -> List[int]:
    """Convert territory names to deduplicated numeric country codes."""
    seen = set()
    codes: List[int] = []
    for country in countries:
        mapping = COUNTRY_CODES.get(country.upper())
        if mapping is None:
            continue
        if isinstance(mapping, list):
            for code in mapping:
                if code not in seen:
                    seen.add(code)
                    codes.append(code)
        else:
            if mapping not in seen:
                seen.add(mapping)
                codes.append(mapping)
    return codes


def _build_search_payload(
    country_codes: List[int],
    page: int = 1,
    page_size: int = 100,
    filters: Optional[dict] = None,
) -> dict:
    """Build the POST body for the Pro API advanced-search endpoint.

    Matches the real SimilarWeb Lead Generator payload format:
    queryFilters → leadsFilters (countries, industryFilter with tags)
                 → signalsFilters (technologies, traffic signals)
    """
    site_tags = (filters or {}).get("siteTags", [])
    industries = (filters or {}).get("industries", [])

    return {
        "page": page,
        "pageSize": page_size,
        "orderBy": "visits",
        "asc": False,
        "isNewOnly": False,
        "leadGenTrafficAggregationEnabled": True,
        "queryFilters": {
            "leadsFilters": {
                "countries": country_codes,
                "suppressionListIds": [],
                "industryFilter": {
                    "inclusion": "includeOnly",
                    "tags": site_tags,
                    "industryCodesFilter": {
                        "sicCodes": [],
                        "naicsCodes": [],
                    },
                    "values": industries,
                },
                "searchText": "",
            },
            "signalsFilters": {
                "AdNetwork": [],
                "Technology": [],
                "Traffic": [],
                "News": [],
                "Intent": [],
                "mode": "specific",
            },
        },
    }


def _call_pro_api(endpoint_path: str, payload: dict) -> Optional[dict]:
    """POST to BASE_URL + endpoint_path with cookie auth.

    Returns parsed JSON on HTTP 200, None on any error.
    """
    cookies_str = load_cookies()
    if not cookies_str:
        log.warning("No cookies available — cannot call Pro API.")
        return None

    url = BASE_URL + endpoint_path
    headers = dict(HEADERS)
    headers["Cookie"] = cookies_str
    headers["Content-Type"] = "application/json"

    n_tags = len(
        payload.get("queryFilters", {}).get("leadsFilters", {}).get("industryFilter", {}).get("tags", [])
    ) if "queryFilters" in payload else 0
    log.info("Pro API POST %s — payload keys: %s, tags count: %d",
             endpoint_path, list(payload.keys()), n_tags)
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            log.info("Pro API %s — HTTP 200, response keys: %s, rows: %s",
                     endpoint_path, list(data.keys()) if isinstance(data, dict) else type(data),
                     len(data.get("rows", [])) if isinstance(data, dict) and "rows" in data else "N/A")
            return data
        log.error("Pro API returned %s for %s: %s", resp.status_code, endpoint_path, resp.text[:200])
        return None
    except Exception as exc:
        log.error("Pro API request failed for %s: %s", endpoint_path, exc)
        return None


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


def _parse_search_rows(rows: List[dict]) -> pd.DataFrame:
    """Map Pro API search rows to a clean DataFrame."""
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        country_raw = row.get("country")
        try:
            country_code = int(country_raw) if country_raw is not None else None
        except (ValueError, TypeError):
            country_code = None
        territory = _CODE_TO_TERRITORY.get(country_code, str(country_raw) if country_raw is not None else "")

        site_tags_raw = row.get("site_tags") or []
        site_tags = ", ".join(site_tags_raw) if isinstance(site_tags_raw, list) else str(site_tags_raw)

        pay_tech_raw = row.get("techCategory:Payment & Currencies") or []
        payment_technologies = ", ".join(pay_tech_raw) if isinstance(pay_tech_raw, list) else str(pay_tech_raw)

        gender = row.get("male_vs_female_share") or []
        male_share = gender[0] if len(gender) > 0 else None
        female_share = gender[1] if len(gender) > 1 else None

        industry_raw = row.get("industry") or ""
        if isinstance(industry_raw, str) and "/" in industry_raw:
            category = industry_raw.split("/")[-1].strip()
        else:
            category = industry_raw.strip() if isinstance(industry_raw, str) else ""

        revenue_bucket = row.get("company_revenue_range", "")
        employee_bucket = row.get("company_employee_range", "")
        transactions_bucket = row.get("monthly_avg_transactions_range", "")

        record = {
            "domain": normalise_domain(row.get("site", "")),
            "country": territory,
            "monthly_traffic": row.get("visits"),
            "yoy_growth": (row.get("monthly_visits_change_yoy") or 0) * 100,
            "mom_growth": (row.get("monthly_visits_change_mom") or 0) * 100,
            "avg_monthly_visits": row.get("avg_monthly_estimated_visits"),
            "industry": industry_raw,
            "category": category,
            "employees_bucket": employee_bucket,
            "annual_revenue_bucket": revenue_bucket,
            "monthly_transactions_bucket": transactions_bucket,
            "email": row.get("company_email", ""),
            "phone": row.get("company_phone", ""),
            "hq_country": row.get("company_country", ""),
            "top_country": str(row.get("top_geo_country")) if row.get("top_geo_country") is not None else "",
            "in_hubspot_sw": row.get("is_in_hubspot"),
            "is_new": row.get("is_new"),
            "total_page_views": row.get("pageviews"),
            "desktop_page_views": row.get("desktop_pageviews"),
            "mobile_page_views": row.get("mobileweb_pageviews"),
            "bounce_rate": row.get("bounce_rate"),
            "direct_visits": row.get("direct_visits"),
            "referrals_visits": row.get("referrals_visits"),
            "paid_search_share": row.get("paid_search_visits_share"),
            "international_visits": row.get("international_visits"),
            "business_model": row.get("business_model", ""),
            "linkedin_url": row.get("linkedin_url", ""),
            "site_tags": site_tags,
            "payment_technologies": payment_technologies,
            "male_share": male_share,
            "female_share": female_share,
            "age_18_24": row.get("age_group_18_24_share"),
            "age_25_34": row.get("age_group_25_34_share"),
            "age_35_44": row.get("age_group_35_44_share"),
            "age_45_54": row.get("age_group_45_54_share"),
            "age_55_64": row.get("age_group_55_64_share"),
            "age_65_plus": row.get("age_group_65_share"),
        }

        record["annual_revenue_est"] = parse_revenue_bucket(revenue_bucket)
        record["employees_est"] = parse_employees_bucket(employee_bucket)
        record["monthly_transactions_est"] = parse_transactions_bucket(transactions_bucket)

        records.append(record)

    df = pd.DataFrame(records)
    if "domain" in df.columns:
        df = df[df["domain"] != ""].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Details merge
# ---------------------------------------------------------------------------

_DETAIL_FIELD_MAP = {
    "visits": "monthly_traffic",
    "monthly_visits_change_yoy": "yoy_growth",
    "monthly_visits_change_mom": "mom_growth",
    "avg_monthly_estimated_visits": "avg_monthly_visits",
    "pageviews": "total_page_views",
    "desktop_pageviews": "desktop_page_views",
    "mobileweb_pageviews": "mobile_page_views",
    "bounce_rate": "bounce_rate",
    "direct_visits": "direct_visits",
    "referrals_visits": "referrals_visits",
    "paid_search_visits_share": "paid_search_share",
    "international_visits": "international_visits",
}

_YOY_MOM_FIELDS = {"monthly_visits_change_yoy", "monthly_visits_change_mom"}


def _merge_details(df: pd.DataFrame, details: dict, country_codes: List[int]) -> None:
    """Merge per-country detail data into *df* in-place.

    Only overwrites cells that are currently NaN or zero.
    """
    for domain, field_map in details.items():
        mask = df["domain"] == normalise_domain(domain)
        if not mask.any():
            continue
        idx = df.index[mask][0]

        for api_field, col_name in _DETAIL_FIELD_MAP.items():
            if api_field not in field_map:
                continue
            country_values = field_map[api_field]
            if not isinstance(country_values, dict):
                continue

            value = None
            for code in country_codes:
                v = country_values.get(str(code))
                if v is not None:
                    value = v
                    break

            if value is None:
                continue

            if api_field in _YOY_MOM_FIELDS:
                value = value * 100

            if col_name not in df.columns:
                continue

            current = df.at[idx, col_name]
            try:
                is_missing = pd.isna(current) or current == 0
            except (TypeError, ValueError):
                is_missing = current is None

            if is_missing:
                df.at[idx, col_name] = value


# ---------------------------------------------------------------------------
# Pro API main ingestion
# ---------------------------------------------------------------------------


def _fetch_single_tag_batch(
    country_codes: List[int],
    filters: Optional[dict],
    page_size: int,
    max_pages: int,
) -> List[dict]:
    """Run paginated search for a single set of filters (one tag batch)."""
    all_rows: List[dict] = []
    for page in range(1, max_pages + 1):
        if page > 1:
            time.sleep(RATE_LIMIT_SECONDS)

        payload = _build_search_payload(country_codes, page=page, page_size=page_size, filters=filters)
        n_tags = len((filters or {}).get("siteTags", []))
        log.info("Pro API search page %d — %d siteTags", page, n_tags)
        result = _call_pro_api("/sales-api/advanced-search/websites", payload)

        if result is None:
            log.warning("Search page %d returned None — stopping pagination.", page)
            break

        page_rows = result.get("rows", [])
        if not page_rows:
            log.info("No more rows on page %d — stopping pagination.", page)
            break

        all_rows.extend(page_rows)
        log.info("Fetched page %d: %d rows (total so far: %d)", page, len(page_rows), len(all_rows))

        if len(page_rows) < page_size:
            break

    return all_rows


def fetch_leads_pro_api(
    countries: List[str],
    page_size: int = 100,
    max_pages: int = 1,
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Fetch leads from the Similarweb Pro API.

    If siteTags exceed SITE_TAGS_BATCH_SIZE, splits into batches and
    deduplicates results by domain (keeping the row with highest visits).
    """
    cookies = load_cookies()
    if not cookies:
        log.warning("No cookies — skipping Pro API fetch.")
        return pd.DataFrame()

    country_codes = _resolve_country_codes(countries)
    if not country_codes:
        log.warning("No valid country codes resolved from: %s", countries)
        return pd.DataFrame()

    # Build tag batches
    all_tags = (filters or {}).get("siteTags", [])
    other_filters = {k: v for k, v in (filters or {}).items() if k != "siteTags"}

    if len(all_tags) <= SITE_TAGS_BATCH_SIZE:
        tag_batches = [all_tags] if all_tags else [[]]
    else:
        tag_batches = [
            all_tags[i : i + SITE_TAGS_BATCH_SIZE]
            for i in range(0, len(all_tags), SITE_TAGS_BATCH_SIZE)
        ]
        log.info("Splitting %d siteTags into %d batches of ≤%d",
                 len(all_tags), len(tag_batches), SITE_TAGS_BATCH_SIZE)

    # Step 1: Paginated search per batch
    all_rows: List[dict] = []
    seen_domains: set = set()
    for batch_idx, tag_batch in enumerate(tag_batches):
        if batch_idx > 0:
            time.sleep(RATE_LIMIT_SECONDS)

        batch_filters = dict(other_filters)
        if tag_batch:
            batch_filters["siteTags"] = tag_batch

        log.info("Tag batch %d/%d — %d tags", batch_idx + 1, len(tag_batches), len(tag_batch))
        batch_rows = _fetch_single_tag_batch(country_codes, batch_filters, page_size, max_pages)

        # Deduplicate across batches: keep first occurrence (highest visits within batch)
        for row in batch_rows:
            domain = normalise_domain(row.get("site", ""))
            if domain and domain not in seen_domains:
                seen_domains.add(domain)
                all_rows.append(row)

        log.info("Batch %d/%d done: %d new rows (total unique: %d)",
                 batch_idx + 1, len(tag_batches), len(batch_rows), len(all_rows))

    if not all_rows:
        return pd.DataFrame()

    # Step 2: Batch details
    domains = [normalise_domain(r.get("site", "")) for r in all_rows if r.get("site")]
    domains = [d for d in domains if d]

    details: dict = {}
    if domains:
        details_payload = {"domains": domains, "countries": country_codes}
        details_result = _call_pro_api("/sales-api/advanced-search/websites/details", details_payload)
        if details_result and isinstance(details_result, dict):
            details = details_result

    # Step 3: Parse and merge
    df = _parse_search_rows(all_rows)
    if df.empty:
        return df

    if details:
        _merge_details(df, details, country_codes)

    return df


def _get_api_key() -> Optional[str]:
    return os.getenv("SIMILARWEB_API_KEY", "").strip() or None


# ── PUBLIC API MODE ────────────────────────────────────────────────
def fetch_top_sites_api(
    country: str = "es",
    category: str = "all",
    limit: int = 200,
) -> pd.DataFrame:
    """Fetch top websites from Similarweb API for a given country."""
    api_key = _get_api_key()
    if not api_key:
        log.warning("No SIMILARWEB_API_KEY found — use CSV upload instead.")
        return pd.DataFrame()

    url = (
        f"{SW_BASE}/TopSites/category/{category}/country/{country}"
        f"?api_key={api_key}&limit={limit}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        sites = data.get("TopSites", data.get("top_sites", []))
        df = pd.json_normalize(sites)
        return clean_similarweb_df(df)
    except Exception as exc:
        log.error(f"Similarweb API error: {exc}")
        return pd.DataFrame()


def fetch_domain_traffic_api(domain: str) -> dict:
    """Get traffic details for a single domain."""
    api_key = _get_api_key()
    if not api_key:
        return {}
    url = (
        f"{SW_BASE}/website/{domain}/total-traffic-and-engagement/visits"
        f"?api_key={api_key}&country=world&granularity=monthly&main_domain_only=false"
    )
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.error(f"SW traffic API error for {domain}: {exc}")
        return {}


# ── CSV MODE ────────────────────────────────────────────────
def load_from_csv(uploaded_file) -> pd.DataFrame:
    """Parse a Similarweb CSV/XLSX export uploaded via Streamlit."""
    try:
        name = getattr(uploaded_file, "name", "file.csv")
        if name.endswith(".xlsx") or name.endswith(".xls"):
            xls = pd.ExcelFile(uploaded_file)
            if "Accounts" in xls.sheet_names:
                df = pd.read_excel(uploaded_file, sheet_name="Accounts")
            else:
                df = pd.read_excel(uploaded_file, sheet_name=0)
            # Handle case where header row got shifted
            if df.columns[0] != "Domain" and "Domain" in df.iloc[0].values:
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
        else:
            df = pd.read_csv(uploaded_file)
        log.info(f"Loaded {len(df)} rows from {name}")
        return clean_similarweb_df(df)
    except Exception as exc:
        log.error(f"CSV parse error: {exc}")
        return pd.DataFrame()


# ── UNIFIED ENTRYPOINT ──────────────────────────────────────
def ingest(
    country: str,
    uploaded_file=None,
    use_pro_api: bool = False,
    page_size: int = 100,
    max_pages: int = 1,
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Main entry: try CSV first, then Pro API or public API."""
    if uploaded_file is not None:
        df = load_from_csv(uploaded_file)
        if not df.empty:
            df["country"] = country.upper()
            return df

    if use_pro_api:
        df = fetch_leads_pro_api(
            [country.upper()],
            page_size=page_size,
            max_pages=max_pages,
            filters=filters,
        )
    else:
        df = fetch_top_sites_api(country=country.lower())

    if not df.empty:
        df["country"] = country.upper()
    return df
