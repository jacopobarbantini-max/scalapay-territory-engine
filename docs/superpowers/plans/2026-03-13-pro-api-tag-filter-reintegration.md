# Pro API Tag Filter Re-integration — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-integrate Similarweb Pro API mode with product tag filtering into Territory Engine v4, including an integration test that verifies `siteTags` filters work against the live API.

**Architecture:** Restore `similarweb_cookies.py` from git history for cookie auth. Extend `similarweb_client.py` with Pro API functions (search, details, parse, merge). Add Pro API mode + tag filter UI to `app.py` sidebar. Restore `bnpl_tags_by_category.json` from git stash.

**Tech Stack:** Python 3, pandas, requests, Streamlit, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `similarweb_cookies.py` | Restore from git (`33003a3`) | Cookie auth for Pro API |
| `bnpl_tags_by_category.json` | Restore from git stash | Tag-to-category mapping data |
| `similarweb_client.py` | Modify (lines 1–112 kept, new code appended) | Add Pro API fetch + tag filters |
| `app.py` | Modify (sidebar, can_run, run_pipeline, logger) | Pro API mode UI + pipeline branch |
| `tests/test_similarweb_client.py` | Create | Unit tests for Pro API functions |
| `tests/test_pro_api_tags.py` | Create | Integration test with real API |

---

## Chunk 1: Backend — Cookie Module + Pro API Client

### Task 1: Restore `similarweb_cookies.py`

**Files:**
- Create: `similarweb_cookies.py` (restore from git history)

- [ ] **Step 1: Restore the file from git history**

```bash
git show 33003a3:similarweb_cookies.py > similarweb_cookies.py
```

- [ ] **Step 2: Verify the module loads**

```bash
python -c "from similarweb_cookies import load_cookies, HEADERS, BASE_URL, is_expired, get_cookie_status; print('OK:', bool(load_cookies()), BASE_URL, 'expired:', is_expired())"
```

Expected: `OK: True https://pro.similarweb.com expired: False`

- [ ] **Step 3: Commit**

```bash
git add similarweb_cookies.py
git commit -m "feat: restore similarweb_cookies.py from git history"
```

---

### Task 2: Unit tests for Pro API helper functions

**Files:**
- Create: `tests/test_similarweb_client.py`

- [ ] **Step 1: Write unit tests for `_build_search_payload`, `_parse_search_rows`, `_merge_details`**

```python
"""Unit tests for Pro API functions in similarweb_client.py."""
import pandas as pd
import pytest


def test_build_search_payload_with_site_tags():
    from similarweb_client import _build_search_payload

    payload = _build_search_payload(
        country_codes=[380],
        page=1,
        page_size=50,
        filters={"siteTags": ["shoes", "bags"]},
    )
    assert payload["countries"] == [380]
    assert payload["page"] == 1
    assert payload["pageSize"] == 50
    assert payload["filters"]["siteTags"] == ["shoes", "bags"]
    # Default filter keys should still be present
    assert "industries" in payload["filters"]
    assert "technologies" in payload["filters"]


def test_build_search_payload_without_filters():
    from similarweb_client import _build_search_payload

    payload = _build_search_payload(country_codes=[724, 620], page=2, page_size=100)
    assert payload["countries"] == [724, 620]
    assert payload["page"] == 2
    assert "siteTags" not in payload["filters"]


def test_resolve_country_codes():
    from similarweb_client import _resolve_country_codes

    assert _resolve_country_codes(["IT"]) == [380]
    assert _resolve_country_codes(["IB"]) == [724, 620]
    assert _resolve_country_codes(["FR", "IT"]) == [250, 380]
    # IB should not duplicate if ES also given
    codes = _resolve_country_codes(["IB", "ES"])
    assert codes == [724, 620]  # ES=724 already in IB
    # Unknown territory ignored
    assert _resolve_country_codes(["XX"]) == []


def test_parse_search_rows_basic():
    from similarweb_client import _parse_search_rows

    rows = [
        {
            "site": "example.com",
            "country": 380,
            "visits": 500000,
            "monthly_visits_change_yoy": 0.25,
            "monthly_visits_change_mom": -0.05,
            "avg_monthly_estimated_visits": 480000,
            "industry": "Apparel/Clothing",
            "company_revenue_range": "10M - 15M",
            "company_employee_range": "100 - 200",
            "monthly_avg_transactions_range": "5K - 10K",
            "company_email": "info@example.com",
            "company_phone": "+39123456",
            "company_country": "Italy",
            "top_geo_country": 380,
            "is_in_hubspot": False,
            "is_new": True,
            "pageviews": 1200000,
            "desktop_pageviews": 800000,
            "mobileweb_pageviews": 400000,
            "bounce_rate": 0.45,
            "direct_visits": 200000,
            "referrals_visits": 50000,
            "paid_search_visits_share": 0.12,
            "international_visits": 100000,
            "business_model": "e-commerce",
            "linkedin_url": "https://linkedin.com/company/example",
            "site_tags": ["shoes", "fashion"],
            "techCategory:Payment & Currencies": ["Stripe", "PayPal"],
            "male_vs_female_share": [0.4, 0.6],
            "age_group_18_24_share": 0.15,
            "age_group_25_34_share": 0.35,
            "age_group_35_44_share": 0.25,
            "age_group_45_54_share": 0.15,
            "age_group_55_64_share": 0.07,
            "age_group_65_share": 0.03,
        }
    ]
    df = _parse_search_rows(rows)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["domain"] == "example.com"
    assert row["country"] == "IT"
    assert row["monthly_traffic"] == 500000
    assert row["yoy_growth"] == 25.0  # 0.25 * 100
    assert row["mom_growth"] == -5.0  # -0.05 * 100
    assert row["industry"] == "Apparel/Clothing"
    assert row["category"] == "Clothing"
    assert row["email"] == "info@example.com"
    assert "shoes" in row["site_tags"]
    assert "Stripe" in row["payment_technologies"]
    assert row["male_share"] == 0.4
    assert row["female_share"] == 0.6


def test_parse_search_rows_ib_territory():
    """Verify country code 724 (ES) maps to territory 'IB'."""
    from similarweb_client import _parse_search_rows

    rows = [{"site": "tienda.es", "country": 724, "visits": 1000}]
    df = _parse_search_rows(rows)
    assert df.iloc[0]["country"] == "IB"


def test_parse_search_rows_empty():
    from similarweb_client import _parse_search_rows

    df = _parse_search_rows([])
    assert df.empty


def test_merge_details_overwrites_nan():
    from similarweb_client import _merge_details

    df = pd.DataFrame({
        "domain": ["example.com"],
        "monthly_traffic": [0],
        "yoy_growth": [float("nan")],
        "bounce_rate": [0.5],  # Non-zero — should NOT be overwritten
    })
    details = {
        "example.com": {
            "visits": {"380": 999999},
            "monthly_visits_change_yoy": {"380": 0.33},
            "bounce_rate": {"380": 0.8},  # Should NOT overwrite existing 0.5
        }
    }
    _merge_details(df, details, [380])
    assert df.at[0, "monthly_traffic"] == 999999  # Was 0 -> overwritten
    assert df.at[0, "yoy_growth"] == 33.0  # Was NaN -> overwritten, 0.33 * 100
    assert df.at[0, "bounce_rate"] == 0.5  # Was 0.5 -> NOT overwritten


def test_fetch_leads_pro_api_mocked(monkeypatch):
    """Test fetch_leads_pro_api orchestration with mocked API calls."""
    from similarweb_client import fetch_leads_pro_api
    import similarweb_client

    search_response = {
        "rows": [{"site": "shop.it", "country": 380, "visits": 5000}],
        "totalCount": 1,
    }
    details_response = {
        "shop.it": {"visits": {"380": 6000}},
    }

    call_log = []

    def mock_call_pro_api(endpoint, payload):
        call_log.append(endpoint)
        if "details" in endpoint:
            return details_response
        return search_response

    monkeypatch.setattr(similarweb_client, "_call_pro_api", mock_call_pro_api)
    monkeypatch.setattr("similarweb_cookies.load_cookies", lambda: "fake-cookie")

    df = fetch_leads_pro_api(["IT"], page_size=10, max_pages=1, filters={"siteTags": ["shoes"]})
    assert not df.empty
    assert len(call_log) == 2  # search + details
    assert "/sales-api/advanced-search/websites" in call_log[0]
    assert "details" in call_log[1]


def test_ingest_backwards_compatible():
    """Verify existing call sites work without new params."""
    import inspect
    from similarweb_client import ingest

    sig = inspect.signature(ingest)
    params = sig.parameters
    # New params must have defaults
    assert params["use_pro_api"].default is False
    assert params["page_size"].default == 100
    assert params["max_pages"].default == 1
    assert params["filters"].default is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_similarweb_client.py -v
```

Expected: FAIL — `_build_search_payload`, `_resolve_country_codes`, `_parse_search_rows`, `_merge_details` not yet defined in `similarweb_client.py`

- [ ] **Step 3: Commit test file**

```bash
git add tests/test_similarweb_client.py
git commit -m "test: add unit tests for Pro API helper functions (red)"
```

---

### Task 3: Implement Pro API functions in `similarweb_client.py`

**Files:**
- Modify: `similarweb_client.py`

- [ ] **Step 1: Update imports and add constants**

At line 10, change `from typing import Optional` to:

```python
from typing import Dict, List, Optional
```

At line 12, add after `import requests`:

```python
import time
```

At line 15, change `from utils import get_logger, clean_similarweb_df` to:

```python
from utils import (
    get_logger,
    clean_similarweb_df,
    normalise_domain,
    parse_revenue_bucket,
    parse_employees_bucket,
    parse_transactions_bucket,
)
```

Add after `log = get_logger(__name__)`:

```python
from similarweb_cookies import load_cookies, HEADERS, BASE_URL

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
```

- [ ] **Step 2: Add `_resolve_country_codes` function**

Add after the constants:

```python
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
```

- [ ] **Step 3: Add `_build_search_payload` function**

```python
def _build_search_payload(
    country_codes: List[int],
    page: int = 1,
    page_size: int = 100,
    filters: Optional[dict] = None,
) -> dict:
    """Build the POST body for the Pro API advanced-search endpoint."""
    default_filters: dict = {
        "mode": "specific",
        "industries": [],
        "technologies": [],
        "employeeRange": [],
        "revenueRange": [],
        "businessModel": [],
        "siteTags": [],
    }
    if filters:
        default_filters.update(filters)

    return {
        "countries": country_codes,
        "page": page,
        "pageSize": page_size,
        "orderBy": "visits",
        "asc": False,
        "isNewOnly": False,
        "leadGenTrafficAggregationEnabled": True,
        "filters": default_filters,
    }
```

- [ ] **Step 4: Add `_call_pro_api` function**

```python
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

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        log.error("Pro API returned %s for %s: %s", resp.status_code, endpoint_path, resp.text[:200])
        return None
    except Exception as exc:
        log.error("Pro API request failed for %s: %s", endpoint_path, exc)
        return None
```

- [ ] **Step 5: Add `_parse_search_rows` function**

```python
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
```

Note: This function uses `normalise_domain`, `parse_revenue_bucket`, `parse_employees_bucket`, `parse_transactions_bucket` from `utils.py` — these were already added to the import in Step 1.

- [ ] **Step 6: Add detail merge constants and `_merge_details` function**

```python
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
```

- [ ] **Step 7: Add `fetch_leads_pro_api` function**

```python
def fetch_leads_pro_api(
    countries: List[str],
    page_size: int = 100,
    max_pages: int = 1,
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Fetch leads from the Similarweb Pro API.

    Three-step flow: paginated search, batch details, parse+merge.
    """
    cookies = load_cookies()
    if not cookies:
        log.warning("No cookies — skipping Pro API fetch.")
        return pd.DataFrame()

    country_codes = _resolve_country_codes(countries)
    if not country_codes:
        log.warning("No valid country codes resolved from: %s", countries)
        return pd.DataFrame()

    # Step 1: Paginated search
    all_rows: List[dict] = []
    for page in range(1, max_pages + 1):
        if page > 1:
            time.sleep(RATE_LIMIT_SECONDS)

        payload = _build_search_payload(country_codes, page=page, page_size=page_size, filters=filters)
        log.info("Pro API search page %d — siteTags: %s", page, (filters or {}).get("siteTags", []))
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
```

- [ ] **Step 8: Update `ingest()` signature with backwards-compatible defaults**

Replace **only** the `# ── UNIFIED ENTRYPOINT ──` section (lines 96–112, from the comment through end of file). Do NOT touch `load_from_csv` above it:

```python
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
```

- [ ] **Step 9: Run unit tests to verify they pass**

```bash
python -m pytest tests/test_similarweb_client.py -v
```

Expected: ALL PASS (10 tests)

- [ ] **Step 10: Commit**

```bash
git add similarweb_client.py tests/test_similarweb_client.py
git commit -m "feat: add Pro API functions with siteTags filter support"
```

---

### Task 4: Restore `bnpl_tags_by_category.json` from git stash

**Files:**
- Restore: `bnpl_tags_by_category.json`

- [ ] **Step 1: Extract the file from git stash**

The file was stashed in `stash@{0}`. Restore it:

```bash
git checkout stash@{0} -- bnpl_tags_by_category.json
```

If that fails (the file was untracked, not staged in stash), try:

```bash
git show stash@{0}^3:bnpl_tags_by_category.json > bnpl_tags_by_category.json 2>/dev/null
```

If the stash does not contain it, check if it still exists on disk or in another stash.

- [ ] **Step 2: Verify the file is valid JSON**

```bash
python -c "import json; d=json.load(open('bnpl_tags_by_category.json')); print(f'{len(d)} categories, {sum(len(v) for v in d.values())} subcategories')"
```

Expected: `16 categories, ...` (some number of subcategories)

- [ ] **Step 3: Commit**

```bash
git add bnpl_tags_by_category.json
git commit -m "feat: restore bnpl_tags_by_category.json tag mapping data"
```

---

## Chunk 2: Frontend — App.py Sidebar + Pipeline

### Task 5: Add Pro API mode to `app.py` sidebar and pipeline

**Files:**
- Modify: `app.py` (lines 5, 39, 66–93, 304–341, 405–409)

- [ ] **Step 1: Add `json` import and Pro API variables**

At `app.py:5`, change:

```python
import io, os, logging
```

to:

```python
import io, json, os, logging
```

- [ ] **Step 2: Add `similarweb_cookies` to logger registration**

At `app.py:39`, change:

```python
for logger_name in ["app", "enrichment", "hubspot_client", "scoring", "utils", "similarweb_client"]:
```

to:

```python
for logger_name in ["app", "enrichment", "hubspot_client", "scoring", "utils", "similarweb_client", "similarweb_cookies"]:
```

- [ ] **Step 3: Add 4th radio option and `use_pro_api` variable**

At `app.py:71-77`, change the radio and mode variables:

```python
    data_mode = st.radio("Similarweb input", [
        "📁 Upload (XLSX/CSV)",
        "🔄 Reload Export (add CRM)",
        "🧪 Demo (sample data)",
    ], index=0)
    use_sample = "Demo" in data_mode
    use_reload = "Reload" in data_mode
```

to:

```python
    data_mode = st.radio("Similarweb input", [
        "📁 Upload (XLSX/CSV)",
        "🔄 Reload Export (add CRM)",
        "🔌 Pro API (Similarweb)",
        "🧪 Demo (sample data)",
    ], index=0)
    use_sample = "Demo" in data_mode
    use_reload = "Reload" in data_mode
    use_pro_api = "Pro API" in data_mode
```

- [ ] **Step 4: Add Pro API controls in sidebar**

After the `elif not use_sample:` upload block (after `app.py:92`), add a new `elif` for Pro API **before** the integrations section:

```python
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
```

Note: Also add `api_filters = None` initialization for non-Pro API modes. Add after `file_ib = file_fr = file_it = reload_file = None` (line 80):

```python
    api_filters = None
    page_size = 100
    max_pages = 1
    pro_api_country = "IT"
```

- [ ] **Step 5: Update `can_run` guard**

At `app.py:408`, change:

```python
    can_run = (use_sample or use_reload and reload_file or file_ib or file_fr or file_it) and w_sum <= 100
```

to:

```python
    can_run = (use_sample or use_pro_api or (use_reload and reload_file) or file_ib or file_fr or file_it) and w_sum <= 100
```

- [ ] **Step 6: Update `run_pipeline()` to handle Pro API mode**

In `run_pipeline()`, at `app.py:320-341`, change the ingestion block:

```python
    if use_sample:
        for country in ["ES", "FR"]:
            df = load_sample_data(country)
            if not df.empty:
                all_dfs.append(df)
                st.success(f"✅ {FLAGS.get(country,'')} {country}: {len(df)} demo leads")
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
```

to:

```python
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
```

- [ ] **Step 7: Verify the app loads without errors**

```bash
python -c "import ast; ast.parse(open('app.py').read()); print('Syntax OK')"
```

Expected: `Syntax OK`

- [ ] **Step 8: Run existing tests to make sure nothing broke**

```bash
python -m pytest tests/ -v
```

Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add app.py
git commit -m "feat: add Pro API mode with tag filter to app.py sidebar"
```

---

## Chunk 3: Integration Test

### Task 6: Integration test — verify `siteTags` filter works against live API

**Files:**
- Create: `tests/test_pro_api_tags.py`

- [ ] **Step 1: Write integration test**

```python
"""Integration test: verify siteTags filter works with live Similarweb Pro API.

Requires valid cookies in cookie_meta.json. Auto-skips if cookies are
missing or expired.
"""
import pytest
from similarweb_cookies import load_cookies, is_expired
from similarweb_client import fetch_leads_pro_api

_skip = not load_cookies() or is_expired()


@pytest.mark.skipif(_skip, reason="No valid Similarweb cookies available")
def test_pro_api_with_site_tags_filter():
    """Fetch IT leads filtered by siteTags=['shoes'] and verify results."""
    df = fetch_leads_pro_api(
        countries=["IT"],
        page_size=10,
        max_pages=1,
        filters={"siteTags": ["shoes"]},
    )

    # Must return results
    assert not df.empty, "Pro API returned empty DataFrame with siteTags=['shoes']"
    assert "domain" in df.columns, "Missing 'domain' column in results"
    assert "site_tags" in df.columns, "Missing 'site_tags' column in results"

    # At least one result should have 'shoes' in its site_tags
    has_shoes = df["site_tags"].str.lower().str.contains("shoes", na=False).any()
    assert has_shoes, f"No results contain 'shoes' in site_tags. Tags found: {df['site_tags'].tolist()[:5]}"


@pytest.mark.skipif(_skip, reason="No valid Similarweb cookies available")
def test_pro_api_without_filter_returns_results():
    """Fetch IT leads without siteTags filter — should return results."""
    df = fetch_leads_pro_api(
        countries=["IT"],
        page_size=5,
        max_pages=1,
    )

    assert not df.empty, "Pro API returned empty DataFrame without filters"
    assert "domain" in df.columns
```

- [ ] **Step 2: Run integration tests**

```bash
python -m pytest tests/test_pro_api_tags.py -v
```

Expected: PASS (if cookies are valid), SKIPPED (if cookies expired)

- [ ] **Step 3: Run ALL tests together**

```bash
python -m pytest tests/ -v
```

Expected: ALL PASS (unit tests pass, integration tests pass or skip)

- [ ] **Step 4: Commit**

```bash
git add tests/test_pro_api_tags.py
git commit -m "test: add integration test verifying siteTags filter with live Pro API"
```

---

## Final Verification

- [ ] **Run full test suite:**

```bash
python -m pytest tests/ -v
```

- [ ] **Manual verification** (optional but recommended):

```bash
streamlit run app.py --server.port 8502 --server.headless true
```

1. Open http://localhost:8502
2. Select "Pro API (Similarweb)" radio
3. Choose territory IT, page_size 10, max_pages 1
4. Select category "Apparel & Fashion"
5. Click Generate
6. Verify results appear in the dashboard
7. Check pipeline logs at bottom — should show `siteTags: [...]` in the log
