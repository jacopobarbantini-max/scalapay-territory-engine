# Pro API Tag Filter — Re-integration in v4

## Goal

Re-integrate the Similarweb Pro API mode with product tag filtering into the Territory Engine v4 codebase. The feature was previously implemented but lost when v4 was pushed to git without it.

## Context

Territory Engine v4 currently supports three data input modes: CSV upload, reload previous export, and demo. The Pro API mode — which fetches leads directly from Similarweb's internal sales API using cookie authentication — needs to be restored along with the product tag filter that lets users narrow results by Scalapay product categories.

Additionally, an automated test must verify that the `siteTags` filter actually affects Pro API results.

## Files Changed

### Restored from git history

- **`similarweb_cookies.py`** — shared cookie management module (commit `33003a3`). Reads/writes `cookie_meta.json`, provides `load_cookies()`, `save_cookies()`, `is_expired()`, `get_cookie_status()`. No modifications needed.

### Restored from git stash

- **`bnpl_tags_by_category.json`** — static data file mapping ~30k Similarweb tags to 16 Scalapay categories and 62 subcategories. Structure: `{category: {subcategory: [tags]}}`.

### Modified

- **`similarweb_client.py`** — extended with Pro API mode. Current public API + CSV logic stays intact. New additions:
  - Constants: `COUNTRY_CODES` (ES=724, PT=620, FR=250, IT=380, IB=[724,620]), `_CODE_TO_TERRITORY`, `RATE_LIMIT_SECONDS=2`
  - `_resolve_country_codes(countries)` — converts territory names to numeric codes
  - `_build_search_payload(country_codes, page, page_size, filters)` — builds POST body for advanced-search endpoint, passes `siteTags` via the `filters.siteTags` field
  - `_call_pro_api(endpoint_path, payload)` — POST with cookie auth from `similarweb_cookies.py`
  - `_parse_search_rows(rows)` — maps 30+ fields from Pro API response to clean DataFrame (domain, country, traffic, growth, industry, demographics, payment technologies, revenue/employee/transaction buckets, etc.)
  - `_merge_details(df, details, country_codes)` — merges per-country batch detail data into DataFrame
  - `fetch_leads_pro_api(countries, page_size, max_pages, filters)` — orchestrates 3-step flow: paginated search, batch details, parse+merge
  - `ingest()` signature updated: adds `page_size`, `max_pages`, `filters`, `use_pro_api` parameters. When `use_pro_api=True`, calls `fetch_leads_pro_api` instead of `fetch_top_sites_api`.

- **`app.py`** — sidebar extended:
  - 4th radio option: "Pro API (Similarweb)"
  - When Pro API selected:
    - Country selector (IT, FR, ES, IB)
    - `page_size` slider (default 100)
    - `max_pages` slider (default 1)
    - "Filtro Prodotti (Tags)" section:
      - Multiselect for Scalapay categories (from `bnpl_tags_by_category.json`)
      - Dynamic multiselect for subcategories based on selected categories
      - Free-text search across all tags
      - Tag count display with expander preview
    - Builds `api_filters = {"siteTags": selected_tags}` passed to `ingest()`
  - Pipeline call updated to pass `use_pro_api`, `page_size`, `max_pages`, `filters` to `ingest()`

### Created

- **`tests/test_pro_api_tags.py`** — integration test that makes a real Pro API call with `siteTags: ["shoes"]` and verifies results contain the tag. Skips automatically if cookies are unavailable or expired.

## Data Flow

```
Sidebar UI
  -> user selects categories / subcategories / free-text search
  -> collects tags into list
  -> passes {"siteTags": [...]} as filters
  -> ingest(country, use_pro_api=True, filters=filters)
    -> fetch_leads_pro_api(countries, filters=filters)
      -> _build_search_payload(filters={"siteTags": [...]})
      -> POST /sales-api/advanced-search/websites (paginated)
      -> POST /sales-api/advanced-search/websites/details (batch)
      -> _parse_search_rows + _merge_details
    -> DataFrame with 30+ columns
  -> v4 pipeline continues (enrichment -> hubspot -> scoring)
```

## Error Handling

- **Cookies missing or expired**: warning displayed in UI, Pro API option still visible but shows status. Pipeline does not crash — returns empty DataFrame with user-facing message.
- **Pro API returns error**: logged via `get_logger`, user sees warning in pipeline output. No crash.
- **`bnpl_tags_by_category.json` missing**: tag filter section hidden in sidebar. Pro API still works without tag filters.
- **No tags selected**: Pro API called without `siteTags` filter — returns all results (no filter applied).

## What Does NOT Change

- CSV upload mode, Reload mode, Demo mode — untouched
- `config.py`, `scoring.py`, `enrichment.py`, `hubspot_client.py` — not modified
- Downstream pipeline — receives a DataFrame regardless of source, processes identically
- `cookie_meta.json` — already exists with valid cookies, no changes needed

## Test Strategy

1. **Unit tests**: mock `_call_pro_api` to test `_parse_search_rows`, `_merge_details`, `_build_search_payload` with `siteTags` filter
2. **Integration test**: real Pro API call with `siteTags: ["shoes"]`, verify non-empty results and tag presence in response. Auto-skip when cookies unavailable.
3. **Manual verification**: run Streamlit app, select Pro API mode, pick a category, generate — confirm results appear and log shows `siteTags` in payload.
