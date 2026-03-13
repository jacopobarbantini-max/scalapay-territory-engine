# Pro API Tag Filter — Re-integration in v4

## Goal

Re-integrate the Similarweb Pro API mode with product tag filtering into the Territory Engine v4 codebase. The feature was previously implemented but lost when v4 was pushed to git without it.

## Context

Territory Engine v4 currently supports three data input modes: CSV upload, reload previous export, and demo. The Pro API mode — which fetches leads directly from Similarweb's internal sales API using cookie authentication — needs to be restored along with the product tag filter that lets users narrow results by Scalapay product categories.

Additionally, an automated test must verify that the `siteTags` filter actually affects Pro API results.

## Files Changed

### Restored from git history

- **`similarweb_cookies.py`** — shared cookie management module. Restore via `git show 33003a3:similarweb_cookies.py > similarweb_cookies.py`. Reads/writes `cookie_meta.json`, provides `load_cookies()`, `save_cookies()`, `is_expired()`, `get_cookie_status()`. No modifications needed. If git history is unavailable, the full source is ~120 lines and can be reconstructed from the module description above plus `cookie_meta.json`'s schema.

### Restored from git stash

- **`bnpl_tags_by_category.json`** — static data file mapping ~30k Similarweb tags to 16 Scalapay categories and 62 subcategories. Structure: `{category: {subcategory: [tags]}}`. Restore via `git stash pop` or `git stash show -p stash@{0}`. If stash is unavailable, regenerate by running `fetch_all_tags_parallel.py` (also in stash) followed by the category mapping script. Fallback: the tag filter section is hidden when this file is missing — Pro API still works without it.

### Modified

- **`similarweb_client.py`** — extended with Pro API mode. Current public API + CSV logic stays intact. New additions:
  - Import `similarweb_cookies` for `load_cookies`, `HEADERS`, `BASE_URL`
  - Constants: `COUNTRY_CODES` (ES=724, PT=620, FR=250, IT=380, IB=[724,620]), `_CODE_TO_TERRITORY`, `RATE_LIMIT_SECONDS=2`
  - `_resolve_country_codes(countries)` — converts territory names to numeric codes; IB expands to [724, 620]
  - `_build_search_payload(country_codes, page, page_size, filters)` — builds POST body for advanced-search endpoint. The `filters` dict is merged into the default filters structure; `siteTags` goes into `filters.siteTags` as a list of strings
  - `_call_pro_api(endpoint_path, payload)` — POST to `https://pro.similarweb.com` + endpoint_path. Headers: `HEADERS` from `similarweb_cookies` plus `Cookie` (from `load_cookies()`) and `Content-Type: application/json`. Returns parsed JSON on 200, `None` on any error.
  - `_parse_search_rows(rows)` — maps 30+ fields from Pro API response to clean DataFrame. Key fields: `site` -> `domain`, `country` (numeric) -> territory via `_CODE_TO_TERRITORY`, `visits` -> `monthly_traffic`, `monthly_visits_change_yoy/mom` (multiplied by 100), `industry`, `company_revenue_range`, `company_employee_range`, `monthly_avg_transactions_range`, `site_tags`, `techCategory:Payment & Currencies`, `male_vs_female_share`, age groups, etc.
  - `_merge_details(df, details, country_codes)` — merges per-country batch detail data into DataFrame. Details response format: `{domain: {api_field: {country_code_str: value}}}`. All domains from the search are sent in a single batch request (no size limit observed in practice). Only overwrites cells that are NaN or zero.
  - `fetch_leads_pro_api(countries, page_size, max_pages, filters)` — orchestrates 3-step flow: paginated search, batch details, parse+merge
  - `ingest()` signature updated with **backwards-compatible defaults**: `use_pro_api=False`, `page_size=100`, `max_pages=1`, `filters=None`. Existing call sites (`ingest(country, uploaded_file=f)`) continue to work without changes.

- **`app.py`** — changes in three areas:

  1. **Sidebar radio** — 4th option added: `"🔌 Pro API (Similarweb)"`. New variable `use_pro_api = "Pro API" in data_mode`.

  2. **Sidebar Pro API controls** (shown only when `use_pro_api` is True):
     - Country selector (IT, FR, ES, IB)
     - `page_size` number input (default 100)
     - `max_pages` number input (default 1)
     - "Filtro Prodotti (Tags)" section:
       - Multiselect for Scalapay categories (from `bnpl_tags_by_category.json`, cached in `st.session_state`)
       - Dynamic multiselect for subcategories based on selected categories
       - Free-text search across all tags (uses `similarweb_all_tags.json` if available)
       - Tag count display with expander preview
     - Builds `api_filters = {"siteTags": selected_tags}` when tags are selected

  3. **`can_run` guard** — must be updated to also pass when `use_pro_api` is True:
     ```python
     can_run = (use_sample or use_pro_api or (use_reload and reload_file) or file_ib or file_fr or file_it) and w_sum <= 100
     ```

  4. **`run_pipeline()` function** — must handle Pro API branch. When `use_pro_api` is True, instead of iterating over uploaded files, call `ingest(pro_api_country, use_pro_api=True, page_size=page_size, max_pages=max_pages, filters=api_filters)` for the selected country. The result is appended to `all_dfs` the same way uploads are.

  5. **Logger registration** — add `"similarweb_cookies"` to the logger names list so cookie module logs appear in the Streamlit log viewer.

### Created

- **`tests/test_pro_api_tags.py`** — integration test:
  - Skips (`pytest.mark.skipif`) when `load_cookies()` returns empty or `is_expired()` returns True
  - Calls `fetch_leads_pro_api(["IT"], page_size=10, max_pages=1, filters={"siteTags": ["shoes"]})`
  - Asserts: DataFrame is not empty, has `domain` column, has `site_tags` column, at least one row's `site_tags` contains "shoes"
  - File location: `tests/test_pro_api_tags.py`

- **Unit test fixtures** (in same file or `tests/test_similarweb_client.py`):
  - `_build_search_payload` with `filters={"siteTags": ["shoes", "bags"]}` — assert `siteTags` appears in output payload under `filters.siteTags`
  - `_parse_search_rows` with sample row fixture — assert correct field mapping and territory resolution
  - `_merge_details` with sample detail fixture — assert values merged only when target is NaN/zero

## Pro API Endpoint Details

- **Base URL**: `https://pro.similarweb.com`
- **Search**: `POST /sales-api/advanced-search/websites`
  - Request body: `{countries: [int], page: int, pageSize: int, orderBy: "visits", asc: false, filters: {siteTags: [str], ...}}`
  - Response: `{rows: [{site, country, visits, industry, ...}], totalCount: int}`
- **Details**: `POST /sales-api/advanced-search/websites/details`
  - Request body: `{domains: [str], countries: [int]}`
  - Response: `{domain: {field: {country_code_str: value}}}`
- **Auth**: Cookie header from `cookie_meta.json` via `load_cookies()`

## Data Flow

```
Sidebar UI
  -> user selects categories / subcategories / free-text search
  -> collects tags into deduplicated list
  -> passes {"siteTags": [...]} as filters
  -> run_pipeline()
    -> if use_pro_api:
      -> ingest(country, use_pro_api=True, page_size, max_pages, filters)
        -> fetch_leads_pro_api(countries, filters=filters)
          -> _build_search_payload(filters={"siteTags": [...]})
          -> POST /sales-api/advanced-search/websites (paginated, rate-limited 2s)
          -> POST /sales-api/advanced-search/websites/details (single batch)
          -> _parse_search_rows + _merge_details
        -> DataFrame with 30+ columns
    -> else: existing CSV/upload/demo logic
  -> v4 pipeline continues (enrichment -> hubspot -> scoring)
```

## Error Handling

- **Cookies missing or expired**: warning in UI, Pro API returns empty DataFrame with user-facing message. Pipeline shows "No data to process."
- **Pro API returns HTTP error**: logged via `get_logger`, returns `None` from `_call_pro_api`. Pipeline continues gracefully.
- **`bnpl_tags_by_category.json` missing**: tag filter section hidden in sidebar. Pro API works without tag filters.
- **No tags selected**: Pro API called without `siteTags` in filters — returns all results (unfiltered).

## What Does NOT Change

- CSV upload mode, Reload mode, Demo mode — untouched
- `config.py`, `scoring.py`, `enrichment.py`, `hubspot_client.py` — not modified
- Downstream pipeline — receives a DataFrame regardless of source, processes identically
- `cookie_meta.json` — already exists with valid cookies, no changes needed
- Existing `ingest()` call sites — new parameters have backwards-compatible defaults

## Test Strategy

1. **Unit tests** (`tests/test_similarweb_client.py`): mock `_call_pro_api` to test `_parse_search_rows`, `_merge_details`, `_build_search_payload` with `siteTags` filter. Include sample API response fixtures.
2. **Integration test** (`tests/test_pro_api_tags.py`): real Pro API call with `siteTags: ["shoes"]`, verify non-empty DataFrame and tag presence in `site_tags` column. Auto-skip when cookies unavailable/expired.
3. **Manual verification**: run Streamlit app, select Pro API mode, pick a category, generate — confirm results appear and pipeline log shows `siteTags` in payload.
