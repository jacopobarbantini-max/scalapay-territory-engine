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
    assert payload["filters"]["siteTags"] == []  # Empty list = no filter


def test_resolve_country_codes():
    from similarweb_client import _resolve_country_codes

    assert _resolve_country_codes(["IT"]) == [380]
    assert _resolve_country_codes(["IB"]) == [724, 620]
    assert _resolve_country_codes(["FR", "IT"]) == [250, 380]
    # Deduplication: ES=724 already covered by IB
    codes = _resolve_country_codes(["IB", "ES"])
    assert codes == [724, 620]
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
