"""
similarweb_client.py — Similarweb data ingestion.

Dual mode:
  • API mode  — calls Similarweb Digital Data API if SIMILARWEB_API_KEY is set.
  • CSV mode  — accepts an uploaded DataFrame (from Streamlit file_uploader).
"""

import os
from typing import Optional

import pandas as pd
import requests

from utils import get_logger, clean_similarweb_df

log = get_logger(__name__)

SW_BASE = "https://api.similarweb.com/v1"


def _get_api_key() -> Optional[str]:
    return os.getenv("SIMILARWEB_API_KEY", "").strip() or None


# ── API MODE ────────────────────────────────────────────────
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
) -> pd.DataFrame:
    """Main entry: try CSV first, then API."""
    if uploaded_file is not None:
        df = load_from_csv(uploaded_file)
        if not df.empty:
            df["country"] = country.upper()
            return df

    df = fetch_top_sites_api(country=country.lower())
    if not df.empty:
        df["country"] = country.upper()
    return df
