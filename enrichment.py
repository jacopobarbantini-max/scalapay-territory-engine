"""
enrichment.py - Checkout / Competitor / Ad-tech / Store Locator enrichment.

Four enrichment vectors:
  1. Checkout scraping   -> detect PSP & BNPL from homepage/checkout HTML
  2. SERP-based lookup   -> fallback competitor intel via search
  3. Ad pixel detection  -> Meta Pixel / Google Ads tags on homepage
  4. Store locator       -> detect physical store presence

All scraping uses multithreading (10 workers) for ~10x speedup.
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import KNOWN_COMPETITORS, KNOWN_PSPS
from utils import get_logger, normalise_domain

log = get_logger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 12
MAX_WORKERS = 10


# -- 1. HOMEPAGE SCRAPING
def _fetch_homepage(domain):
    for proto in ("https://", "http://"):
        try:
            resp = requests.get(
                f"{proto}{domain}", headers=HEADERS, timeout=TIMEOUT, allow_redirects=True
            )
            if resp.status_code == 200:
                return resp.text
        except Exception:
            continue
    return None


def detect_from_html(html):
    html_lower = html.lower()
    soup = BeautifulSoup(html, "html.parser")
    found_bnpl = []
    found_psp = []

    all_scripts = " ".join(
        (tag.get("src", "") + " " + (tag.string or ""))
        for tag in soup.find_all("script")
    ).lower()

    all_links = " ".join(
        tag.get("href", "") for tag in soup.find_all("a")
    ).lower()

    combined = html_lower + " " + all_scripts + " " + all_links

    for comp in KNOWN_COMPETITORS:
        if comp in combined:
            found_bnpl.append(comp.title())

    for psp in KNOWN_PSPS:
        if psp in combined:
            found_psp.append(psp.title())

    has_meta_pixel = any([
        "fbq(" in all_scripts,
        "facebook.com/tr" in combined,
        "connect.facebook.net" in combined,
        "fbevents.js" in combined,
    ])

    has_google_ads = any([
        "googleads" in combined,
        "google_conversion" in combined,
        "gtag(" in all_scripts and "AW-" in all_scripts,
        "googleadservices.com" in combined,
        "googlesyndication.com" in combined,
    ])

    return found_bnpl, found_psp, has_meta_pixel, has_google_ads


# -- 2. SERP FALLBACK
def _serp_competitor_check(domain):
    api_key = os.getenv("SERP_API_KEY", "").strip()
    if not api_key:
        return []
    query = f"{domain} payment BNPL klarna alma oney"
    url = "https://serpapi.com/search"
    params = {"q": query, "api_key": api_key, "num": 5}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        results_text = str(resp.json().get("organic_results", "")).lower()
        return [c.title() for c in KNOWN_COMPETITORS if c in results_text]
    except Exception as exc:
        log.error(f"SERP API error for {domain}: {exc}")
        return []


# -- 3. SINGLE DOMAIN ENRICHMENT
def enrich_single_domain(domain):
    result = {
        "competitors_bnpl": [],
        "psp_detected": [],
        "has_meta_pixel": False,
        "has_google_ads": False,
        "is_advertising_heavy": False,
    }
    html = _fetch_homepage(domain)
    if html:
        bnpl, psp, meta, gads = detect_from_html(html)
        result["competitors_bnpl"] = bnpl
        result["psp_detected"] = psp
        result["has_meta_pixel"] = meta
        result["has_google_ads"] = gads
        result["is_advertising_heavy"] = meta or gads
    else:
        serp_comps = _serp_competitor_check(domain)
        result["competitors_bnpl"] = serp_comps
    return result


# -- 4. MULTITHREADED ENRICHMENT PIPELINE
def enrich_dataframe(df, enable_scraping=True, progress_callback=None):
    """Add enrichment columns. Uses 10 threads for ~10x speedup."""
    df["competitors_bnpl"] = ""
    df["psp_detected"] = ""
    df["has_meta_pixel"] = False
    df["has_google_ads"] = False
    df["is_advertising_heavy"] = False

    if not enable_scraping:
        log.info("Scraping disabled.")
        return df

    total = len(df)
    counter = {"done": 0}
    lock = Lock()

    def process_row(idx, domain):
        try:
            info = enrich_single_domain(domain)
            return idx, info, None
        except Exception as exc:
            return idx, None, exc

    work_items = []
    for idx, row in df.iterrows():
        domain = normalise_domain(row.get("domain", ""))
        if domain:
            work_items.append((idx, domain))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_row, idx, domain): (idx, domain)
            for idx, domain in work_items
        }
        for future in as_completed(futures):
            idx, info, exc = future.result()
            if info:
                df.at[idx, "competitors_bnpl"] = ", ".join(info["competitors_bnpl"])
                df.at[idx, "psp_detected"] = ", ".join(info["psp_detected"])
                df.at[idx, "has_meta_pixel"] = info["has_meta_pixel"]
                df.at[idx, "has_google_ads"] = info["has_google_ads"]
                df.at[idx, "is_advertising_heavy"] = info["is_advertising_heavy"]
            elif exc:
                domain = futures[future][1]
                log.error(f"Enrichment failed for {domain}: {exc}")
            with lock:
                counter["done"] += 1
                if progress_callback:
                    progress_callback(counter["done"], total)

    return df


# -- 5. STORE LOCATOR DETECTION (lightweight, separate pass)
STORE_LOCATOR_SIGNALS = [
    "store locator", "store-locator", "find a store", "find-a-store",
    "our stores", "our-stores", "store finder", "store-finder",
    "nuestras tiendas", "encuentra tu tienda", "puntos de venta",
    "localizador de tiendas", "tiendas fisicas", "tiendas-fisicas",
    "nossas lojas", "encontre uma loja", "lojas fisicas",
    "nos boutiques", "nos magasins", "trouver un magasin",
    "trouver-un-magasin", "points de vente", "nos-magasins",
    "i nostri negozi", "trova negozio", "punti vendita",
    "/stores", "/tiendas", "/boutiques", "/magasins",
    "/lojas", "/negozi", "/storelocator", "/store-locator",
]


def detect_store_locator(domain):
    try:
        for proto in ("https://", "http://"):
            try:
                resp = requests.get(
                    f"{proto}{domain}",
                    headers=HEADERS,
                    timeout=8,
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    html_lower = resp.text.lower()
                    for signal in STORE_LOCATOR_SIGNALS:
                        if signal in html_lower:
                            return True
                    return False
            except Exception:
                continue
    except Exception:
        pass
    return False


def enrich_store_locator(df, progress_callback=None):
    """Detect physical stores. Uses 10 threads for ~10x speedup."""
    df["has_physical_stores"] = False
    df["channel_type"] = "Pure E-commerce"

    total = len(df)
    counter = {"done": 0}
    lock = Lock()

    def check_store(idx, domain):
        try:
            has_stores = detect_store_locator(domain)
            return idx, has_stores, None
        except Exception as exc:
            return idx, False, exc

    work_items = []
    for idx, row in df.iterrows():
        domain = normalise_domain(row.get("domain", ""))
        if domain:
            work_items.append((idx, domain))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(check_store, idx, domain): (idx, domain)
            for idx, domain in work_items
        }
        for future in as_completed(futures):
            idx, has_stores, exc = future.result()
            df.at[idx, "has_physical_stores"] = has_stores
            df.at[idx, "channel_type"] = "Omnichannel" if has_stores else "Pure E-commerce"
            if exc:
                domain = futures[future][1]
                log.error(f"Store locator failed for {domain}: {exc}")
            with lock:
                counter["done"] += 1
                if progress_callback:
                    progress_callback(counter["done"], total)

    return df
