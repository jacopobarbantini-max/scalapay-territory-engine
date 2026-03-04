"""
enrichment.py - Checkout / Competitor / Ad-tech / Store Locator enrichment.

Five enrichment vectors:
  1. Multi-page scraping  -> homepage + checkout/cart/product pages
  2. SERP-based lookup    -> fallback competitor intel via search
  3. Ad pixel detection   -> Meta Pixel / Google Ads tags on homepage
  4. Store locator        -> detect physical store presence
  5. Meta tag / schema.org -> structured data extraction (JS-free turnaround)

All scraping uses multithreading (10 workers) for ~10x speedup.
Results cached 7 days to avoid duplicate requests.
"""

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8,fr;q=0.7,it;q=0.6",
}
TIMEOUT = 12
MAX_WORKERS = 10

# ── CACHE (7 days) ──────────────────────────────────────────
CACHE_DIR = Path("cache")
CACHE_TTL_DAYS = 7


def _cache_key(domain):
    return hashlib.md5(domain.encode()).hexdigest()


def _get_cached(domain):
    """Return cached result if <7 days old, else None."""
    try:
        CACHE_DIR.mkdir(exist_ok=True)
        path = CACHE_DIR / f"{_cache_key(domain)}.json"
        if path.exists():
            data = json.loads(path.read_text())
            age_days = (time.time() - data.get("_ts", 0)) / 86400
            if age_days < CACHE_TTL_DAYS:
                return data
    except Exception:
        pass
    return None


def _set_cache(domain, data):
    """Save result to cache."""
    try:
        CACHE_DIR.mkdir(exist_ok=True)
        data["_ts"] = time.time()
        path = CACHE_DIR / f"{_cache_key(domain)}.json"
        path.write_text(json.dumps(data, default=str))
    except Exception:
        pass


# ── FETCHING ────────────────────────────────────────────────
def _fetch_page(url):
    """Fetch a single URL, return HTML or None."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def _fetch_homepage(domain):
    for proto in ("https://", "http://"):
        html = _fetch_page(f"{proto}{domain}")
        if html:
            return html
    return None


# Checkout/cart path variants per market (ES, PT, FR, IT)
CHECKOUT_PATHS = [
    "/checkout", "/cart", "/basket", "/bag",
    "/cesta", "/carrito", "/panier", "/carrello",
    "/payment", "/pago", "/paiement", "/pagamento",
]

# Product listing paths (catches BNPL widgets on product pages)
PRODUCT_PATHS = [
    "/products", "/shop", "/tienda", "/boutique", "/negozio",
]


def _fetch_checkout_pages(domain):
    """Try common checkout/cart/product paths. Return combined HTML."""
    combined = []
    base = f"https://{domain}"
    for path in CHECKOUT_PATHS + PRODUCT_PATHS:
        html = _fetch_page(f"{base}{path}")
        if html:
            combined.append(html)
            if len(combined) >= 3:  # Cap at 3 extra pages to save time
                break
    return "\n".join(combined) if combined else None


# ── DETECTION (word-boundary regex) ─────────────────────────
def _build_regex(name):
    """Build word-boundary regex for a competitor/PSP name."""
    escaped = re.escape(name)
    return re.compile(r'\b' + escaped + r'\b', re.IGNORECASE)


# Pre-compile all regexes at module load
COMPETITOR_PATTERNS = {c: _build_regex(c) for c in KNOWN_COMPETITORS}
PSP_PATTERNS = {p: _build_regex(p) for p in KNOWN_PSPS}


def detect_from_html(html):
    """Detect BNPL competitors, PSPs, and ad pixels from HTML."""
    soup = BeautifulSoup(html, "html.parser")

    all_scripts = " ".join(
        (tag.get("src", "") + " " + (tag.string or ""))
        for tag in soup.find_all("script")
    ).lower()

    all_links = " ".join(
        tag.get("href", "") for tag in soup.find_all("a")
    ).lower()

    combined = html.lower() + " " + all_scripts + " " + all_links

    # Word-boundary matching (no more false positives from blog articles)
    found_bnpl = []
    for comp, pattern in COMPETITOR_PATTERNS.items():
        if pattern.search(combined):
            found_bnpl.append(comp.title())

    found_psp = []
    for psp, pattern in PSP_PATTERNS.items():
        if pattern.search(combined):
            found_psp.append(psp.title())

    # Ad pixel detection
    has_meta_pixel = any([
        "fbq(" in all_scripts,
        "facebook.com/tr" in combined,
        "connect.facebook.net" in combined,
        "fbevents.js" in combined,
    ])

    has_google_ads = any([
        "googleads" in combined,
        "google_conversion" in combined,
        ("gtag(" in all_scripts and "AW-" in all_scripts),
        "googleadservices.com" in combined,
        "googlesyndication.com" in combined,
    ])

    return found_bnpl, found_psp, has_meta_pixel, has_google_ads


# ── TURNAROUND #1: Schema.org / JSON-LD extraction ─────────
def _extract_structured_data(html):
    """
    Extract payment methods from JSON-LD and schema.org markup.
    Many sites declare accepted payment methods in structured data
    even if the BNPL widget loads via JS. This is FREE data.
    """
    found = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        # JSON-LD blocks
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "{}")
                text = json.dumps(data).lower()
                for comp, pattern in COMPETITOR_PATTERNS.items():
                    if pattern.search(text):
                        found.append(comp.title())
            except (json.JSONDecodeError, TypeError):
                continue

        # Meta tags (og:payment, payment-method, etc.)
        for meta in soup.find_all("meta"):
            content = (meta.get("content", "") + " " + meta.get("name", "")).lower()
            for comp, pattern in COMPETITOR_PATTERNS.items():
                if pattern.search(content):
                    found.append(comp.title())
    except Exception:
        pass
    return list(set(found))


# ── TURNAROUND #2: robots.txt / sitemap checkout discovery ──
def _find_checkout_from_sitemap(domain):
    """
    Parse robots.txt and sitemap.xml to discover actual checkout URLs.
    Many sites block /checkout in robots.txt (which confirms it exists)
    or list payment/checkout pages in their sitemap.
    """
    checkout_urls = []
    try:
        # Check robots.txt for Disallow: /checkout patterns
        robots = _fetch_page(f"https://{domain}/robots.txt")
        if robots:
            for line in robots.split("\n"):
                line_lower = line.lower().strip()
                if "disallow" in line_lower:
                    for kw in ["checkout", "cart", "payment", "pago", "panier"]:
                        if kw in line_lower:
                            path = line.split(":", 1)[-1].strip()
                            if path:
                                checkout_urls.append(f"https://{domain}{path}")
                                break

        # Quick sitemap check (just first 50KB)
        sitemap = _fetch_page(f"https://{domain}/sitemap.xml")
        if sitemap:
            for kw in ["checkout", "payment", "cart", "pago", "panier"]:
                pattern = re.compile(r'<loc>(https?://[^<]*' + kw + r'[^<]*)</loc>', re.I)
                for match in pattern.findall(sitemap[:50000]):
                    checkout_urls.append(match)
    except Exception:
        pass
    return checkout_urls[:3]  # Max 3


# ── TURNAROUND #3: Google Cache / Wayback Machine fallback ──
def _fetch_via_cache_fallback(domain):
    """
    Use Google's text-only cache as anti-bot bypass. Google already
    crawled the site — we read Google's cached version, no bot detection.
    Falls back to Wayback Machine if Google cache unavailable.
    """
    # Try Google cache (text version, lighter)
    google_cache_url = (
        f"https://webcache.googleusercontent.com/search?q=cache:{domain}+checkout&strip=1"
    )
    html = _fetch_page(google_cache_url)
    if html and len(html) > 500:
        return html

    # Try Wayback Machine CDX API (find most recent snapshot)
    try:
        cdx_url = (
            f"https://web.archive.org/cdx/search/cdx?"
            f"url={domain}&output=json&limit=1&fl=timestamp&filter=statuscode:200"
        )
        resp = requests.get(cdx_url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) > 1:  # first row is header
                ts = data[1][0]
                wayback_url = f"https://web.archive.org/web/{ts}/{domain}"
                return _fetch_page(wayback_url)
    except Exception:
        pass
    return None


# ── SERP FALLBACK ───────────────────────────────────────────
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


# ── SINGLE DOMAIN ENRICHMENT (full pipeline) ────────────────
def enrich_single_domain(domain):
    # Check cache first
    cached = _get_cached(domain)
    if cached:
        cached.pop("_ts", None)
        return cached

    result = {
        "competitors_bnpl": [],
        "psp_detected": [],
        "has_meta_pixel": False,
        "has_google_ads": False,
        "is_advertising_heavy": False,
    }

    all_bnpl = []
    all_psp = []
    meta_pixel = False
    google_ads = False

    # Phase 1: Homepage
    html = _fetch_homepage(domain)
    if html:
        bnpl, psp, meta, gads = detect_from_html(html)
        all_bnpl.extend(bnpl)
        all_psp.extend(psp)
        meta_pixel = meta_pixel or meta
        google_ads = google_ads or gads

        # Phase 1b: Extract structured data (JSON-LD, schema.org)
        structured = _extract_structured_data(html)
        all_bnpl.extend(structured)

    # Phase 2: Checkout/cart/product pages
    checkout_html = _fetch_checkout_pages(domain)
    if checkout_html:
        bnpl, psp, meta, gads = detect_from_html(checkout_html)
        all_bnpl.extend(bnpl)
        all_psp.extend(psp)
        meta_pixel = meta_pixel or meta
        google_ads = google_ads or gads

    # Phase 3: Sitemap/robots.txt checkout discovery
    if not all_bnpl:
        checkout_urls = _find_checkout_from_sitemap(domain)
        for url in checkout_urls:
            extra_html = _fetch_page(url)
            if extra_html:
                bnpl, psp, _, _ = detect_from_html(extra_html)
                all_bnpl.extend(bnpl)
                all_psp.extend(psp)

    # Phase 4: Google Cache / Wayback fallback (if homepage blocked)
    if not html and not checkout_html:
        cached_html = _fetch_via_cache_fallback(domain)
        if cached_html:
            bnpl, psp, meta, gads = detect_from_html(cached_html)
            all_bnpl.extend(bnpl)
            all_psp.extend(psp)
            meta_pixel = meta_pixel or meta
            google_ads = google_ads or gads

    # Phase 5: SERP fallback (last resort)
    if not all_bnpl and not html:
        serp_comps = _serp_competitor_check(domain)
        all_bnpl.extend(serp_comps)

    # Deduplicate
    result["competitors_bnpl"] = list(dict.fromkeys(all_bnpl))
    result["psp_detected"] = list(dict.fromkeys(all_psp))
    result["has_meta_pixel"] = meta_pixel
    result["has_google_ads"] = google_ads
    result["is_advertising_heavy"] = meta_pixel or google_ads

    # Cache result
    _set_cache(domain, result)

    return result


# ── MULTITHREADED ENRICHMENT PIPELINE ───────────────────────
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


# ── STORE LOCATOR DETECTION ─────────────────────────────────
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
