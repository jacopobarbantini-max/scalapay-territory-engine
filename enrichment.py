"""
enrichment.py - Checkout / Competitor / Ad-tech / Store Locator enrichment.

Enrichment pipeline (in order):
  1. Homepage HTML + JS source analysis (script src, data-attrs, CSS classes)
  2. Schema.org / JSON-LD structured data extraction
  3. Product page discovery & scraping (BNPL widgets on product pages)
  4. JS bundle analysis (download external .js files, search for BNPL strings)
  5. Checkout/cart page scraping
  6. Sitemap/robots.txt checkout URL discovery
  7. DNS CNAME check (checkout.merchant.com → klarna.com)
  8. Google Cache / Wayback Machine fallback
  9. SERP fallback (requires API key)

Anti-bot: Uses curl_cffi (Chrome TLS fingerprint) if available, falls back to requests.
All scraping uses multithreading (10 workers) for ~10x speedup.
Results cached 7 days to avoid duplicate requests.
"""

import hashlib
import json
import os
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from config import KNOWN_COMPETITORS, KNOWN_PSPS
from utils import get_logger, normalise_domain

log = get_logger(__name__)

# ── HTTP CLIENT: curl_cffi (Cloudflare bypass) or requests fallback ──
try:
    from curl_cffi import requests as cffi_requests
    _SESSION = cffi_requests.Session(impersonate="chrome120")
    USE_CFFI = True
    log.info("Using curl_cffi with Chrome TLS fingerprint (Cloudflare bypass enabled)")
except ImportError:
    import requests
    _SESSION = requests.Session()
    USE_CFFI = False
    log.info("curl_cffi not available, using standard requests (some sites may block)")

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
    """Fetch a single URL using curl_cffi (Cloudflare bypass) or requests."""
    try:
        if USE_CFFI:
            resp = _SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        else:
            resp = _SESSION.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
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


def _fetch_checkout_pages(domain):
    """Try common checkout/cart paths. Return combined HTML."""
    combined = []
    base = f"https://{domain}"
    for path in CHECKOUT_PATHS:
        html = _fetch_page(f"{base}{path}")
        if html:
            combined.append(html)
            if len(combined) >= 2:
                break
    return "\n".join(combined) if combined else None


# ── PRODUCT PAGE DISCOVERY ──────────────────────────────────
# URL patterns that indicate a product detail page
PRODUCT_URL_PATTERNS = re.compile(
    r'/(?:'
    r'product|producto|produit|prodotto|pd|item|artikel'  # /product/slug
    r')/[\w-]+'
    r'|/p/[\w-]+'  # short /p/slug
    r'|/shop/[\w-]+/[\w-]+'  # /shop/cat/product
    r'|/collections/[\w-]+/products/[\w-]+'  # Shopify
    r'|/tienda/[\w-]+/[\w-]+'  # ES
    r'|/boutique/[\w-]+/[\w-]+'  # FR
    , re.IGNORECASE,
)

# Sitemap URL patterns for products
SITEMAP_PRODUCT_PATTERNS = re.compile(
    r'<loc>(https?://[^<]*/(?:product|producto|produit|prodotto|p|pd|item|shop/[^<]+))</loc>',
    re.IGNORECASE,
)


def _find_product_urls_from_homepage(html, domain):
    """
    Extract real product page URLs from homepage links.
    Most e-commerce homepages link to featured/bestseller products.
    """
    product_urls = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        base = f"https://{domain}"
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            # Make absolute
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue
            # Must be same domain
            if domain not in href:
                continue
            # Match product URL pattern
            if PRODUCT_URL_PATTERNS.search(href):
                product_urls.append(href)
                if len(product_urls) >= 10:
                    break
    except Exception:
        pass
    return product_urls


def _find_product_urls_from_sitemap(domain):
    """
    Find real product URLs from sitemap.xml.
    Product pages are always in the sitemap — they need SEO indexing.
    """
    product_urls = []
    try:
        # Try main sitemap
        sitemap = _fetch_page(f"https://{domain}/sitemap.xml")
        if not sitemap:
            return []

        # Check for sitemap index (sitemap of sitemaps)
        product_sitemap_url = None
        if "<sitemapindex" in sitemap.lower():
            # Find product-specific sitemap
            for pattern in [r'<loc>(https?://[^<]*product[^<]*sitemap[^<]*)</loc>',
                           r'<loc>(https?://[^<]*sitemap[^<]*product[^<]*)</loc>',
                           r'<loc>(https?://[^<]*sitemap[^<]*)</loc>']:
                matches = re.findall(pattern, sitemap, re.I)
                for m in matches:
                    if any(kw in m.lower() for kw in ["product", "produit", "producto"]):
                        product_sitemap_url = m
                        break
                if product_sitemap_url:
                    break
            if product_sitemap_url:
                sitemap = _fetch_page(product_sitemap_url)
                if not sitemap:
                    return []

        # Extract product URLs (max first 100KB to save time)
        for match in SITEMAP_PRODUCT_PATTERNS.findall(sitemap[:100000]):
            product_urls.append(match)
            if len(product_urls) >= 10:
                break
    except Exception:
        pass
    return product_urls


def _scrape_product_pages(domain, homepage_html=None):
    """
    Find and scrape real product pages to detect BNPL widgets.
    BNPL providers show "Pay in 3 installments with Klarna" on the
    product page — this is a selling point, so it's in the static HTML.
    
    Strategy:
    1. Extract product URLs from homepage links (fastest)
    2. Fall back to sitemap product discovery
    3. Scrape up to 3 product pages
    """
    product_urls = []

    # Strategy 1: From homepage links
    if homepage_html:
        product_urls = _find_product_urls_from_homepage(homepage_html, domain)

    # Strategy 2: From sitemap (if homepage didn't have enough)
    if len(product_urls) < 2:
        sitemap_urls = _find_product_urls_from_sitemap(domain)
        product_urls.extend(sitemap_urls)

    # Deduplicate
    product_urls = list(dict.fromkeys(product_urls))

    if not product_urls:
        return None

    # Scrape up to 3 product pages
    combined = []
    for url in product_urls[:5]:  # Try 5, keep 3 successes
        html = _fetch_page(url)
        if html:
            combined.append(html)
            if len(combined) >= 3:
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
    """Detect BNPL competitors, PSPs, and ad pixels from HTML.
    
    Includes JS source analysis: even on SPA/React sites, the <script src>
    tags and data-attributes are in the static HTML because the browser
    needs them to know WHAT JavaScript to load.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Collect all script sources and inline JS
    all_scripts = " ".join(
        (tag.get("src", "") + " " + (tag.string or ""))
        for tag in soup.find_all("script")
    ).lower()

    all_links = " ".join(
        tag.get("href", "") for tag in soup.find_all("a")
    ).lower()

    # Collect ALL data-* attributes from all elements
    # BNPL widgets use data-klarna, data-alma, data-sequra, etc.
    all_data_attrs = []
    for tag in soup.find_all(True):
        for attr_name, attr_val in tag.attrs.items():
            if isinstance(attr_name, str):
                all_data_attrs.append(attr_name.lower())
            if isinstance(attr_val, str):
                all_data_attrs.append(attr_val.lower())
            elif isinstance(attr_val, list):
                all_data_attrs.extend(str(v).lower() for v in attr_val)
    data_attrs_text = " ".join(all_data_attrs)

    # Collect all CSS class names and IDs (widgets often have klarna-placement, alma-widget, etc.)
    all_classes_ids = []
    for tag in soup.find_all(True):
        classes = tag.get("class", [])
        if isinstance(classes, list):
            all_classes_ids.extend(c.lower() for c in classes)
        tag_id = tag.get("id", "")
        if tag_id:
            all_classes_ids.append(tag_id.lower())
    classes_ids_text = " ".join(all_classes_ids)

    combined = html.lower() + " " + all_scripts + " " + all_links + " " + data_attrs_text + " " + classes_ids_text

    # ── JS SOURCE ANALYSIS: Script URLs and SDK signatures ──
    # These are ALWAYS in static HTML — the browser must know what to load
    JS_BNPL_SIGNATURES = {
        # Klarna
        "klarna": [
            "js.klarna.com", "klarna.com/web-sdk", "klarna-payment",
            "klarna-placement", "data-klarna", "klarnaasync",
            "klarna.payments", "klarna-on-site-messaging",
            "klarna-osm", "data-purchase-amount",  # Klarna OSM widget
            "x-]klarna-rendering",
        ],
        # Alma
        "alma": [
            "cdn.alma.eu", "alma-widget", "data-alma",
            "alma.widgets", "alma-payment-plans", "alma.eu/js",
            "alma-badge", "alma-installments",
        ],
        # Sequra
        "sequra": [
            "sequra.js", "data-sequra", "sequraconfiguration",
            "sequra.es", "cdn.sequra", "sequra-widget",
            "sequra-promotion", "sequra-payment",
        ],
        # Oney
        "oney": [
            "oney.js", "data-oney", "oney-widget",
            "cdn.oney", "oney.io", "oney-simulation",
            "oney-facilypay",
        ],
        # Afterpay / Clearpay
        "afterpay": [
            "afterpay.js", "data-afterpay", "afterpay-placement",
            "js.afterpay.com", "static.afterpay.com",
            "afterpay-widget", "clearpay-placement",
            "js.clearpay.co", "static.clearpay.co",
        ],
        # Cofidis
        "cofidis": [
            "cofidis-widget", "data-cofidis", "cdn.cofidis",
            "simulador-cofidis", "cofidis.es", "cofidis.fr",
        ],
        # Aplazame
        "aplazame": [
            "aplazame.com/js", "data-aplazame", "aplazame-widget",
            "cdn.aplazame.com",
        ],
        # Zip (Quadpay)
        "zip": [
            "widgets.quadpay.com", "quadpay-widget",
            "zip.co/widget", "data-quadpay",
        ],
        # PayPal Pay Later
        "pay later": [
            "paypal.com/sdk/js.*enable-funding=paylater",
            "data-sdk-integration-source.*paylater",
            "paypal-paylater", "pp-paylater",
        ],
        # Pledg
        "pledg": [
            "pledg.co", "data-pledg", "cdn.pledg",
        ],
        # Pagantis -> now Sequra
        "pagantis": [
            "pagantis.com", "cdn.pagantis",
        ],
        # Cetelem
        "cetelem": [
            "cetelem-widget", "simulador-cetelem",
            "cetelem.es", "cetelem.fr",
        ],
        # Soisy
        "soisy": [
            "soisy.it", "data-soisy", "cdn.soisy",
        ],
        # Pagolight
        "pagolight": [
            "pagolight-widget", "data-pagolight", "pagolight.it",
        ],
    }

    found_bnpl = []

    # Standard word-boundary matching on combined text
    for comp, pattern in COMPETITOR_PATTERNS.items():
        if pattern.search(combined):
            found_bnpl.append(comp.title())

    # JS SDK signature matching (catches JS-loaded widgets)
    for comp_name, signatures in JS_BNPL_SIGNATURES.items():
        if comp_name.title() in found_bnpl:
            continue  # Already found via regex
        for sig in signatures:
            if sig in combined:
                found_bnpl.append(comp_name.title())
                break

    # PSP detection (same word-boundary approach)
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
        resp = _SESSION.get(cdx_url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) > 1:  # first row is header
                ts = data[1][0]
                wayback_url = f"https://web.archive.org/web/{ts}/{domain}"
                return _fetch_page(wayback_url)
    except Exception:
        pass
    return None


# ── TURNAROUND #4: JS Bundle Analysis ───────────────────────
def _analyze_js_bundles(html, domain):
    """
    Download external .js files referenced in script tags and search
    for BNPL strings inside them. The browser must download these files
    to render the page, so they're never behind anti-bot protection.
    
    Cap at 5 JS files, max 200KB each to stay fast.
    """
    found = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        js_urls = []
        base = f"https://{domain}"
        for tag in soup.find_all("script", src=True):
            src = tag["src"].strip()
            # Make absolute
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = base + src
            elif not src.startswith("http"):
                src = base + "/" + src
            # Only same domain or CDN (skip third-party analytics etc.)
            if domain in src or "cdn" in src or "assets" in src or "static" in src:
                js_urls.append(src)
            if len(js_urls) >= 5:
                break

        for js_url in js_urls:
            try:
                if USE_CFFI:
                    resp = _SESSION.get(js_url, timeout=8, allow_redirects=True)
                else:
                    resp = _SESSION.get(js_url, headers=HEADERS, timeout=8, allow_redirects=True)
                if resp.status_code == 200:
                    # Only read first 200KB
                    js_text = resp.text[:200000].lower()
                    for comp, pattern in COMPETITOR_PATTERNS.items():
                        if pattern.search(js_text) and comp.title() not in found:
                            found.append(comp.title())
            except Exception:
                continue
    except Exception:
        pass
    return found


# ── TURNAROUND #5: DNS CNAME Check ─────────────────────────
# Known BNPL checkout subdomains and CNAME targets
BNPL_DNS_TARGETS = {
    "klarna": ["klarna.com", "klarna.net", "klarnacdn.net"],
    "alma": ["alma.eu", "getalma.eu"],
    "sequra": ["sequra.com", "sequra.es"],
    "afterpay": ["afterpay.com", "clearpay.co.uk", "clearpay.com"],
    "oney": ["oney.com", "oney.fr", "oney.es"],
    "cofidis": ["cofidis.com", "cofidis.fr", "cofidis.es"],
}

CHECKOUT_SUBDOMAINS = ["checkout", "pay", "payment", "pago", "secure"]


def _dns_cname_check(domain):
    """
    Check if checkout.merchant.com (or pay.merchant.com etc.) has a
    CNAME pointing to a BNPL provider. This is free, instant, and
    impossible to block because it's DNS, not HTTP.
    """
    found = []
    for sub in CHECKOUT_SUBDOMAINS:
        fqdn = f"{sub}.{domain}"
        try:
            cname = socket.getfqdn(fqdn)
            cname_lower = cname.lower()
            for comp, targets in BNPL_DNS_TARGETS.items():
                for target in targets:
                    if target in cname_lower:
                        found.append(comp.title())
                        break
        except Exception:
            continue

        # Also try direct A record resolution and reverse lookup
        try:
            addrs = socket.getaddrinfo(fqdn, 443, proto=socket.IPPROTO_TCP)
            if addrs:
                # The subdomain exists — even if we can't match CNAME,
                # this is useful info (merchant has a checkout subdomain)
                pass
        except (socket.gaierror, OSError):
            continue

    return list(set(found))


# ── SERP FALLBACK ───────────────────────────────────────────
def _serp_competitor_check(domain):
    api_key = os.getenv("SERP_API_KEY", "").strip()
    if not api_key:
        return []
    query = f"{domain} payment BNPL klarna alma oney"
    url = "https://serpapi.com/search"
    params = {"q": query, "api_key": api_key, "num": 5}
    try:
        if USE_CFFI:
            resp = _SESSION.get(url, params=params, timeout=15)
        else:
            resp = _SESSION.get(url, params=params, headers=HEADERS, timeout=15)
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

    # Phase 1: Homepage + JS source analysis
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

    # Phase 2: Product pages (BNPL widgets on product pages)
    if not all_bnpl:
        product_html = _scrape_product_pages(domain, homepage_html=html)
        if product_html:
            bnpl, psp, meta, gads = detect_from_html(product_html)
            all_bnpl.extend(bnpl)
            all_psp.extend(psp)
            meta_pixel = meta_pixel or meta
            google_ads = google_ads or gads

    # Phase 3: JS bundle analysis (download .js files, search BNPL strings)
    if not all_bnpl and html:
        js_found = _analyze_js_bundles(html, domain)
        all_bnpl.extend(js_found)

    # Phase 4: Checkout/cart pages
    if not all_bnpl:
        checkout_html = _fetch_checkout_pages(domain)
        if checkout_html:
            bnpl, psp, meta, gads = detect_from_html(checkout_html)
            all_bnpl.extend(bnpl)
            all_psp.extend(psp)
            meta_pixel = meta_pixel or meta
            google_ads = google_ads or gads

    # Phase 5: Sitemap/robots.txt checkout discovery
    if not all_bnpl:
        checkout_urls = _find_checkout_from_sitemap(domain)
        for url in checkout_urls:
            extra_html = _fetch_page(url)
            if extra_html:
                bnpl, psp, _, _ = detect_from_html(extra_html)
                all_bnpl.extend(bnpl)
                all_psp.extend(psp)

    # Phase 6: DNS CNAME check (checkout.merchant.com → klarna.com)
    if not all_bnpl:
        dns_found = _dns_cname_check(domain)
        all_bnpl.extend(dns_found)

    # Phase 7: Google Cache / Wayback fallback (if homepage blocked)
    if not html and not all_bnpl:
        cached_html = _fetch_via_cache_fallback(domain)
        if cached_html:
            bnpl, psp, meta, gads = detect_from_html(cached_html)
            all_bnpl.extend(bnpl)
            all_psp.extend(psp)
            meta_pixel = meta_pixel or meta
            google_ads = google_ads or gads

    # Phase 8: SERP fallback (last resort)
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
                if USE_CFFI:
                    resp = _SESSION.get(
                        f"{proto}{domain}", timeout=8, allow_redirects=True,
                    )
                else:
                    resp = _SESSION.get(
                        f"{proto}{domain}", headers=HEADERS, timeout=8, allow_redirects=True,
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
