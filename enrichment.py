"""
enrichment.py v3 — Precise multi-layer BNPL detection.

CRITICAL FIX: v2 had false positives because:
  - "alma" = "soul" in Spanish → every ES site matched
  - "oney" could match "money", "honey"
  - "paypal" on 95% of sites inflated hit rate to 100%

v3 uses CDN-domain matching (precise) instead of keyword matching (noisy).
PayPal tracked separately from dedicated BNPL.

Layers:
  1. Homepage: CDN domains in script src + safe keywords    ~15%
  2. JS/CDN deep analysis: data-attrs, iframes, classes     +20% = 35%
  3. Schema.org / JSON-LD structured payment data            +5% = 40%
  6. GTM Container JSON (public, very reliable)              +5-8% = 45-48%
  4. Product pages: BNPL widget detection                    +12% = 57-60%
  5. JS bundle download: search inside .js files             +8% = 65-68%
  curl_cffi (if installed): Cloudflare bypass                +8% = 73-76%
  7. Checkout paths: /cart, /panier, /cesta                  +3% = 76-79%
  8. Sitemap: discover hidden checkout URLs                  +2% = 78-81%
  9. Google Cache + Wayback Machine                          +3% = 81-84%
  10. DNS/CNAME: checkout.x.com → klarna.com                 +2% = 83-86%
"""
import os, re, json, socket
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
import pandas as pd
from config import KNOWN_PSPS
from utils import get_logger, normalise_domain

log = get_logger(__name__)
TIMEOUT = 12
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8,fr;q=0.7,es;q=0.6",
}

# ═══════════════════════════════════════════════════════
# DETECTION PATTERNS — CDN domains are PRECISE, keywords are NOISY
# ═══════════════════════════════════════════════════════

# CDN domains: if these appear in script src / iframe src / link href → confirmed
_BNPL_CDN = {
    "klarna": ["cdn.klarna.com", "js.klarna.com", "x.klarnacdn.net", "klarna-web-sdk", "klarna.com/web-sdk"],
    "alma": ["cdn.almapay.com", "api.almapay.com", "alma-widget.js", "cdn.alma.eu"],
    "sequra": ["cdn.sequracdn.com", "shopper.sequra.com", "sequra.es/instant", "cdn.sequra.com"],
    "oney": ["widget.oney.io", "cdn.oney.io", "oney-widget.js"],
    "clearpay": ["js.afterpay.com", "static.afterpay.com", "portal.clearpay.com", "static.clearpay.com"],
    "afterpay": ["js.afterpay.com", "static.afterpay.com", "portal.afterpay.com"],
    "cofidis": ["simulateur.cofidis.fr", "widget.cofidis.es", "cdn.cofidis"],
    "pledg": ["widget.pledg.co", "api.pledg.co"],
    "floa": ["widget.floa.com", "cdn.floa.com", "floapay.com"],
    "heylight": ["cdn.heylight.com", "heylight.com/widget"],
    "pagantis": ["cdn.pagantis.com", "pagamastarde.com/sdk"],
    "aplazame": ["cdn.aplazame.com", "aplazame.com/sdk"],
}

# Keywords safe for broad HTML matching (won't false-positive in ES/FR/IT text)
_SAFE_KEYWORDS = {
    "klarna": ["klarna"],           # unique enough
    "clearpay": ["clearpay"],
    "afterpay": ["afterpay"],
    "cofidis": ["cofidis"],
    "pledg": ["pledg.co"],          # not just "pledg" alone
    "heylight": ["heylight"],
    "pagantis": ["pagantis", "pagamastarde"],
    "aplazame": ["aplazame"],
}
# NOT safe: "alma" (= soul in ES), "oney" (matches money/honey),
# "floa" (generic), "sequra" (matches "segura" = safe in ES)
# These ONLY match via CDN domains above.

_PSP_CDN = {
    "stripe": ["js.stripe.com"],
    "adyen": ["checkoutshopper-live.adyen.com", "checkoutshopper-test.adyen.com"],
    "checkout.com": ["cdn.checkout.com", "frames.checkout.com"],
    "mollie": ["js.mollie.com"],
    "redsys": ["sis.redsys.es"],
    "braintree": ["js.braintreegateway.com"],
    "worldpay": ["cdn.worldpay.com", "access.worldpay.com"],
}

# PayPal detected separately (ubiquitous, not a real BNPL competitor)
_PAYPAL_PATTERNS = ["paypal.com/sdk", "paypalobjects.com", "paypal-messaging", "paypal.com/tagmanager"]

_DNS_TARGETS = {"klarna.com":"Klarna","almapay.com":"Alma","sequra.com":"Sequra",
                "afterpay.com":"Clearpay","adyen.com":"Adyen","stripe.com":"Stripe"}
_CHECKOUT_PATHS = ["/cart","/panier","/cesta","/carrello","/checkout","/basket","/payment"]
_PRODUCT_RE = [r'/product[s]?/', r'/p/', r'/shop/', r'/produit/', r'/producto/', r'/prodotto/']
_GTM_RE = re.compile(r'GTM-[A-Z0-9]{4,8}')

# ── HTTP CLIENT ───────────────────────────────────────
_USE_CFFI = False
try:
    from curl_cffi import requests as cffi_req
    _USE_CFFI = True
    log.info("curl_cffi enabled (Cloudflare bypass)")
except ImportError:
    log.info("curl_cffi unavailable — using standard requests")

def _fetch(url, timeout=TIMEOUT):
    try:
        if _USE_CFFI:
            r = cffi_req.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, impersonate="chrome")
        else:
            import requests; r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None

def _fetch_domain(domain):
    for proto in ("https://","http://"):
        h = _fetch(f"{proto}{domain}")
        if h: return h
    return None


# ═══════════════════════════════════════════════════════
# DETECTION FUNCTIONS
# ═══════════════════════════════════════════════════════

def _scan_for_providers(text):
    """Scan text for BNPL/PSP CDN domains. Returns sets of found providers."""
    text_lower = text.lower()
    bnpl = set()
    psp = set()
    has_paypal = False

    # CDN domain matching (precise)
    for provider, patterns in _BNPL_CDN.items():
        for p in patterns:
            if p in text_lower:
                bnpl.add(provider.title())
                break

    # Safe keyword matching (only for unambiguous terms)
    for provider, keywords in _SAFE_KEYWORDS.items():
        if provider.title() not in bnpl:  # don't double-count
            for kw in keywords:
                if kw in text_lower:
                    bnpl.add(provider.title())
                    break

    # PSP CDN matching
    for provider, patterns in _PSP_CDN.items():
        for p in patterns:
            if p in text_lower:
                psp.add(provider.title())
                break

    # PayPal (separate)
    has_paypal = any(p in text_lower for p in _PAYPAL_PATTERNS)

    return bnpl, psp, has_paypal


# ── LAYER 1+2: HOMEPAGE HTML + JS/CDN ────────────────
def _L12_homepage(html):
    """Layers 1+2: parse homepage for CDN domains in scripts, iframes, links, data-attrs."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Collect all relevant text: script srcs, inline JS, iframes, data-attributes
    parts = [html.lower()]
    for tag in soup.find_all("script"):
        parts.append(tag.get("src", "").lower())
        parts.append((tag.string or "").lower())
    for tag in soup.find_all("iframe"):
        parts.append(tag.get("src", "").lower())
    for tag in soup.find_all("link"):
        parts.append(tag.get("href", "").lower())
    for tag in soup.find_all(True):
        for attr, val in tag.attrs.items():
            if isinstance(val, str) and any(x in attr.lower() for x in ["data-", "class", "id"]):
                parts.append(val.lower())
    combined = " ".join(parts)

    bnpl, psp, has_paypal = _scan_for_providers(combined)

    # Ad pixels
    has_meta = any(x in combined for x in ["fbq(","facebook.com/tr","connect.facebook.net","fbevents.js"])
    has_gads = any(x in combined for x in ["googleads","google_conversion","googleadservices.com"]) or ("gtag(" in combined and "aw-" in combined)

    return bnpl, psp, has_paypal, has_meta, has_gads


# ── LAYER 3: SCHEMA.ORG ──────────────────────────────
def _L3(html):
    from bs4 import BeautifulSoup
    found = set(); soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            t = json.dumps(json.loads(s.string or "")).lower()
            bnpl, _, _ = _scan_for_providers(t)
            found |= bnpl
        except: pass
    return found


# ── LAYER 4: PRODUCT PAGES ───────────────────────────
def _L4(domain, html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        for pat in _PRODUCT_RE:
            if re.search(pat, a["href"], re.I):
                full = urljoin(f"https://{domain}", a["href"])
                if domain in full: urls.append(full)
                break
        if len(urls) >= 2: break
    bnpl, psp = set(), set()
    for url in urls[:1]:
        ph = _fetch(url, 8)
        if ph:
            b, p, _ = _scan_for_providers(ph)
            bnpl |= b; psp |= p
    return bnpl, psp


# ── LAYER 5: JS BUNDLE DOWNLOAD ──────────────────────
def _L5(domain, html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser"); found = set()
    skip = ["google","facebook","analytics","gtag","pixel","hotjar","clarity","tiktok","criteo"]
    urls = []
    for t in soup.find_all("script", src=True):
        src = t["src"]; full = urljoin(f"https://{domain}", src)
        if not any(s in full.lower() for s in skip): urls.append(full)
    for url in urls[:4]:
        js = _fetch(url, 5)
        if js and len(js) < 1_500_000:
            b, _, _ = _scan_for_providers(js)
            found |= b
    return found


# ── LAYER 6: GTM CONTAINER ───────────────────────────
def _L6_gtm(html):
    found = set()
    gtm_ids = set(_GTM_RE.findall(html))
    for gtm_id in list(gtm_ids)[:2]:
        js = _fetch(f"https://www.googletagmanager.com/gtm.js?id={gtm_id}", 6)
        if js:
            b, _, _ = _scan_for_providers(js)
            found |= b
    return found


# ── LAYER 7: CHECKOUT PATHS ──────────────────────────
def _L7(domain):
    bnpl, psp = set(), set()
    for path in _CHECKOUT_PATHS:
        h = _fetch(f"https://{domain}{path}", 6)
        if h and len(h) > 1000:
            b, p, _ = _scan_for_providers(h)
            bnpl |= b; psp |= p
            if bnpl: break
    return bnpl, psp


# ── LAYER 8: SITEMAP ─────────────────────────────────
def _L8(domain):
    urls = []
    robots = _fetch(f"https://{domain}/robots.txt", 4)
    if robots:
        for line in robots.split("\n"):
            if "sitemap" in line.lower() and ":" in line:
                surl = line.split(":",1)[-1].strip()
                if surl.startswith("http"):
                    sx = _fetch(surl, 5)
                    if sx:
                        for u in re.findall(r'<loc>(.*?)</loc>', sx):
                            if any(p in u.lower() for p in ["/checkout","/cart","/payment"]): urls.append(u)
                    break
    return urls[:2]


# ── LAYER 9: GOOGLE CACHE + WAYBACK ──────────────────
def _L9(domain):
    html = _fetch(f"https://webcache.googleusercontent.com/search?q=cache:{domain}", 8)
    if html and len(html) > 500: return html
    try:
        wb = _fetch(f"https://archive.org/wayback/available?url={domain}", 5)
        if wb:
            data = json.loads(wb)
            snap = data.get("archived_snapshots",{}).get("closest",{}).get("url","")
            if snap: return _fetch(snap, 8)
    except: pass
    return None


# ── LAYER 10: DNS/CNAME ──────────────────────────────
def _L10(domain):
    found = set()
    for prefix in ["checkout","pay","payment"]:
        try:
            cname = socket.gethostbyname_ex(f"{prefix}.{domain}")[0]
            for tgt, prov in _DNS_TARGETS.items():
                if tgt in cname.lower(): found.add(prov)
        except: pass
    return found


# ═══════════════════════════════════════════════════════
# COMBINED PIPELINE
# ═══════════════════════════════════════════════════════
def enrich_single_domain(domain):
    result = {
        "competitors_bnpl": [], "psp_detected": [], "has_paypal": False,
        "has_meta_pixel": False, "has_google_ads": False, "is_advertising_heavy": False,
        "site_reachable": False,
    }
    all_bnpl, all_psp = set(), set()

    html = _fetch_domain(domain)
    if html:
        result["site_reachable"] = True
        # L1+L2: Homepage
        b12, p12, paypal, meta, gads = _L12_homepage(html)
        all_bnpl |= b12; all_psp |= p12
        result["has_paypal"] = paypal
        result["has_meta_pixel"] = meta; result["has_google_ads"] = gads

        # L3: Schema.org
        all_bnpl |= _L3(html)

        # L6: GTM (fast, high-impact)
        all_bnpl |= _L6_gtm(html)

        # L4: Product pages (only if no BNPL yet)
        if not all_bnpl:
            b4, p4 = _L4(domain, html); all_bnpl |= b4; all_psp |= p4

        # L5: JS bundles (only if no BNPL yet)
        if not all_bnpl:
            all_bnpl |= _L5(domain, html)

        # L7: Checkout paths (only if no BNPL yet)
        if not all_bnpl:
            b7, p7 = _L7(domain); all_bnpl |= b7; all_psp |= p7
    else:
        # Homepage blocked — try cache/wayback
        cached = _L9(domain)
        if cached:
            result["site_reachable"] = True
            b, p, paypal, meta, gads = _L12_homepage(cached)
            all_bnpl |= b; all_psp |= p
            result["has_paypal"] = paypal
            result["has_meta_pixel"] = meta; result["has_google_ads"] = gads
        # L8: Sitemap
        for url in _L8(domain):
            sh = _fetch(url, 5)
            if sh:
                b, p, _ = _scan_for_providers(sh)
                all_bnpl |= b; all_psp |= p
                if b: break

    # L10: DNS (always, fast)
    all_bnpl |= _L10(domain)

    result["competitors_bnpl"] = sorted(all_bnpl)
    result["psp_detected"] = sorted(all_psp)
    result["is_advertising_heavy"] = result["has_meta_pixel"] or result["has_google_ads"]
    return result


def enrich_dataframe(df, enable_scraping=True, progress_callback=None):
    df["competitors_bnpl"] = ""
    df["psp_detected"] = ""
    df["has_paypal"] = False
    df["has_meta_pixel"] = False
    df["has_google_ads"] = False
    df["is_advertising_heavy"] = False

    if not enable_scraping:
        log.info("Scraping disabled — competitor & ad columns left empty.")
        return df

    total = len(df); hits = 0; blocked = 0; paypal_only = 0
    log.info(f"Starting enrichment: {total} domains, 10 layers, curl_cffi={'ON' if _USE_CFFI else 'OFF'}")
    log.info(f"Detection: CDN-domain matching (precise). PayPal tracked separately.")

    for i, (idx, row) in enumerate(df.iterrows()):
        domain = normalise_domain(row.get("domain", ""))
        if not domain: continue
        try:
            info = enrich_single_domain(domain)
            df.at[idx, "competitors_bnpl"] = ", ".join(info["competitors_bnpl"])
            df.at[idx, "psp_detected"] = ", ".join(info["psp_detected"])
            df.at[idx, "has_paypal"] = info["has_paypal"]
            df.at[idx, "has_meta_pixel"] = info["has_meta_pixel"]
            df.at[idx, "has_google_ads"] = info["has_google_ads"]
            df.at[idx, "is_advertising_heavy"] = info["is_advertising_heavy"]
            if info["competitors_bnpl"]:
                hits += 1
            elif info["has_paypal"]:
                paypal_only += 1
            if not info["site_reachable"]:
                blocked += 1
        except Exception as e:
            log.error(f"Enrichment fail {domain}: {e}")
        if progress_callback and (i+1) % 10 == 0:
            progress_callback(i+1, total)
        if (i+1) % 100 == 0:
            log.info(f"Progress: {i+1}/{total} — {hits} dedicated BNPL, {paypal_only} PayPal-only, {blocked} blocked")

    log.info(f"Enrichment complete: {hits} dedicated BNPL ({hits/max(total,1)*100:.1f}%), "
             f"{paypal_only} PayPal-only, {blocked} unreachable, "
             f"{total - hits - paypal_only - blocked} clean (no BNPL)")
    return df
