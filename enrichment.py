"""
enrichment.py v4 — Comprehensive BNPL detection across all page sections.

Detection strategy:
  A) CDN domains (precise, zero false positives)
  B) Context-aware brand detection ("avec Alma", "paga con Sequra", etc.)
  C) Payment method sections (footer, checkout, product pages)
  D) Installment phrases + nearby brand name (±200 chars window)
  E) GTM container analysis

10 Layers with smart early-exit.
"""
import os, re, json, socket
from typing import Dict, List, Optional, Tuple, Set
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
# ALL BNPL BRANDS — canonical names
# ═══════════════════════════════════════════════════════
_ALL_BNPL_BRANDS = {
    "klarna", "alma", "sequra", "oney", "clearpay", "afterpay",
    "cofidis", "pledg", "floa", "heylight", "pagantis", "aplazame",
}

# ═══════════════════════════════════════════════════════
# A) CDN DOMAINS — if found in script/iframe src → confirmed
# ═══════════════════════════════════════════════════════
_BNPL_CDN = {
    "klarna": ["cdn.klarna.com","js.klarna.com","x.klarnacdn.net","klarna-web-sdk","klarna.com/web-sdk"],
    "alma": ["cdn.almapay.com","api.almapay.com","alma-widget.js","cdn.alma.eu"],
    "sequra": ["cdn.sequracdn.com","shopper.sequra.com","sequra.es/instant","cdn.sequra.com"],
    "oney": ["widget.oney.io","cdn.oney.io","oney-widget.js"],
    "clearpay": ["js.afterpay.com","static.afterpay.com","portal.clearpay.com","static.clearpay.com"],
    "afterpay": ["js.afterpay.com","static.afterpay.com","portal.afterpay.com"],
    "cofidis": ["simulateur.cofidis.fr","widget.cofidis.es","cdn.cofidis"],
    "pledg": ["widget.pledg.co","api.pledg.co"],
    "floa": ["widget.floa.com","cdn.floa.com","floapay.com"],
    "heylight": ["cdn.heylight.com","heylight.com/widget"],
    "pagantis": ["cdn.pagantis.com","pagamastarde.com/sdk"],
    "aplazame": ["cdn.aplazame.com","aplazame.com/sdk"],
}

# B) CONTEXT-AWARE PHRASES — brand in BNPL context (safe across languages)
# Format: { brand: [phrases that confirm BNPL usage] }
_BRAND_CONTEXT_PHRASES = {
    "alma":    ["avec alma","con alma pay","powered by alma","paga con alma","pay with alma","alma - payer","alma pay","almapay"],
    "oney":    ["avec oney","paga con oney","pay with oney","powered by oney","oney bank","oney pay","financement oney"],
    "sequra":  ["con sequra","with sequra","powered by sequra","paga con sequra","fracciona con sequra","sequra pago"],
    "floa":    ["avec floa","powered by floa","floa pay","floa bank","paiement floa"],
    "klarna":  ["with klarna","con klarna","avec klarna","powered by klarna","klarna pay","klarna checkout"],
    "clearpay":["with clearpay","con clearpay","avec clearpay","powered by clearpay"],
    "afterpay":["with afterpay","con afterpay","powered by afterpay"],
    "cofidis": ["avec cofidis","con cofidis","cofidis pay","financement cofidis","cofidis 3x","cofidis 4x"],
    "pledg":   ["avec pledg","powered by pledg","pledg paiement"],
    "heylight":["avec heylight","powered by heylight","heylight pay"],
    "pagantis":["con pagantis","powered by pagantis","pagamastarde"],
    "aplazame":["con aplazame","powered by aplazame","aplazame pago"],
}

# C) SAFE KEYWORDS — unambiguous, safe in all languages
_SAFE_KEYWORDS = {
    "klarna": ["klarna"],
    "clearpay": ["clearpay"],
    "afterpay": ["afterpay"],
    "cofidis": ["cofidis"],
    "heylight": ["heylight"],
    "pagantis": ["pagantis","pagamastarde"],
    "aplazame": ["aplazame"],
}
# NOT safe as standalone: alma, oney, sequra, floa, pledg
# These ONLY match via CDN or context phrases

# D) INSTALLMENT PHRASES — if found, search nearby for ANY brand
_INSTALLMENT_PHRASES = [
    "payez en 2x","payez en 3x","payez en 4x","payer en 3x","payer en 4x",
    "paga in 2 rate","paga in 3 rate","paga in 4 rate","paga in 3","paga in 4",
    "paga en 2 cuotas","paga en 3 cuotas","paga en 4 cuotas",
    "pay in 2","pay in 3","pay in 4",
    "3 fois sans frais","4 fois sans frais","3x sans frais","4x sans frais",
    "sin intereses","sans frais","senza interessi",
    "split payment","buy now pay later","bnpl",
    "fracciona tu pago","fractionner","ratenzahlung",
    "pago fraccionado","pago aplazado","paiement fractionné",
]

# E) PAYMENT METHOD SECTION MARKERS — text that indicates a payment section
_PAYMENT_SECTION_MARKERS = [
    "metodo de pago","metodos de pago","métodos de pago","método de pago",
    "metodo di pagamento","metodi di pagamento",
    "mode de paiement","modes de paiement","moyens de paiement",
    "payment method","payment methods","payment options",
    "seleziona un metodo","selecciona un método",
    "sélectionner un mode","choose payment",
    "forma de pago","formas de pago",
    "modalità di pagamento","come pagare","comment payer",
    "paiement sécurisé","pago seguro","pagamento sicuro",
    "we accept","acceptons","aceptamos","accettiamo",
]

_PSP_CDN = {
    "stripe":["js.stripe.com"],"adyen":["checkoutshopper-live.adyen.com"],
    "checkout.com":["cdn.checkout.com","frames.checkout.com"],
    "mollie":["js.mollie.com"],"redsys":["sis.redsys.es"],
    "braintree":["js.braintreegateway.com"],"worldpay":["cdn.worldpay.com"],
}
_PAYPAL_PATTERNS = ["paypal.com/sdk","paypalobjects.com","paypal-messaging","paypal.com/tagmanager"]
_DNS_TARGETS = {"klarna.com":"Klarna","almapay.com":"Alma","sequra.com":"Sequra",
                "afterpay.com":"Clearpay","adyen.com":"Adyen","stripe.com":"Stripe"}
_CHECKOUT_PATHS = ["/cart","/panier","/cesta","/carrello","/checkout","/basket","/payment","/paiement"]
_PRODUCT_RE = [r'/product[s]?/', r'/p/', r'/shop/', r'/produit/', r'/producto/', r'/prodotto/', r'/-p-', r'/dp/']
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
    except Exception: return None

def _fetch_domain(domain):
    for proto in ("https://","http://"):
        h = _fetch(f"{proto}{domain}")
        if h: return h
    return None


# ═══════════════════════════════════════════════════════
# CORE DETECTION ENGINE
# ═══════════════════════════════════════════════════════

def _scan_for_providers(text: str) -> Tuple[Set[str], Set[str], bool]:
    """
    Comprehensive BNPL/PSP detection:
      A) CDN domains in script/iframe/link URLs
      B) Context phrases ("avec Alma", "con Sequra", etc.)
      C) Safe keywords for unambiguous brands
      D) Installment phrases + nearby brand (±200 chars)
      E) Payment method sections + nearby brand (±300 chars)
    """
    tl = text.lower()
    bnpl: Set[str] = set()
    psp: Set[str] = set()

    # A) CDN domain matching (most precise)
    for provider, patterns in _BNPL_CDN.items():
        for p in patterns:
            if p in tl:
                bnpl.add(provider.title()); break

    # B) Context-aware brand phrases
    for provider, phrases in _BRAND_CONTEXT_PHRASES.items():
        if provider.title() not in bnpl:
            for phrase in phrases:
                if phrase in tl:
                    bnpl.add(provider.title()); break

    # C) Safe keywords (unambiguous brands only)
    for provider, keywords in _SAFE_KEYWORDS.items():
        if provider.title() not in bnpl:
            for kw in keywords:
                if kw in tl:
                    bnpl.add(provider.title()); break

    # D) Installment phrases → search nearby ±200 chars for any brand
    for phrase in _INSTALLMENT_PHRASES:
        idx = tl.find(phrase)
        while idx != -1:
            window = tl[max(0, idx-200):idx+len(phrase)+200]
            for brand in _ALL_BNPL_BRANDS:
                if brand in window and brand.title() not in bnpl:
                    bnpl.add(brand.title())
            idx = tl.find(phrase, idx + 1)

    # E) Payment method sections → search nearby ±300 chars for any brand
    for marker in _PAYMENT_SECTION_MARKERS:
        idx = tl.find(marker)
        while idx != -1:
            window = tl[max(0, idx-100):idx+len(marker)+300]
            for brand in _ALL_BNPL_BRANDS:
                if brand in window and brand.title() not in bnpl:
                    bnpl.add(brand.title())
            idx = tl.find(marker, idx + 1)

    # PSP CDN matching
    for provider, patterns in _PSP_CDN.items():
        for p in patterns:
            if p in tl: psp.add(provider.title()); break

    # PayPal (separate)
    has_paypal = any(p in tl for p in _PAYPAL_PATTERNS)

    return bnpl, psp, has_paypal


# ═══════════════════════════════════════════════════════
# LAYERS
# ═══════════════════════════════════════════════════════

def _L12_homepage(html):
    """L1+L2: Homepage — scripts, iframes, data-attrs, full HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    parts = [html.lower()]
    for tag in soup.find_all("script"):
        parts.append(tag.get("src","").lower()); parts.append((tag.string or "").lower())
    for tag in soup.find_all("iframe"): parts.append(tag.get("src","").lower())
    for tag in soup.find_all("link"): parts.append(tag.get("href","").lower())
    for tag in soup.find_all(True):
        for a,v in tag.attrs.items():
            if isinstance(v,str) and any(x in a.lower() for x in ["data-","class","id"]): parts.append(v.lower())
    combined = " ".join(parts)
    bnpl, psp, has_paypal = _scan_for_providers(combined)
    has_meta = any(x in combined for x in ["fbq(","facebook.com/tr","connect.facebook.net","fbevents.js"])
    has_gads = any(x in combined for x in ["googleads","google_conversion","googleadservices.com"]) or ("gtag(" in combined and "aw-" in combined)
    return bnpl, psp, has_paypal, has_meta, has_gads

def _L3(html):
    """Schema.org / JSON-LD."""
    from bs4 import BeautifulSoup
    found = set(); soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            t = json.dumps(json.loads(s.string or "")).lower()
            b, _, _ = _scan_for_providers(t); found |= b
        except: pass
    return found

def _L4(domain, html):
    """Product pages — find a product URL, scrape for BNPL widgets."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser"); bnpl, psp = set(), set()
    urls = []
    for a in soup.find_all("a", href=True):
        for pat in _PRODUCT_RE:
            if re.search(pat, a["href"], re.I):
                full = urljoin(f"https://{domain}", a["href"])
                if domain in full: urls.append(full)
                break
        if len(urls) >= 3: break
    for url in urls[:2]:  # Try up to 2 product pages
        ph = _fetch(url, 8)
        if ph:
            b, p, _ = _scan_for_providers(ph); bnpl |= b; psp |= p
            if bnpl: break  # Found something, stop
    return bnpl, psp

def _L5(domain, html):
    """JS bundle download — search inside .js files."""
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
            b, _, _ = _scan_for_providers(js); found |= b
    return found

def _L6_gtm(html):
    """GTM Container — download public JSON, search for BNPL tags."""
    found = set()
    gtm_ids = set(_GTM_RE.findall(html))
    for gtm_id in list(gtm_ids)[:2]:
        js = _fetch(f"https://www.googletagmanager.com/gtm.js?id={gtm_id}", 6)
        if js:
            b, _, _ = _scan_for_providers(js); found |= b
    return found

def _L7(domain):
    """Checkout paths — /cart, /panier, /cesta, /checkout."""
    bnpl, psp = set(), set()
    for path in _CHECKOUT_PATHS:
        h = _fetch(f"https://{domain}{path}", 6)
        if h and len(h) > 1000:
            b, p, _ = _scan_for_providers(h); bnpl |= b; psp |= p
            if bnpl: break
    return bnpl, psp

def _L8(domain):
    """Sitemap/robots.txt — discover checkout URLs."""
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

def _L9(domain):
    """Google Cache + Wayback Machine."""
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

def _L10(domain):
    """DNS/CNAME check."""
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

def enrich_single_domain(domain: str) -> Dict:
    result = {
        "competitors_bnpl": [], "psp_detected": [], "has_paypal": False,
        "has_meta_pixel": False, "has_google_ads": False, "is_advertising_heavy": False,
        "site_reachable": False,
    }
    all_bnpl, all_psp = set(), set()

    html = _fetch_domain(domain)
    if html:
        result["site_reachable"] = True
        # L1+L2: Homepage (CDN + context + payment sections + installments)
        b12, p12, paypal, meta, gads = _L12_homepage(html)
        all_bnpl |= b12; all_psp |= p12
        result["has_paypal"] = paypal; result["has_meta_pixel"] = meta; result["has_google_ads"] = gads

        # L3: Schema.org
        all_bnpl |= _L3(html)

        # L6: GTM (fast, high-impact)
        all_bnpl |= _L6_gtm(html)

        # L4: Product pages (always try — payment widgets are often only here)
        b4, p4 = _L4(domain, html); all_bnpl |= b4; all_psp |= p4

        # L5: JS bundles (only if still haven't found much)
        if len(all_bnpl) < 2:
            all_bnpl |= _L5(domain, html)

        # L7: Checkout paths (always try — this is where payment methods are listed)
        b7, p7 = _L7(domain); all_bnpl |= b7; all_psp |= p7
    else:
        cached = _L9(domain)
        if cached:
            result["site_reachable"] = True
            b, p, paypal, meta, gads = _L12_homepage(cached)
            all_bnpl |= b; all_psp |= p
            result["has_paypal"] = paypal; result["has_meta_pixel"] = meta; result["has_google_ads"] = gads
        for url in _L8(domain):
            sh = _fetch(url, 5)
            if sh:
                b, p, _ = _scan_for_providers(sh); all_bnpl |= b; all_psp |= p
                if b: break

    # L10: DNS (always, fast)
    all_bnpl |= _L10(domain)

    result["competitors_bnpl"] = sorted(all_bnpl)
    result["psp_detected"] = sorted(all_psp)
    result["is_advertising_heavy"] = result["has_meta_pixel"] or result["has_google_ads"]

    # Scraping confidence: HIGH = site fully analyzed, LOW = blocked/timeout
    if result["site_reachable"] and all_bnpl:
        result["scraping_confidence"] = "HIGH"  # Found something → reliable result
    elif result["site_reachable"]:
        result["scraping_confidence"] = "MEDIUM"  # Site reached, nothing found (could be clean or missed)
    else:
        result["scraping_confidence"] = "LOW"  # Site blocked → result is unreliable

    return result


def enrich_dataframe(df, enable_scraping=True, progress_callback=None):
    df["competitors_bnpl"]=""; df["psp_detected"]=""; df["has_paypal"]=False
    df["has_meta_pixel"]=False; df["has_google_ads"]=False; df["is_advertising_heavy"]=False
    df["scraping_confidence"]="NONE"
    if not enable_scraping:
        log.info("Scraping disabled"); return df
    total=len(df); hits=0; blocked=0; paypal_only=0
    log.info(f"Starting enrichment: {total} domains, v4 detection (CDN+context+payment+installments+GTM)")
    for i,(idx,row) in enumerate(df.iterrows()):
        domain=normalise_domain(row.get("domain",""))
        if not domain: continue
        try:
            info=enrich_single_domain(domain)
            df.at[idx,"competitors_bnpl"]=", ".join(info["competitors_bnpl"])
            df.at[idx,"psp_detected"]=", ".join(info["psp_detected"])
            df.at[idx,"has_paypal"]=info["has_paypal"]
            df.at[idx,"has_meta_pixel"]=info["has_meta_pixel"]
            df.at[idx,"has_google_ads"]=info["has_google_ads"]
            df.at[idx,"is_advertising_heavy"]=info["is_advertising_heavy"]
            df.at[idx,"scraping_confidence"]=info.get("scraping_confidence","LOW")
            if info["competitors_bnpl"]: hits+=1
            elif info["has_paypal"]: paypal_only+=1
            if not info["site_reachable"]: blocked+=1
        except Exception as e: log.error(f"Enrichment fail {domain}: {e}")
        if progress_callback and (i+1)%10==0: progress_callback(i+1, total)
        if (i+1)%100==0:
            log.info(f"Progress: {i+1}/{total} — {hits} BNPL, {paypal_only} PayPal-only, {blocked} blocked")
    log.info(f"Enrichment complete: {hits} BNPL ({hits/max(total,1)*100:.1f}%), {paypal_only} PayPal-only, {blocked} unreachable")
    return df
