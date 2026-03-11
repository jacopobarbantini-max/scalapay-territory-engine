# Scalapay Territory Engine v4

Automated lead scoring & territory list generator for IT/FR/IB BNPL sales team.

## Quick Start

```bash
pip install -r requirements.txt
python -m streamlit run app.py
```

Open `http://localhost:8501`. Upload Similarweb XLSX exports → scored territory lists.

## Setup

1. Copy `.env.example` to `.env`
2. Add your HubSpot Private App token: `HUBSPOT_API_KEY=pat-eu1-...`
3. Optional: `pip install curl_cffi` for Cloudflare bypass (+10% scraping hit rate)

## Scoring Model (5 components)

| Component | Weight | Source |
|-----------|--------|--------|
| Tier | 25% | Internal risk matrix (32 cats × 3 regions, 14 travel sub-categories) |
| Penetration/TTV | 25% | SW Monthly Txns × AOV(category) × BNPL Penetration(cat × country) |
| Growth | 15% | YoY (60%) + MoM (40%) traffic momentum from Similarweb |
| Approachability | 20% | HubSpot CRM: Net New > Lost>6mo > Stale(45d) > Cold < Warm |
| Market Opportunity | 15% | BNPL competition at checkout: TOP/MEDIUM-HIGH/MEDIUM/LOW |

Account Size (Strategic/Enterprise/Executive) used for territory assignment only, not scoring.

## Enrichment (10 layers, ~35-45% hit rate)

CDN-domain matching (v3) — zero false positives. PayPal tracked separately.

| Layer | Method | Impact |
|-------|--------|--------|
| L1-L2 | Homepage HTML + JS/CDN sources (30+ patterns) | ~35% |
| L3 | Schema.org / JSON-LD structured data | +5% |
| L6 | GTM Container JSON (public, high impact) | +5-8% |
| L4 | Product pages (BNPL widgets) | +12% |
| L5 | JS bundle download + search | +8% |
| L7 | Checkout paths (/cart, /panier, /cesta) | +3% |
| L8-L9 | Sitemap + Google Cache + Wayback Machine | +5% |
| L10 | DNS/CNAME check | +2% |

## Files

```
app.py              — Streamlit UI (upload, sidebar, dashboard, export)
config.py           — Tiering matrix, AOV, penetration, whitespace categories
scoring.py          — 5-component scoring, TTV estimation, market opportunity
enrichment.py       — 10-layer BNPL/PSP/ad detection (v3, CDN matching)
hubspot_client.py   — Bulk fetch, fuzzy matching, stale deal detection
similarweb_client.py — SW file parser
utils.py            — Domain normalization, bucket parsing, logging
```

## Run Times (9,000 leads)

- **Without scraping:** ~5-25 min (ingest + HubSpot + scoring)
- **With scraping:** ~4-8 hours (overnight recommended)

## Country Tiering

- **PT = Iberia (ES)** — same tiers and penetration rates
- **IT = Italy** — separate tier matrix
- **Travel:** 14 sub-categories by net default rate (OTA/Hotel = BRONZE, Theme Parks/Ticketing = GOLD)

---
Scalapay Strategy & RevOps · v4.0
