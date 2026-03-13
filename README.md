# Scalapay Territory Engine v5

Lead scoring & territory assignment for IT/FR/ES/PT sales team.

## Quick Start
```bash
pip install -r requirements.txt
python -m streamlit run app.py
```

## v5 Features
- **TTV = avg_monthly_visits (L12M) × 3% × AOV × BNPL Penetration × 12 × AOV_Viability × SAM_Factor**
- **AOV Viability**: HubSpot-backed on 6,567 merchants (Shein €90=€323M, Temu €44=€227M)
- **SAM Factor**: Competition-adjusted (Alma FR=0.65, 3+=0.25). Confidence-aware: blocked sites → 0.75
- **Stage-aware stale detection**: SQL+30d=dead, Negotiation+45d=stale, KYC+60d=probably waiting
- **Holding groups**: Parent company lookup from HubSpot (hs_parent_company_id)
- **Scraping confidence**: HIGH/MEDIUM/LOW — LOW triggers conservative SAM
- **All 12 BNPL brands**: CDN + context phrases + payment sections + installment proximity + GTM
- **PT separate sheets, Competitor Maps per country, HubSpot re-approach leads**

## Scoring Model (5 components)
| Component | Weight | Source |
|-----------|--------|--------|
| Tier | 25% | Internal risk matrix (32 cats × 3 regions) |
| Penetration/TTV | 25% | BNPL pen × estimated MR |
| Growth | 15% | YoY (60%) + MoM (40%) |
| Approachability | 20% | HubSpot CRM: stage-aware stale detection |
| Market Opportunity | 15% | 10-layer BNPL checkout detection |

---
Scalapay Strategy & RevOps · v5.0
