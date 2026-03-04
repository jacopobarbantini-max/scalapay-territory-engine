"""
hubspot_client.py — HubSpot CRM integration.

For each merchant domain, looks up:
  - Existing company record
  - Associated deals (pipeline, stage, owner)
  - Cross-country flag
  - Lead warmth classification

Falls back to empty data if no API key is configured.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from config import (
    CLOSED_LOST_REACTIVATION_MONTHS,
    HS_CROSS_COUNTRY_PROPERTY,
    HS_PIPELINES,
    HS_STAGE_LABELS,
    HS_WON_STAGE_IDS,
    HS_LOST_STAGE_IDS,
    HS_ACTIVE_STAGE_IDS,
)
from utils import get_logger, normalise_domain

log = get_logger(__name__)


def _get_api_key() -> Optional[str]:
    return os.getenv("HUBSPOT_API_KEY", "").strip() or None


# ── LIVE HUBSPOT LOOKUP ─────────────────────────────────────
def _search_company_by_domain(domain: str, api_key: str) -> Optional[dict]:
    """Search HubSpot companies by domain property."""
    import requests

    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "domain",
                        "operator": "CONTAINS_TOKEN",
                        "value": domain,
                    }
                ]
            }
        ],
        "properties": [
            "domain",
            "name",
            HS_CROSS_COUNTRY_PROPERTY,
            "country",
            "hs_lead_status",
        ],
        "limit": 1,
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0] if results else None
    except Exception as exc:
        log.error(f"HS company search error ({domain}): {exc}")
        return None


def _get_deals_for_company(company_id: str, api_key: str) -> List[dict]:
    """Fetch associated deals for a HubSpot company, including pipeline ID."""
    import requests

    # Get associated deal IDs
    assoc_url = (
        f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        f"/associations/deals"
    )
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(assoc_url, headers=headers, timeout=15)
        resp.raise_for_status()
        deal_ids = [r["id"] for r in resp.json().get("results", [])]
    except Exception as exc:
        log.error(f"HS deal association error: {exc}")
        return []

    deals = []
    for did in deal_ids[:10]:  # cap to avoid rate limits
        deal_url = f"https://api.hubapi.com/crm/v3/objects/deals/{did}"
        params = {
            "properties": "dealstage,dealname,hubspot_owner_id,closedate,amount,pipeline"
        }
        try:
            resp = requests.get(
                deal_url, headers=headers, params=params, timeout=10
            )
            resp.raise_for_status()
            deals.append(resp.json().get("properties", {}))
        except Exception:
            continue
    return deals


def _resolve_owner_name(owner_id: str, api_key: str) -> str:
    """Resolve HubSpot owner ID to display name."""
    import requests

    if not owner_id:
        return ""
    url = f"https://api.hubapi.com/crm/v3/owners/{owner_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
    except Exception:
        return owner_id


# ── DEAL CLASSIFICATION ─────────────────────────────────────
def classify_deal(deals: List[dict]) -> dict:
    """
    Classify lead based on deal history. Returns:
      - lead_warmth: Warm / Existing Won / Cold/Lost / Net New
      - pipeline: Sales / Inbound / Partner / Partnership / Account Mgmt / Churn
      - deal_stage_label: human-readable stage
      - is_won: True if any deal is Won
      - is_in_pipeline: True if any deal is actively in pipeline
      - deal_owner: owner of the most relevant deal
    """
    result = {
        "lead_warmth": "Net New",
        "pipeline": "",
        "deal_stage_label": "",
        "is_won": False,
        "is_in_pipeline": False,
    }

    if not deals:
        return result

    # Sort deals by priority: active > won > lost
    # Pick the most relevant deal to show
    best_deal = None
    best_priority = -1

    for deal in deals:
        stage_id = str(deal.get("dealstage", ""))
        pipeline_id = str(deal.get("pipeline", ""))

        if stage_id in HS_ACTIVE_STAGE_IDS:
            priority = 3  # Active pipeline — highest
        elif stage_id in HS_WON_STAGE_IDS:
            priority = 2  # Won
        elif stage_id in HS_LOST_STAGE_IDS:
            priority = 1  # Lost
        else:
            priority = 0  # Unknown

        if priority > best_priority:
            best_priority = priority
            best_deal = deal

    if not best_deal:
        return result

    stage_id = str(best_deal.get("dealstage", ""))
    pipeline_id = str(best_deal.get("pipeline", ""))

    result["pipeline"] = HS_PIPELINES.get(pipeline_id, pipeline_id)
    result["deal_stage_label"] = HS_STAGE_LABELS.get(stage_id, stage_id)

    # Check all deals for Won and Active flags
    for deal in deals:
        sid = str(deal.get("dealstage", ""))
        if sid in HS_WON_STAGE_IDS:
            result["is_won"] = True
        if sid in HS_ACTIVE_STAGE_IDS:
            result["is_in_pipeline"] = True

    # Classify warmth
    if result["is_in_pipeline"]:
        result["lead_warmth"] = "Warm"
    elif result["is_won"]:
        result["lead_warmth"] = "Existing Won"
    elif stage_id in HS_LOST_STAGE_IDS:
        # Check reactivation window
        close_str = best_deal.get("closedate", "")
        cutoff = datetime.utcnow() - timedelta(
            days=CLOSED_LOST_REACTIVATION_MONTHS * 30
        )
        if close_str:
            try:
                close_dt = datetime.fromisoformat(
                    close_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)
                if close_dt < cutoff:
                    result["lead_warmth"] = "Cold/Lost (re-approachable)"
                else:
                    result["lead_warmth"] = "Cold/Lost"
            except Exception:
                result["lead_warmth"] = "Cold/Lost"
        else:
            result["lead_warmth"] = "Cold/Lost"
    else:
        result["lead_warmth"] = "In HubSpot (unknown)"

    return result


# ── MAIN ENRICHMENT FUNCTION ────────────────────────────────
def enrich_with_hubspot(df: pd.DataFrame, progress_callback=None) -> pd.DataFrame:
    """Add HubSpot columns to the leads DataFrame. Multithreaded."""
    api_key = _get_api_key()

    # Prepare new columns
    df["hs_exists"] = False
    df["hs_company_name"] = ""
    df["pipeline"] = ""
    df["deal_stage"] = ""
    df["deal_owner"] = ""
    df["hs_cross_country"] = False
    df["is_won"] = False
    df["is_in_pipeline"] = False
    df["lead_warmth"] = "Net New"

    if not api_key:
        log.warning("No HUBSPOT_API_KEY — all leads marked as Net New.")
        return df

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Lock

    total = len(df)
    counter = {"done": 0}
    lock = Lock()

    def process_domain(idx, domain):
        try:
            result = {"idx": idx}
            company = _search_company_by_domain(domain, api_key)
            if not company:
                return result

            props = company.get("properties", {})
            company_id = company.get("id")

            result["hs_exists"] = True
            result["hs_company_name"] = props.get("name", "")
            result["hs_cross_country"] = bool(props.get(HS_CROSS_COUNTRY_PROPERTY, ""))

            deals = _get_deals_for_company(company_id, api_key) if company_id else []
            info = classify_deal(deals)
            result["lead_warmth"] = info["lead_warmth"]
            result["pipeline"] = info["pipeline"]
            result["deal_stage"] = info["deal_stage_label"]
            result["is_won"] = info["is_won"]
            result["is_in_pipeline"] = info["is_in_pipeline"]

            if deals:
                owner_id = deals[0].get("hubspot_owner_id", "")
                result["deal_owner"] = _resolve_owner_name(owner_id, api_key)

            return result
        except Exception as exc:
            log.error(f"HubSpot enrichment failed for {domain}: {exc}")
            return {"idx": idx}

    work_items = []
    for idx, row in df.iterrows():
        domain = normalise_domain(row.get("domain", ""))
        if domain:
            work_items.append((idx, domain))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_domain, idx, domain): (idx, domain)
            for idx, domain in work_items
        }
        for future in as_completed(futures):
            result = future.result()
            idx = result["idx"]
            for col in ["hs_exists", "hs_company_name", "pipeline", "deal_stage",
                        "deal_owner", "hs_cross_country", "is_won", "is_in_pipeline",
                        "lead_warmth"]:
                if col in result:
                    df.at[idx, col] = result[col]

            with lock:
                counter["done"] += 1
                if counter["done"] % 50 == 0:
                    log.info(f"HubSpot enrichment: {counter['done']}/{total}")
                if progress_callback:
                    progress_callback(counter["done"], total)

    return df
