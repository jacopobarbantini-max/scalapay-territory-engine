"""
utils.py - Shared helpers for the Scalapay Territory Engine.
"""

import logging
import re
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import numpy as np


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def normalise_domain(raw: Optional[str]) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    raw = raw.strip().lower()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    parsed = urlparse(raw)
    domain = parsed.netloc or parsed.path
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.rstrip("/")
    return domain


def parse_revenue_bucket(val) -> float:
    if not val or not isinstance(val, str) or val.strip().lower() == "nan":
        return 0.0
    val = val.strip().replace(",", "")
    multipliers = {"B": 1_000_000_000, "M": 1_000_000, "K": 1_000}
    if val.startswith(">"):
        num_str = val.replace(">", "").strip()
        for suffix, mult in multipliers.items():
            if suffix in num_str:
                return float(num_str.replace(suffix, "").strip()) * mult * 1.5
        return 0.0
    if " - " in val:
        parts = val.split(" - ")
        nums = []
        for p in parts:
            p = p.strip()
            for suffix, mult in multipliers.items():
                if suffix in p:
                    nums.append(float(p.replace(suffix, "").strip()) * mult)
                    break
        if len(nums) == 2:
            return (nums[0] + nums[1]) / 2
        elif nums:
            return nums[0]
    return 0.0


def parse_employees_bucket(val) -> int:
    if not val or not isinstance(val, str) or val.strip().lower() == "nan":
        return 0
    val = val.strip().replace(",", "")
    if val.startswith(">"):
        try:
            return int(float(val.replace(">", "").strip()) * 1.5)
        except ValueError:
            return 0
    if " - " in val:
        parts = val.split(" - ")
        try:
            nums = [int(float(p.strip())) for p in parts]
            return (nums[0] + nums[1]) // 2
        except ValueError:
            return 0
    try:
        return int(float(val))
    except ValueError:
        return 0


def parse_transactions_bucket(val) -> float:
    if not val or not isinstance(val, str) or val.strip().lower() == "nan":
        return 0.0
    val = val.strip().replace(",", "").replace("+", "")
    if " - " in val:
        parts = val.split(" - ")
        nums = []
        for p in parts:
            p = p.strip()
            if "K" in p:
                nums.append(float(p.replace("K", "")) * 1000)
            elif "M" in p:
                nums.append(float(p.replace("M", "")) * 1_000_000)
            else:
                try:
                    nums.append(float(p))
                except ValueError:
                    pass
        if len(nums) == 2:
            return (nums[0] + nums[1]) / 2
        elif nums:
            return nums[0]
        return 0.0
    if "K" in val:
        return float(val.replace("K", "").strip()) * 1000
    if "M" in val:
        return float(val.replace("M", "").strip()) * 1_000_000
    try:
        return float(val)
    except ValueError:
        return 0.0


def clean_similarweb_df(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {
        "Domain": "domain",
        "Get data from": "data_scope",
        "In HubSpot": "in_hubspot_sw",
        "Is new": "is_new",
        "Annual Revenue": "annual_revenue_bucket",
        "Employees": "employees_bucket",
        "Top country": "top_country",
        "Industry": "industry",
        "Monthly visits": "monthly_traffic",
        "YoY traffic change": "yoy_growth",
        "Total page views": "total_page_views",
        "Monthly transactions": "monthly_transactions_bucket",
        "HQ country": "hq_country",
        "Email address": "email",
        "Average monthly visits": "avg_monthly_visits",
        "MoM traffic change": "mom_growth",
    }
    renamed = {}
    for old, new in col_map.items():
        if old in df.columns:
            renamed[old] = new
    df = df.rename(columns=renamed)

    if "domain" in df.columns:
        df["domain"] = df["domain"].apply(normalise_domain)
        df = df[df["domain"] != ""].reset_index(drop=True)

    for col in ["monthly_traffic", "total_page_views", "avg_monthly_visits"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["yoy_growth", "mom_growth"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0) * 100

    if "annual_revenue_bucket" in df.columns:
        df["annual_revenue_est"] = df["annual_revenue_bucket"].apply(parse_revenue_bucket)

    if "employees_bucket" in df.columns:
        df["employees_est"] = df["employees_bucket"].apply(parse_employees_bucket)

    if "monthly_transactions_bucket" in df.columns:
        df["monthly_transactions_est"] = df["monthly_transactions_bucket"].apply(
            parse_transactions_bucket
        )

    if "industry" in df.columns:
        df["category"] = df["industry"].apply(
            lambda x: x.split("/")[-1].strip()
            if isinstance(x, str) and "/" in x
            else (x.strip() if isinstance(x, str) else "Unknown")
        )

    return df


def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
