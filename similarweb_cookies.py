"""
similarweb_cookies.py

Shared cookie management module for Similarweb Pro API.
Imported by similarweb_client.py (ingestion) and similarweb_scraper.py (enrichment).
"""

import json
import os
from datetime import datetime
from typing import Optional

from utils import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

META_FILE_NAME = "cookie_meta.json"
BASE_URL = "https://pro.similarweb.com"
DEFAULT_EXPIRY_THRESHOLD_DAYS = 25

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://pro.similarweb.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
    "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"',
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _meta_path() -> str:
    """Return the absolute path to cookie_meta.json (same directory as this module)."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), META_FILE_NAME)


def _read_meta() -> dict:
    """
    Read and return the contents of cookie_meta.json.
    Returns {} on any error (missing file, malformed JSON, permission error).
    Logs a warning on error.
    """
    path = _meta_path()
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        logger.warning("cookie_meta.json contains malformed JSON: %s", exc)
        return {}
    except OSError as exc:
        logger.warning("Could not read cookie_meta.json: %s", exc)
        return {}


def _write_meta(data: dict) -> None:
    """
    Write *data* to cookie_meta.json.
    Logs a warning on error and does NOT raise.
    """
    path = _meta_path()
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except OSError as exc:
        logger.warning("Could not write cookie_meta.json: %s", exc)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def load_cookies() -> str:
    """
    Load cookies from cookie_meta.json first; fall back to the SW_COOKIES env var.
    If the env var is used, auto-save to cookie_meta.json.
    Returns an empty string if nothing is found.
    """
    meta = _read_meta()
    cookies = meta.get("cookies", "").strip()
    if cookies:
        return cookies

    env_cookies = os.environ.get("SW_COOKIES", "")
    if env_cookies:
        logger.info("Loaded cookies from SW_COOKIES env var; saving to cookie_meta.json.")
        save_cookies(env_cookies)
        return env_cookies

    return ""


def save_cookies(cookie_string: str) -> None:
    """
    Save *cookie_string* together with a current timestamp.
    Preserves existing expiry_threshold_days if present; defaults to 25 if not.
    """
    existing = _read_meta()
    threshold = existing.get("expiry_threshold_days", DEFAULT_EXPIRY_THRESHOLD_DAYS)
    data = dict(existing)
    data["cookies"] = cookie_string.strip()
    data["cookies_updated_at"] = datetime.now().isoformat(timespec="seconds")
    data["expiry_threshold_days"] = threshold
    _write_meta(data)


def save_threshold(threshold_days: int) -> None:
    """Save a custom expiry threshold to cookie_meta.json."""
    data = _read_meta()
    data["expiry_threshold_days"] = threshold_days
    _write_meta(data)


def get_threshold() -> int:
    """Return the current expiry threshold from cookie_meta.json; default 25."""
    meta = _read_meta()
    return meta.get("expiry_threshold_days", DEFAULT_EXPIRY_THRESHOLD_DAYS)


def cookies_age_days() -> int:
    """
    Return the number of whole days since cookies_updated_at.
    Returns 999 if the timestamp is missing or unparseable.
    """
    meta = _read_meta()
    raw_ts = meta.get("cookies_updated_at")
    if not raw_ts:
        return 999
    try:
        updated_at = datetime.fromisoformat(raw_ts)
    except (ValueError, TypeError):
        return 999
    return (datetime.now() - updated_at).days


def is_expired(threshold_days: Optional[int] = None) -> bool:
    """
    Return True if the cookies are older than threshold_days.
    Uses get_threshold() when no argument is supplied.
    """
    if threshold_days is None:
        threshold_days = get_threshold()
    return cookies_age_days() > threshold_days


def get_cookie_status() -> dict:
    """
    Return a summary dict with keys:
        cookies, age_days, expired, threshold_days
    """
    cookies = load_cookies()
    age = cookies_age_days()
    threshold = get_threshold()
    return {
        "cookies": cookies,
        "age_days": age,
        "expired": age > threshold,
        "threshold_days": threshold,
    }
