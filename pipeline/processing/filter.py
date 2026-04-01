"""
processing/filter.py

Hard-rejects any listing that provably fails a must-have criterion.
Missing data is given benefit of the doubt (not rejected).
Only rejects when value is explicitly known to be out of range.
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

log = logging.getLogger(__name__)

MAX_RENT = 8000
MIN_BEDS = 2
MIN_BATHS = 2.0
MIN_SQFT = 1200
MOVE_IN_DEADLINE = date(2025, 6, 1)


def apply_hard_filters(listings: list[dict]) -> list[dict]:
    passed, rejected = [], 0
    for l in listings:
        fails = _check(l)
        if fails:
            log.debug(f"  REJECT {l.get('address', '?')[:40]}: {', '.join(fails)}")
            rejected += 1
        else:
            passed.append(l)
    return passed


def _check(l: dict) -> list[str]:
    fails = []

    # ── Price ──────────────────────────────────────────
    price = l.get("price")
    if price is None:
        fails.append("no_price")
    elif price > MAX_RENT:
        fails.append(f"price_${price}")

    # ── Bedrooms (reject only if explicitly < 2) ───────
    beds = l.get("bedrooms")
    if beds is not None and beds < MIN_BEDS:
        fails.append(f"beds_{beds}")

    # ── Bathrooms ──────────────────────────────────────
    baths = l.get("bathrooms")
    if baths is not None and baths < MIN_BATHS:
        fails.append(f"baths_{baths}")

    # ── Sqft (reject only if explicitly known small) ───
    sqft = l.get("sqft")
    if sqft is not None and sqft < MIN_SQFT:
        fails.append(f"sqft_{sqft}")

    # ── Ground floor ───────────────────────────────────
    floor = l.get("floor")
    if floor is not None and floor <= 1:
        fails.append(f"ground_floor_{floor}")

    # ── Available date ─────────────────────────────────
    avail = l.get("available_date")
    if avail:
        d = _parse_date(avail)
        if d and d > MOVE_IN_DEADLINE:
            fails.append(f"avail_too_late")

    # ── Address must exist ─────────────────────────────
    if not (l.get("address") or "").strip():
        fails.append("no_address")

    return fails


def _parse_date(s: str) -> Optional[date]:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y", "%b %d %Y"):
        try:
            return datetime.strptime(str(s).strip()[:20], fmt).date()
        except ValueError:
            continue
    return None
