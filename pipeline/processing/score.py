"""
processing/score.py

100-point weighted rubric → score_tier + heat classification.

Points breakdown:
  Hard criteria gate    30  (laundry, DW, floor, price, beds, baths, sqft)
  Commute to Midtown    20
  Square footage        10
  Neighborhood          15
  Natural light         10
  Nice-to-haves         10
  Building reviews       5
  ─────────────────────100

Tier:
  🔥 Must Tour   ≥ 65 AND all hard criteria pass AND commute ≤ 45 min
  👍 Strong      45–79, OR ≥ 68 with commute 45–60 min
  🤔 Backup      35–54
  ❌ Skip        < 35 OR any hard fail

Heat:
  🔥 Hot    first_seen ≤ 3 days AND price ≤ $7,500
             OR first_seen ≤ 7 days AND tier = Must Tour
  ⏳ Normal  4–21 days on market, or unknown
  🧊 Stale   > 21 days on market OR last_seen > 5 days ago
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

log = logging.getLogger(__name__)

# ── Neighborhood weights ───────────────────────────────────────────────────────
# Tuned to stated preferences (waterfront Brooklyn > inland, Hoboken = decent)
_NEIGH_SCORE: dict[str, int] = {
    "brooklyn heights": 15,
    "cobble hill": 15,
    "dumbo": 14,
    "vinegar hill": 13,
    "carroll gardens": 12,
    "boerum hill": 12,
    "williamsburg": 12,
    "greenpoint": 11,
    "columbia street waterfront district": 11,
    "red hook": 10,
    "park slope": 10,
    "gowanus": 8,
    "hoboken": 8,
    "jersey city heights": 5,
    "hunters point lic": 20,
}

# Estimated commute minutes from neighborhood to Midtown by transit
_COMMUTE_EST: dict[str, int] = {
    "brooklyn heights": 30,
    "dumbo": 28,
    "vinegar hill": 30,
    "cobble hill": 35,
    "boerum hill": 33,
    "carroll gardens": 37,
    "columbia street waterfront district": 38,
    "williamsburg": 27,
    "greenpoint": 34,
    "park slope": 44,
    "red hook": 44,
    "gowanus": 42,
    "hoboken": 37,
    "jersey city heights": 48,
}

_LIGHT_POS = [
    "floor-to-ceiling windows", "floor to ceiling", "south-facing", "south facing",
    "corner unit", "sun-drenched", "sun drenched", "light-filled", "light filled",
    "abundant natural light", "great natural light", "eastern exposure",
    "western exposure", "southern exposure", "panoramic", "high floor",
    "penthouse", "airy", "bright and",
]
_LIGHT_NEG = [
    "basement", "garden level", "garden apartment", "below grade",
    "north-facing", "north facing", "limited light", "partial light",
    "courtyard view", "interior unit",
]


# ── Public entry point ─────────────────────────────────────────────────────────

def score_listing(l: dict) -> dict:
    l = l.copy()

    hard_pts, hard_ok = _score_hard(l)
    commute_pts, commute_ok, commute_min, neigh = _score_commute_and_neigh(l)
    sqft_pts = _score_sqft(l)
    neigh_pts = _NEIGH_SCORE.get(neigh or "", 5)
    light_pts, light_conf = _score_light(l)
    amenity_pts = _score_amenities(l)
    building_pts = _score_building(l)

    total = hard_pts + commute_pts + sqft_pts + neigh_pts + light_pts + amenity_pts + building_pts
    total = max(0, min(100, round(total)))

    tier = _tier(total, hard_ok, commute_min)
    heat = _heat(l, tier)

    l.update(
        score_raw=total,
        score_tier=tier,
        heat=heat,
        commute_ok=commute_ok,
        commute_minutes=commute_min or l.get("commute_minutes"),
        natural_light_confidence=light_conf,
        neighborhood=neigh or l.get("neighborhood", ""),
        is_ground_floor=(l.get("floor") or 99) <= 1,
    )
    return l


# ── Component scorers ──────────────────────────────────────────────────────────

def _score_hard(l: dict) -> tuple[int, bool]:
    pts = 30
    ok = True

    price = l.get("price", 0) or 0
    if price > 8000:
        pts -= 30; ok = False

    beds = l.get("bedrooms")
    if beds is not None and beds < 2:
        pts -= 15; ok = False

    baths = l.get("bathrooms")
    if baths is not None and baths < 2.0:
        pts -= 15; ok = False

    # Bonus: 3+ bedrooms with 2+ bathrooms — prioritized in Top Picks
    if beds is not None and baths is not None and beds >= 3 and baths >= 2.0:
        pts += 8

    sqft = l.get("sqft")
    if sqft is not None and sqft < 1200:
        pts -= 10; ok = False

    floor = l.get("floor")
    if floor is not None and floor <= 1:
        pts -= 20; ok = False

    # Penalize known absence (not unknown)
    if l.get("in_unit_laundry") is False:
        pts -= 8
    if l.get("dishwasher") is False:
        pts -= 3

    return max(0, pts), ok


def _score_commute_and_neigh(l: dict) -> tuple[int, bool, Optional[int], Optional[str]]:
    """Returns (commute_pts, commute_ok, minutes, neighborhood_name)."""
    # Resolve neighborhood from listing or address
    neigh = _detect_neighborhood(l)

    minutes = l.get("commute_minutes")
    if minutes is None:
        minutes = _COMMUTE_EST.get(neigh or "", None)

    if minutes is None:
        return 12, True, None, neigh  # unknown → mid-range benefit of doubt

    if minutes <= 25:
        return 20, True, minutes, neigh
    if minutes <= 30:
        return 18, True, minutes, neigh
    if minutes <= 35:
        return 16, True, minutes, neigh
    if minutes <= 45:
        return 14, True, minutes, neigh
    if minutes <= 55:
        return 9, True, minutes, neigh
    if minutes <= 60:
        return 6, True, minutes, neigh
    return 0, False, minutes, neigh


def _score_sqft(l: dict) -> int:
    sqft = l.get("sqft")
    if sqft is None:
        return 5  # unknown → neutral
    if sqft >= 1800: return 10
    if sqft >= 1600: return 9
    if sqft >= 1400: return 8
    if sqft >= 1300: return 7
    if sqft >= 1200: return 5
    return 2


def _score_light(l: dict) -> tuple[int, str]:
    desc = (l.get("description") or "").lower()
    floor = l.get("floor")

    pos = sum(1 for kw in _LIGHT_POS if kw in desc)
    neg = sum(1 for kw in _LIGHT_NEG if kw in desc)

    if floor is not None:
        if floor >= 10: pos += 3
        elif floor >= 6: pos += 2
        elif floor >= 3: pos += 1

    if neg > 0:
        return 2, "Low"
    if pos >= 3:
        return 10, "High"
    if pos >= 1:
        return 6, "Medium"
    return 3, "Unknown"


def _score_amenities(l: dict) -> int:
    pts = 0
    if l.get("storage"):  pts += 3
    if l.get("parking"):  pts += 4
    if l.get("gym"):      pts += 3
    return min(10, pts)


def _score_building(l: dict) -> int:
    r = (l.get("building_reviews") or "").lower()
    return {"good": 5, "mixed": 2, "bad": -5}.get(r, 2)


# ── Neighborhood detection ─────────────────────────────────────────────────────

def _detect_neighborhood(l: dict) -> Optional[str]:
    explicit = (l.get("neighborhood") or "").lower().strip()
    if explicit and explicit in _NEIGH_SCORE:
        return explicit

    haystack = (
        (l.get("address_normalized") or l.get("address") or "") + " " +
        (l.get("description") or "")
    ).lower()

    # Longest match first
    for neigh in sorted(_NEIGH_SCORE, key=len, reverse=True):
        if neigh in haystack:
            return neigh
    return None


# ── Tier + Heat ────────────────────────────────────────────────────────────────

def _tier(score: int, hard_ok: bool, commute_min: Optional[int]) -> str:
    if not hard_ok:
        return "❌ Skip"
    if score >= 68 and (commute_min is None or commute_min <= 45):
        return "🔥 Must Tour"
    if score >= 65:   # commute 45-60
        return "👍 Strong"
    if score >= 45:
        return "👍 Strong"
    if score >= 35:
        return "🤔 Backup"
    return "❌ Skip"


def _heat(l: dict, tier: str) -> str:
    today = date.today()

    def _days_since(field: str) -> Optional[int]:
        val = l.get(field)
        if not val:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                d = datetime.strptime(str(val)[:19], fmt).date()
                return (today - d).days
            except ValueError:
                continue
        return None

    dom = _days_since("first_seen")   # days on market
    dss = _days_since("last_seen")    # days since last seen

    # Stale checks
    if dom is not None and dom > 21:
        return "🧊 Stale"
    if dss is not None and dss > 5:
        return "🧊 Stale"

    # Hot checks
    price = l.get("price", 0) or 0
    if dom is not None:
        if dom <= 3 and price <= 7500:
            return "🔥 Hot"
        if dom <= 7 and tier == "🔥 Must Tour":
            return "🔥 Hot"

    if dom is None:
        return "🔥 Hot"  # just scraped → assume fresh

    return "⏳ Normal"
