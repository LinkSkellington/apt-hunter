"""
processing/dedupe.py

Pass 1 — Exact fingerprint: SHA-256(normalized_address + unit + beds + baths)
Pass 2 — Fuzzy address:     token_sort_ratio ≥ 92 + matching numeric fields
Pass 3 — Description sim:   TF-IDF cosine ≥ 0.85 within same batch → flag only

Returns (new_listings, updates_to_existing)
Updates carry only the fields that change on each run: last_seen, price, sources.
"""

import hashlib
import json
import logging
import re
from datetime import date
from typing import Optional

log = logging.getLogger(__name__)

FUZZY_THRESHOLD = 92
DESC_THRESHOLD = 0.85

# ── Address normalization ──────────────────────────────────────────────────────

_ABBR = {
    r"\bSt\.?\b": "Street", r"\bAve\.?\b": "Avenue", r"\bBlvd\.?\b": "Boulevard",
    r"\bDr\.?\b": "Drive", r"\bPl\.?\b": "Place", r"\bRd\.?\b": "Road",
    r"\bLn\.?\b": "Lane", r"\bCt\.?\b": "Court", r"\bTer\.?\b": "Terrace",
    r"\bPkwy\.?\b": "Parkway", r"\bN\.?\b": "North", r"\bS\.?\b": "South",
    r"\bE\.?\b": "East", r"\bW\.?\b": "West",
}


def norm_address(addr: str) -> str:
    s = str(addr or "").strip()
    # Drop city/state/zip (keep only first two comma-segments)
    parts = s.split(",")
    s = ", ".join(parts[:2])
    for pat, rep in _ABBR.items():
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)
    s = re.sub(r"\b(apt|unit|suite|ste|#|no\.?)\s*[\w-]+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s]", "", s)
    return re.sub(r"\s+", " ", s).strip().lower()


def norm_unit(unit: str) -> str:
    u = re.sub(r"^(apt|unit|suite|ste|#|no\.?)\s*", "", str(unit or ""), flags=re.IGNORECASE)
    return u.strip().lower()


def fingerprint(l: dict) -> str:
    addr = norm_address(l.get("address", "") or l.get("address_normalized", ""))
    unit = norm_unit(l.get("unit", ""))
    beds = str(l.get("bedrooms") or "")
    baths = str(l.get("bathrooms") or "")
    raw = f"{addr}|{unit}|{beds}|{baths}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


# ── Main entry point ───────────────────────────────────────────────────────────

def deduplicate(
    incoming: list[dict], existing: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Args:
        incoming:  filtered listings from this scrape run
        existing:  all rows currently in DB (only key fields needed)

    Returns:
        new_listings:  records to INSERT
        updates:       records to UPDATE (id + changed fields)
    """
    # Augment all incoming with normalized fields + fingerprint
    for l in incoming:
        l["address_normalized"] = norm_address(l.get("address", ""))
        l["unit_normalized"] = norm_unit(l.get("unit", ""))
        l["dedupe_key"] = fingerprint(l)

    # Build lookup indexes from DB rows
    db_by_key: dict[str, dict] = {}
    db_by_addr: dict[str, dict] = {}
    for row in existing:
        key = row.get("dedupe_key", "")
        addr = row.get("address_normalized", "")
        if key:
            db_by_key[key] = row
        if addr:
            db_by_addr[addr] = row

    new_listings: list[dict] = []
    updates: list[dict] = []
    seen_keys: set[str] = set()  # dedupe within the current batch too

    for l in incoming:
        key = l["dedupe_key"]
        if key in seen_keys:
            continue

        # ── Pass 1: exact fingerprint ──────────────────
        existing_row = db_by_key.get(key)
        if existing_row:
            updates.append(_build_update(existing_row, l))
            seen_keys.add(key)
            continue

        # ── Pass 2: fuzzy address ──────────────────────
        existing_row = _fuzzy_match(l, db_by_addr)
        if existing_row:
            updates.append(_build_update(existing_row, l))
            # Backfill key so we don't re-match next run
            db_by_key[key] = existing_row
            seen_keys.add(key)
            continue

        # ── Pass 3: description similarity (within batch) ──
        similar = _desc_match(l, new_listings)
        if similar:
            l["status"] = "possible_duplicate"
            log.info(f"  Pass-3 flag: {l['address'][:40]} ≈ {similar['address'][:40]}")

        new_listings.append(l)
        seen_keys.add(key)

    return new_listings, updates


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_update(db_row: dict, incoming: dict) -> dict:
    today = date.today().isoformat()

    # Merge source lists
    existing_sources: list = db_row.get("sources") or []
    inc_source = incoming.get("source", "")
    if inc_source and inc_source not in existing_sources:
        existing_sources = existing_sources + [inc_source]

    # Track price discrepancies
    inc_price = incoming.get("price")
    cur_min = db_row.get("price_min_seen") or inc_price
    cur_max = db_row.get("price_max_seen") or inc_price
    new_min = min(filter(None, [inc_price, cur_min])) if inc_price else cur_min
    new_max = max(filter(None, [inc_price, cur_max])) if inc_price else cur_max

    # Merge URLs
    existing_urls: list = db_row.get("source_urls") or []
    inc_url = incoming.get("primary_url", "")
    if inc_url and inc_url not in existing_urls:
        existing_urls = existing_urls + [inc_url]

    return {
        "id": db_row["id"],
        "last_seen": today,
        "price": inc_price or db_row.get("price"),
        "price_min_seen": new_min,
        "price_max_seen": new_max,
        "sources": existing_sources,
        "source_urls": existing_urls,
    }


def _fuzzy_match(l: dict, db_by_addr: dict[str, dict]) -> Optional[dict]:
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return None  # graceful degradation if rapidfuzz not installed

    addr = l["address_normalized"]
    beds = l.get("bedrooms")
    baths = l.get("bathrooms")
    sqft = l.get("sqft")

    for db_addr, db_row in db_by_addr.items():
        score = fuzz.token_sort_ratio(addr, db_addr)
        if score < FUZZY_THRESHOLD:
            continue
        # Verify numeric fields match
        if beds is not None and db_row.get("bedrooms") is not None:
            if beds != db_row["bedrooms"]:
                continue
        if baths is not None and db_row.get("bathrooms") is not None:
            if abs(baths - db_row["bathrooms"]) > 0.5:
                continue
        if sqft is not None and db_row.get("sqft") is not None:
            if abs(sqft - db_row["sqft"]) / max(sqft, db_row["sqft"]) > 0.06:
                continue
        log.debug(f"  Fuzzy match ({score}%): '{addr}' ≈ '{db_addr}'")
        return db_row
    return None


def _desc_match(l: dict, candidates: list[dict]) -> Optional[dict]:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np
    except ImportError:
        return None

    desc = (l.get("description") or "")[:500]
    if not desc or not candidates:
        return None

    c_descs = [(c.get("description") or "")[:500] for c in candidates]
    if not any(c_descs):
        return None

    try:
        vect = TfidfVectorizer(min_df=1, stop_words="english")
        mat = vect.fit_transform([desc] + c_descs)
        sims = cosine_similarity(mat[0:1], mat[1:])[0]
        best = int(np.argmax(sims))
        if sims[best] >= DESC_THRESHOLD:
            return candidates[best]
    except Exception:
        pass
    return None
