"""
ingestion/scraper.py

Uses the RentCast API (api.rentcast.io) to fetch active rental listings.
One API call per search area — clean JSON response, no scraping, no blocking.

Endpoint: GET https://api.rentcast.io/v1/listings/rental/long-term
Docs:     https://developers.rentcast.io/reference/rental-listings-long-term

API call budget per run (4x/day):
  6 zip codes x 1 call each = 6 calls per run
  6 x 4 runs/day = 24 calls/day
  24 x 30 days = ~720 calls/month

RentCast pricing:
  Starter: $35/mo — 1,000 calls/mo  (comfortable)
  Basic:   $50/mo — 5,000 calls/mo  (plenty of headroom)
"""

import logging
import os
import time
from datetime import datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

RENTCAST_API_KEY = os.environ.get("RENTCAST_API_KEY", "")
RENTCAST_BASE    = "https://api.rentcast.io/v1"
RENTCAST_LIMIT   = int(os.environ.get("RENTCAST_LIMIT", "500"))

# ── Search areas by zip code ──────────────────────────────────────────────────
# Zip codes chosen to cover all target neighborhoods:
#   11201 = Brooklyn Heights, DUMBO, Cobble Hill, Boerum Hill
#   11211 = Williamsburg
#   11222 = Greenpoint
#   11215 = Park Slope, Gowanus
#   11231 = Carroll Gardens, Red Hook, Columbia St Waterfront
#   07030 = Hoboken

SEARCH_AREAS = [
    ("11201", "NY", "brooklyn heights"),
    ("11211", "NY", "williamsburg"),
    ("11222", "NY", "greenpoint"),
    ("11215", "NY", "park slope"),
    ("11231", "NY", "carroll gardens"),
    ("07030", "NJ", "hoboken"),
    ("11101", "NY", "hunters point lic"),
]

MAX_PRICE = 8000
MIN_BEDS  = 2
MIN_BATHS = 2.0
MIN_SQFT  = 1200


def fetch_all_sources(sources=None) -> list[dict]:
    """
    Fetch rental listings from RentCast API for all target zip codes.
    Returns normalized listing dicts ready for the filter/dedupe/score pipeline.
    """
    if not RENTCAST_API_KEY:
        log.error("RENTCAST_API_KEY not set — add it to pipeline/.env and GitHub secrets")
        return []

    all_listings = []
    seen_ids = set()

    for zipcode, state, neighborhood in SEARCH_AREAS:
        try:
            log.info(f"  RentCast: {neighborhood} ({zipcode})...")
            raw = _fetch_zip(zipcode)
            if raw is None:
                continue

            added = 0
            for item in raw:
                rid = item.get("id", "")
                if rid and rid in seen_ids:
                    continue
                if rid:
                    seen_ids.add(rid)
                normalized = _normalize(item, neighborhood)
                if normalized:
                    all_listings.append(normalized)
                    added += 1

            log.info(f"  -> {added} usable listings from {neighborhood}")
            time.sleep(0.5)

        except Exception as e:
            log.error(f"  RentCast {neighborhood} failed: {e}")

    return all_listings


def _fetch_zip(zipcode: str) -> Optional[list]:
    """Single paginated API call for one zip code."""
    headers = {
        "X-Api-Key": RENTCAST_API_KEY,
        "Accept": "application/json",
    }
    params = {
        "zipCode":   zipcode,
        "bedrooms":  MIN_BEDS,
        "bathrooms": MIN_BATHS,
        "status":    "Active",
        "limit":     RENTCAST_LIMIT,
        "offset":    0,
    }

    all_results = []
    page = 0

    while True:
        params["offset"] = page * RENTCAST_LIMIT
        resp = requests.get(
            f"{RENTCAST_BASE}/listings/rental/long-term",
            headers=headers,
            params=params,
            timeout=30,
        )

        if resp.status_code == 401:
            log.error("  RentCast 401: Invalid API key")
            return None
        if resp.status_code == 429:
            log.warning("  RentCast rate limited — waiting 10s...")
            time.sleep(10)
            continue
        if not resp.ok:
            log.warning(f"  RentCast {resp.status_code}: {resp.text[:300]}")
            return None

        data = resp.json()
        batch = data if isinstance(data, list) else (data.get("listings") or data.get("data") or [])
        all_results.extend(batch)

        if len(batch) < RENTCAST_LIMIT:
            break
        page += 1
        time.sleep(0.3)

    return all_results


def _normalize(item: dict, neighborhood: str) -> Optional[dict]:
    """Map RentCast response fields to our internal schema."""
    price = _safe_int(item.get("price"))
    if not price or price > MAX_PRICE:
        return None

    address = (item.get("formattedAddress") or item.get("addressLine1") or "").strip()
    if not address:
        return None

    beds  = _safe_int(item.get("bedrooms"))
    baths = _safe_float(item.get("bathrooms"))
    sqft  = _safe_int(item.get("squareFootage"))

    if beds  is not None and beds  < MIN_BEDS:  return None
    if baths is not None and baths < MIN_BATHS: return None
    if sqft  is not None and sqft  < MIN_SQFT:  return None

    features  = item.get("features") or {}
    amenities = [str(a).lower() for a in (item.get("amenities") or [])]

    laundry = (
        features.get("laundryType") in ("In Unit", "In Building") or
        features.get("laundry") is True or
        _has(amenities, ["washer", "laundry in unit", "w/d in unit"])
    )
    parking = (
        features.get("garage") is True or
        features.get("parkingType") not in (None, "None", "Street") or
        _has(amenities, ["parking", "garage"])
    )

    unit = (item.get("addressLine2") or "").strip()
    unit = unit.replace("Apt ", "").replace("Unit ", "").replace("# ", "").strip()

    photos = item.get("photos") or []
    photo_url = photos[0].get("url", "") if photos and isinstance(photos[0], dict) else ""

    return {
        "source":          "rentcast",
        "rentcast_id":     item.get("id", ""),
        "primary_url":     item.get("url") or item.get("listingUrl") or "",
        "address":         address,
        "unit":            unit,
        "price":           price,
        "bedrooms":        beds,
        "bathrooms":       baths,
        "sqft":            sqft,
        "floor":           None,
        "in_unit_laundry": laundry,
        "dishwasher":      features.get("dishwasher") is True or _has(amenities, ["dishwasher"]),
        "parking":         parking,
        "storage":         _has(amenities, ["storage"]),
        "gym":             (features.get("gym") is True or
                           features.get("fitnessCenter") is True or
                           _has(amenities, ["gym", "fitness"])),
        "description":     (item.get("description") or "")[:3000],
        "available_date":  item.get("listedDate") or item.get("listedOn") or "",
        "neighborhood":    neighborhood,
        "building_name":   item.get("buildingName") or item.get("community") or "",
        "photos_url":      photo_url,
        "days_on_market":  _safe_int(item.get("daysOnMarket")),
        "property_type":   item.get("propertyType") or "",
        "scraped_at":      datetime.utcnow().isoformat(),
    }


def _safe_int(val) -> Optional[int]:
    try:
        return int(float(str(val).replace(",", "")))
    except Exception:
        return None

def _safe_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", ""))
    except Exception:
        return None

def _has(amenities: list, keywords: list) -> bool:
    return any(kw in a for a in amenities for kw in keywords)
