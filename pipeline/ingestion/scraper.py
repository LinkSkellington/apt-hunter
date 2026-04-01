"""
ingestion/scraper.py

Uses direct HTTP requests with realistic headers instead of Playwright.
Much more reliable in GitHub Actions — no browser installation needed.

Sources:
  - StreetEasy  (JSON API + HTML fallback)
  - Zillow      (JSON search API)
  - Redfin      (JSON API)
"""

import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})

STREETEASY_AREAS = [
    ("brooklyn-heights", "brooklyn heights"),
    ("williamsburg",     "williamsburg"),
    ("cobble-hill",      "cobble hill"),
    ("red-hook",         "red hook"),
    ("greenpoint",       "greenpoint"),
    ("park-slope",       "park slope"),
    ("dumbo",            "dumbo"),
    ("carroll-gardens",  "carroll gardens"),
    ("boerum-hill",      "boerum hill"),
]

ZILLOW_SEARCHES = [
    {
        "label": "brooklyn",
        "searchQueryState": {
            "pagination": {},
            "usersSearchTerm": "Brooklyn, NY",
            "mapBounds": {"west": -74.0479, "east": -73.8765, "south": 40.5771, "north": 40.7396},
            "filterState": {
                "fr": {"value": True}, "fsba": {"value": False}, "fsbo": {"value": False},
                "nc": {"value": False}, "cmsn": {"value": False}, "auc": {"value": False},
                "fore": {"value": False}, "mp": {"max": 8000}, "beds": {"min": 2},
            },
            "isListVisible": True, "isMapVisible": False,
        }
    },
    {
        "label": "hoboken",
        "searchQueryState": {
            "pagination": {},
            "usersSearchTerm": "Hoboken, NJ",
            "mapBounds": {"west": -74.0700, "east": -74.0100, "south": 40.7300, "north": 40.7700},
            "filterState": {
                "fr": {"value": True}, "fsba": {"value": False}, "fsbo": {"value": False},
                "mp": {"max": 8000}, "beds": {"min": 2},
            },
            "isListVisible": True, "isMapVisible": False,
        }
    },
]


def fetch_all_sources(sources=None) -> list[dict]:
    targets = sources or ["streeteasy", "zillow", "redfin"]
    all_listings = []
    for source in targets:
        try:
            log.info(f"  Fetching from {source}...")
            if source == "streeteasy":
                listings = fetch_streeteasy()
            elif source == "zillow":
                listings = fetch_zillow()
            elif source == "redfin":
                listings = fetch_redfin()
            else:
                listings = []
            log.info(f"  -> {len(listings)} listings from {source}")
            all_listings.extend(listings)
        except Exception as e:
            log.error(f"  Source {source} failed: {e}")
    return all_listings


def fetch_streeteasy() -> list[dict]:
    results = []
    for area_slug, area_name in STREETEASY_AREAS:
        try:
            url = f"https://streeteasy.com/for-rent/{area_slug}"
            params = {"price": "-8000", "beds": "2", "baths": "2", "size": "1200"}
            headers = {**SESSION.headers, "Referer": "https://streeteasy.com/", "Accept": "text/html,application/xhtml+xml"}
            resp = SESSION.get(url, params=params, headers=headers, timeout=15)
            if resp.ok:
                extracted = _extract_jsonld_listings(resp.text, "streeteasy", area_name)
                extracted += _extract_se_next_data(resp.text, area_name)
                results.extend(extracted)
            _delay(2, 4)
        except Exception as e:
            log.warning(f"    StreetEasy {area_slug} failed: {e}")
    return results


def _extract_se_next_data(html: str, area_name: str) -> list[dict]:
    results = []
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return results
    try:
        data = json.loads(m.group(1))
        listings = (
            data.get("props", {}).get("pageProps", {}).get("listings", []) or
            data.get("props", {}).get("pageProps", {}).get("searchResults", {}).get("listings", [])
        )
        for item in listings:
            price = _safe_int(item.get("price") or item.get("rent"))
            address = item.get("address") or item.get("building_address") or ""
            if not price or not address:
                continue
            amenities = [str(a).lower() for a in (item.get("amenities") or [])]
            results.append({
                "source": "streeteasy",
                "primary_url": f"https://streeteasy.com{item.get('url', '')}",
                "address": address,
                "unit": str(item.get("unit") or ""),
                "price": price,
                "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
                "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
                "sqft": _safe_int(item.get("sqft") or item.get("square_footage")),
                "floor": _safe_int(item.get("floor")),
                "in_unit_laundry": any(k in amenities for k in ["washer/dryer", "laundry in unit", "w/d"]),
                "dishwasher": "dishwasher" in amenities,
                "parking": any(k in amenities for k in ["parking", "garage"]),
                "storage": "storage" in amenities,
                "gym": any(k in amenities for k in ["gym", "fitness"]),
                "description": item.get("description", "")[:2000],
                "available_date": item.get("available_at") or item.get("available_date"),
                "neighborhood": area_name,
                "scraped_at": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        log.debug(f"    SE __NEXT_DATA__ parse failed: {e}")
    return results


def fetch_zillow() -> list[dict]:
    results = []
    for search in ZILLOW_SEARCHES:
        try:
            headers = {**SESSION.headers, "Referer": "https://www.zillow.com/", "Accept": "application/json"}
            params = {
                "searchQueryState": json.dumps(search["searchQueryState"]),
                "wants": json.dumps({"cat1": ["listResults"]}),
                "requestId": 2,
            }
            resp = SESSION.get("https://www.zillow.com/search/GetSearchPageState.htm", params=params, headers=headers, timeout=20)
            if resp.ok:
                data = resp.json()
                items = data.get("cat1", {}).get("searchResults", {}).get("listResults", [])
                for item in items:
                    n = _normalize_zillow(item, search["label"])
                    if n:
                        results.append(n)
            _delay(2, 4)
        except Exception as e:
            log.warning(f"    Zillow {search['label']} failed: {e}")
    return results


def _normalize_zillow(item: dict, neighborhood: str) -> Optional[dict]:
    price = _safe_int(item.get("unformattedPrice")) or _parse_price(item.get("price", ""))
    if not price:
        return None
    url = item.get("detailUrl", "")
    if url and not url.startswith("http"):
        url = "https://www.zillow.com" + url
    return {
        "source": "zillow",
        "primary_url": url,
        "address": item.get("address", ""),
        "unit": item.get("unit", ""),
        "price": price,
        "bedrooms": _safe_int(item.get("beds")),
        "bathrooms": _safe_float(item.get("baths")),
        "sqft": _safe_int(item.get("area")),
        "floor": None,
        "in_unit_laundry": False, "dishwasher": False,
        "parking": False, "storage": False, "gym": False,
        "description": item.get("statusText", ""),
        "available_date": None,
        "neighborhood": neighborhood,
        "scraped_at": datetime.utcnow().isoformat(),
    }


def fetch_redfin() -> list[dict]:
    results = []
    try:
        headers = {**SESSION.headers, "Referer": "https://www.redfin.com/", "Accept": "*/*"}
        params = {
            "al": 1, "isRentals": "true", "min_beds": 2, "max_price": 8000,
            "region_id": 20274, "region_type": 6, "sf": "1,2,3,5,6,7",
            "start": 0, "count": 100, "v": 8,
        }
        resp = SESSION.get("https://www.redfin.com/stingray/api/gis", params=params, headers=headers, timeout=20)
        if resp.ok:
            text = resp.text
            if text.startswith("{}&&"):
                text = text[4:]
            data = json.loads(text)
            for item in data.get("payload", {}).get("homes", []):
                n = _normalize_redfin(item)
                if n:
                    results.append(n)
    except Exception as e:
        log.warning(f"    Redfin failed: {e}")
    return results


def _normalize_redfin(item: dict) -> Optional[dict]:
    price = _safe_int(item.get("priceInfo", {}).get("amount"))
    if not price:
        return None
    addr_info = item.get("addressInfo", {})
    address = f"{addr_info.get('street', '')} {addr_info.get('city', '')}".strip()
    return {
        "source": "redfin",
        "primary_url": f"https://www.redfin.com{item.get('url', '')}",
        "address": address,
        "unit": addr_info.get("unitNumber", ""),
        "price": price,
        "bedrooms": _safe_int(item.get("beds")),
        "bathrooms": _safe_float(item.get("baths")),
        "sqft": _safe_int(item.get("sqft")),
        "floor": None,
        "in_unit_laundry": False, "dishwasher": False,
        "parking": False, "storage": False, "gym": False,
        "description": "",
        "available_date": None,
        "neighborhood": "brooklyn",
        "scraped_at": datetime.utcnow().isoformat(),
    }


def _extract_jsonld_listings(html: str, source: str, neighborhood: str) -> list[dict]:
    results = []
    for m in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(m)
            items = data if isinstance(data, list) else [data]
            for item in items:
                price = _parse_price(str(item.get("price", "") or item.get("priceRange", "")))
                address = item.get("address", {})
                addr_str = address.get("streetAddress", "") if isinstance(address, dict) else str(address)
                if price and addr_str:
                    results.append({
                        "source": source, "primary_url": item.get("url", ""),
                        "address": addr_str, "unit": "", "price": price,
                        "bedrooms": None, "bathrooms": None, "sqft": None, "floor": None,
                        "in_unit_laundry": False, "dishwasher": False,
                        "parking": False, "storage": False, "gym": False,
                        "description": item.get("description", "")[:1000],
                        "available_date": None, "neighborhood": neighborhood,
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
        except Exception:
            continue
    return results


def _parse_price(s: str) -> Optional[int]:
    if not s:
        return None
    for n in re.findall(r"[\d,]+", str(s)):
        val = int(n.replace(",", ""))
        if 500 < val <= 20000:
            return val
    return None

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

def _delay(lo: float, hi: float):
    time.sleep(random.uniform(lo, hi))
