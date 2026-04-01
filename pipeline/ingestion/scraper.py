"""
ingestion/scraper.py

HTTP-based scraper targeting:
  - Corcoran         (corcoran.com)
  - Compass          (compass.com)
  - Douglas Elliman  (elliman.com)
  - Brown Harris Stevens (bhsusa.com)
  - Sotheby's NY     (sothebysrealty.com)
  - Direct building sites (extendable list)
  - StreetEasy       (JSON/HTML fallback)
  - Zillow           (JSON API)
  - Redfin           (JSON API)

All sites use HTTP requests only — no browser needed.
Add new building URLs to DIRECT_BUILDING_URLS at the bottom.
"""

import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# ── Session with retry logic ──────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = _make_session()

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

HEADERS_JSON = {
    **HEADERS_BROWSER,
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# ── Criteria constants ────────────────────────────────────────────────────────
MAX_PRICE  = 8000
MIN_BEDS   = 2
MIN_BATHS  = 2.0
MIN_SQFT   = 1200

# ── Neighborhood slugs for StreetEasy ────────────────────────────────────────
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

# ── Direct building/development websites ─────────────────────────────────────
# Format: (url, neighborhood, building_name)
# Add any building or development site here — the generic scraper will
# extract price/bed/bath from JSON-LD or visible text automatically.
DIRECT_BUILDING_URLS = [
    # Williamsburg
    ("https://www.theoostenwilliamsburg.com/availability", "williamsburg", "The Oosten"),
    ("https://www.northsidepiers.com/availability",        "williamsburg", "Northside Piers"),
    ("https://www.viawilliamsburg.com/apartments",         "williamsburg", "Via Williamsburg"),
    # Dumbo / Brooklyn Heights
    ("https://www.onebrooklynbridge.com/availability",     "brooklyn heights", "One Brooklyn Bridge Park"),
    ("https://www.1johnstreet.com/residences",             "dumbo",            "1 John Street"),
    ("https://www.clocktowerbuilding.com/rentals",         "dumbo",            "Clock Tower"),
    # Greenpoint
    ("https://www.greenpointhouseofdetention.com/rentals", "greenpoint",       "Greenpoint Landing"),
    ("https://www.greenpointlanding.com/availability",     "greenpoint",       "Greenpoint Landing"),
    # Hoboken / Weehawken / Jersey City
    ("https://www.maxwell-place.com/floor-plans",          "hoboken",          "Maxwell Place"),
    ("https://www.urthhoboken.com/availability",           "hoboken",          "Urth Hoboken"),
    ("https://hudsoncondos.com/rentals",                   "hoboken",          "Hudson Condos"),
    ("https://search.hudsoncondos.com/idx/search/listings?property_type=Residential+Lease&min_beds=2&max_price=8000", "hoboken", "Hudson Condos IDX"),
    # Red Hook / Columbia St Waterfront
    ("https://www.portlandave.com/availability",           "red hook",         "Portland Ave"),
]

# ── NJ-focused brokerage search URLs (scraped as direct sites) ────────────────
# These are added to DIRECT_BUILDING_URLS so they run through the generic extractor.
# Hudson Realty Group — Hoboken rentals
DIRECT_BUILDING_URLS += [
    ("https://hudsonrealtygroup.com/home-search/listings/lp/hoboken-rentals?property_type=Residential+Lease&min_bedrooms=2&max_price=8000", "hoboken", "Hudson Realty Group"),
    ("https://hudsonrealtygroup.com/home-search/listings?property_type=Residential+Lease&city=Hoboken&min_bedrooms=2&max_price=8000", "hoboken", "Hudson Realty Group"),
]


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_all_sources(sources=None) -> list[dict]:
    """
    Run all scrapers. sources can be a list to limit which run.
    Returns flat list of normalized listing dicts.
    """
    all_targets = {
        "corcoran":         fetch_corcoran,
        "compass":          fetch_compass,
        "elliman":          fetch_elliman,
        "bhs":              fetch_bhs,
        "sothebys":         fetch_sothebys,
        "halstead":         fetch_halstead,
        "bond":             fetch_bond,
        "nestseekers":      fetch_nestseekers,
        "level":            fetch_level,
        "buildings":        fetch_direct_buildings,
        "streeteasy":       fetch_streeteasy,
        "zillow":           fetch_zillow,
        "redfin":           fetch_redfin,
    }
    targets = sources or list(all_targets.keys())
    all_listings = []

    for source in targets:
        fn = all_targets.get(source)
        if not fn:
            log.warning(f"  Unknown source: {source}")
            continue
        try:
            log.info(f"  Fetching from {source}...")
            listings = fn()
            log.info(f"  -> {len(listings)} listings from {source}")
            all_listings.extend(listings)
        except Exception as e:
            log.error(f"  Source {source} failed: {e}")

    return all_listings


# ── Corcoran ──────────────────────────────────────────────────────────────────

def fetch_corcoran() -> list[dict]:
    """
    Corcoran exposes a JSON search API used by their website.
    Endpoint discovered via browser network inspection.
    """
    results = []
    neighborhoods = [
        "Brooklyn Heights", "Williamsburg", "Cobble Hill", "Red Hook",
        "Greenpoint", "Park Slope", "DUMBO", "Carroll Gardens", "Boerum Hill",
    ]
    for neigh in neighborhoods:
        try:
            url = "https://www.corcoran.com/api/search/listings"
            payload = {
                "searchType": "rentals",
                "neighborhoods": [neigh],
                "bedrooms": {"min": MIN_BEDS},
                "bathrooms": {"min": MIN_BATHS},
                "price": {"max": MAX_PRICE},
                "squareFeet": {"min": MIN_SQFT},
                "borough": "Brooklyn",
                "sortBy": "listingDate",
                "sortOrder": "desc",
                "page": 1,
                "pageSize": 50,
            }
            headers = {
                **HEADERS_JSON,
                "Referer": "https://www.corcoran.com/",
                "Origin": "https://www.corcoran.com",
            }
            resp = SESSION.post(url, json=payload, headers=headers, timeout=15)
            if resp.ok and "application/json" in resp.headers.get("content-type", ""):
                data = resp.json()
                listings = data.get("listings") or data.get("results") or data.get("data") or []
                for item in listings:
                    n = _normalize_corcoran(item, neigh.lower())
                    if n:
                        results.append(n)
            else:
                # Fallback: scrape the search results page
                page_url = f"https://www.corcoran.com/homes-for-rent/in-{neigh.lower().replace(' ', '-')}/price_max-8000/beds_min-2/baths_min-2"
                resp2 = SESSION.get(page_url, headers=HEADERS_BROWSER, timeout=15)
                if resp2.ok:
                    results.extend(_extract_jsonld_listings(resp2.text, "corcoran", neigh.lower()))
                    results.extend(_extract_next_data(resp2.text, "corcoran", neigh.lower()))
            _delay(1, 3)
        except Exception as e:
            log.warning(f"    Corcoran {neigh} failed: {e}")
    return results


def _normalize_corcoran(item: dict, neighborhood: str) -> Optional[dict]:
    price = _safe_int(item.get("price") or item.get("listPrice") or item.get("rentalPrice"))
    if not price:
        return None
    address = (
        item.get("address", {}).get("full") or
        item.get("address", {}).get("street") or
        item.get("streetAddress") or ""
    )
    if not address:
        return None
    url = item.get("url") or item.get("listingUrl") or ""
    if url and not url.startswith("http"):
        url = "https://www.corcoran.com" + url
    amenities = [str(a).lower() for a in (item.get("amenities") or [])]
    return {
        "source": "corcoran",
        "primary_url": url,
        "address": address,
        "unit": str(item.get("unit") or item.get("unitNumber") or ""),
        "price": price,
        "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
        "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
        "sqft": _safe_int(item.get("squareFeet") or item.get("sqft")),
        "floor": _safe_int(item.get("floor")),
        "in_unit_laundry": _has(amenities, ["washer", "laundry in unit", "w/d"]),
        "dishwasher":       _has(amenities, ["dishwasher"]),
        "parking":          _has(amenities, ["parking", "garage"]),
        "storage":          _has(amenities, ["storage"]),
        "gym":              _has(amenities, ["gym", "fitness"]),
        "description": (item.get("description") or "")[:2000],
        "available_date": item.get("availableDate") or item.get("availableOn"),
        "neighborhood": neighborhood,
        "building_name": item.get("buildingName") or item.get("building", {}).get("name", ""),
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Compass ───────────────────────────────────────────────────────────────────

def fetch_compass() -> list[dict]:
    results = []
    # Compass GraphQL endpoint used by their website
    neighborhoods_ids = {
        "brooklyn heights": "5a87f54b7d95824a060eb2a5",
        "williamsburg":     "5a87f5487d95824a060eb267",
        "cobble hill":      "5a87f5507d95824a060eb2e3",
        "park slope":       "5a87f5507d95824a060eb2e7",
        "greenpoint":       "5a87f5517d95824a060eb2f1",
        "dumbo":            "5a87f54c7d95824a060eb2b3",
    }
    for neigh_name, neigh_id in neighborhoods_ids.items():
        try:
            # Try the Compass search API
            url = "https://www.compass.com/api/v1/search/listings"
            params = {
                "q": neigh_name + " brooklyn",
                "type": "rental",
                "min_beds": MIN_BEDS,
                "max_price": MAX_PRICE,
                "min_sqft": MIN_SQFT,
                "sort": "newest",
                "page": 1,
                "limit": 50,
            }
            headers = {**HEADERS_JSON, "Referer": "https://www.compass.com/"}
            resp = SESSION.get(url, params=params, headers=headers, timeout=15)
            if resp.ok and "json" in resp.headers.get("content-type", ""):
                data = resp.json()
                listings = data.get("listings") or data.get("results") or []
                for item in listings:
                    n = _normalize_compass(item, neigh_name)
                    if n:
                        results.append(n)
            else:
                # Fallback: scrape HTML
                page_url = f"https://www.compass.com/for-rent/{neigh_name.replace(' ', '-')}-brooklyn-ny/price_max={MAX_PRICE}/beds_min={MIN_BEDS}/"
                resp2 = SESSION.get(page_url, headers=HEADERS_BROWSER, timeout=15)
                if resp2.ok:
                    results.extend(_extract_jsonld_listings(resp2.text, "compass", neigh_name))
                    results.extend(_extract_next_data(resp2.text, "compass", neigh_name))
            _delay(1, 3)
        except Exception as e:
            log.warning(f"    Compass {neigh_name} failed: {e}")
    return results


def _normalize_compass(item: dict, neighborhood: str) -> Optional[dict]:
    price = _safe_int(item.get("price") or item.get("listPrice"))
    if not price:
        return None
    address = item.get("address") or item.get("streetAddress") or ""
    url = item.get("url") or item.get("detailUrl") or ""
    if url and not url.startswith("http"):
        url = "https://www.compass.com" + url
    amenities = [str(a).lower() for a in (item.get("amenities") or [])]
    return {
        "source": "compass",
        "primary_url": url,
        "address": address,
        "unit": str(item.get("unit") or ""),
        "price": price,
        "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
        "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
        "sqft": _safe_int(item.get("squareFeet") or item.get("sqft") or item.get("livingArea")),
        "floor": _safe_int(item.get("floor")),
        "in_unit_laundry": _has(amenities, ["washer", "laundry"]),
        "dishwasher":       _has(amenities, ["dishwasher"]),
        "parking":          _has(amenities, ["parking", "garage"]),
        "storage":          _has(amenities, ["storage"]),
        "gym":              _has(amenities, ["gym", "fitness"]),
        "description": (item.get("description") or "")[:2000],
        "available_date": item.get("availableDate"),
        "neighborhood": neighborhood,
        "building_name": item.get("buildingName", ""),
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Douglas Elliman ───────────────────────────────────────────────────────────

def fetch_elliman() -> list[dict]:
    results = []
    search_urls = [
        ("https://www.elliman.com/new-york/rentals/search#location=Brooklyn+Heights,Brooklyn,NY&price_max=8000&bedrooms_min=2&bathrooms_min=2&sqft_min=1200", "brooklyn heights"),
        ("https://www.elliman.com/new-york/rentals/search#location=Williamsburg,Brooklyn,NY&price_max=8000&bedrooms_min=2", "williamsburg"),
        ("https://www.elliman.com/new-york/rentals/search#location=Cobble+Hill,Brooklyn,NY&price_max=8000&bedrooms_min=2", "cobble hill"),
        ("https://www.elliman.com/new-york/rentals/search#location=Park+Slope,Brooklyn,NY&price_max=8000&bedrooms_min=2", "park slope"),
        ("https://www.elliman.com/new-york/rentals/search#location=Hoboken,NJ&price_max=8000&bedrooms_min=2", "hoboken"),
    ]
    # Try Elliman's internal API first
    for page_url, neigh in search_urls:
        try:
            api_url = "https://www.elliman.com/api/search/listings"
            payload = {
                "listingType": "rental",
                "location": neigh,
                "priceMax": MAX_PRICE,
                "bedsMin": MIN_BEDS,
                "bathsMin": MIN_BATHS,
                "sqftMin": MIN_SQFT,
                "sortBy": "listDate",
                "page": 1,
                "pageSize": 48,
            }
            headers = {**HEADERS_JSON, "Referer": "https://www.elliman.com/"}
            resp = SESSION.post(api_url, json=payload, headers=headers, timeout=15)
            if resp.ok and "json" in resp.headers.get("content-type", ""):
                data = resp.json()
                listings = data.get("listings") or data.get("properties") or data.get("results") or []
                for item in listings:
                    n = _normalize_elliman(item, neigh)
                    if n:
                        results.append(n)
            else:
                resp2 = SESSION.get(page_url, headers=HEADERS_BROWSER, timeout=15)
                if resp2.ok:
                    results.extend(_extract_jsonld_listings(resp2.text, "elliman", neigh))
                    results.extend(_extract_next_data(resp2.text, "elliman", neigh))
            _delay(1, 3)
        except Exception as e:
            log.warning(f"    Elliman {neigh} failed: {e}")
    return results


def _normalize_elliman(item: dict, neighborhood: str) -> Optional[dict]:
    price = _safe_int(item.get("price") or item.get("listPrice") or item.get("rentalPrice"))
    if not price:
        return None
    address = item.get("address") or item.get("streetAddress") or item.get("displayAddress") or ""
    url = item.get("url") or item.get("listingUrl") or ""
    if url and not url.startswith("http"):
        url = "https://www.elliman.com" + url
    amenities = [str(a).lower() for a in (item.get("amenities") or [])]
    return {
        "source": "elliman",
        "primary_url": url,
        "address": address,
        "unit": str(item.get("unit") or item.get("aptNumber") or ""),
        "price": price,
        "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
        "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
        "sqft": _safe_int(item.get("squareFeet") or item.get("sqft")),
        "floor": _safe_int(item.get("floor")),
        "in_unit_laundry": _has(amenities, ["washer", "laundry"]),
        "dishwasher":       _has(amenities, ["dishwasher"]),
        "parking":          _has(amenities, ["parking", "garage"]),
        "storage":          _has(amenities, ["storage"]),
        "gym":              _has(amenities, ["gym", "fitness"]),
        "description": (item.get("description") or item.get("remarks") or "")[:2000],
        "available_date": item.get("availableDate") or item.get("availableOn"),
        "neighborhood": neighborhood,
        "building_name": item.get("buildingName") or item.get("building") or "",
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Brown Harris Stevens ──────────────────────────────────────────────────────

def fetch_bhs() -> list[dict]:
    results = []
    neighborhoods = [
        "Brooklyn Heights", "Williamsburg", "Cobble Hill",
        "Park Slope", "DUMBO", "Greenpoint", "Red Hook",
    ]
    for neigh in neighborhoods:
        try:
            # BHS search page with filters in URL
            url = f"https://www.bhsusa.com/rentals/search?neighborhood={neigh.replace(' ', '+')}&price_max={MAX_PRICE}&bedrooms_min={MIN_BEDS}&bathrooms_min={int(MIN_BATHS)}&borough=Brooklyn"
            resp = SESSION.get(url, headers=HEADERS_BROWSER, timeout=15)
            if resp.ok:
                results.extend(_extract_jsonld_listings(resp.text, "bhs", neigh.lower()))
                results.extend(_extract_next_data(resp.text, "bhs", neigh.lower()))
                results.extend(_extract_embedded_json(resp.text, "bhs", neigh.lower()))
            _delay(1, 2)
        except Exception as e:
            log.warning(f"    BHS {neigh} failed: {e}")
    return results


# ── Sotheby's International Realty NY ────────────────────────────────────────

def fetch_sothebys() -> list[dict]:
    results = []
    try:
        url = "https://www.sothebysrealty.com/eng/rentals/new-york-city-new-york-usa"
        params = {
            "pr": f"0-{MAX_PRICE}",
            "bd": f"{MIN_BEDS}-",
            "ba": f"{int(MIN_BATHS)}-",
            "sf": f"{MIN_SQFT}-",
        }
        resp = SESSION.get(url, params=params, headers=HEADERS_BROWSER, timeout=15)
        if resp.ok:
            results.extend(_extract_jsonld_listings(resp.text, "sothebys", "brooklyn"))
            results.extend(_extract_next_data(resp.text, "sothebys", "brooklyn"))
    except Exception as e:
        log.warning(f"    Sotheby's failed: {e}")
    return results


# ── Halstead (now Brown Harris Stevens affiliate) ────────────────────────────

def fetch_halstead() -> list[dict]:
    results = []
    searches = [
        ("brooklyn-heights", "brooklyn heights"),
        ("williamsburg",     "williamsburg"),
        ("cobble-hill",      "cobble hill"),
        ("park-slope",       "park slope"),
        ("dumbo",            "dumbo"),
        ("hoboken",          "hoboken"),
    ]
    for slug, neigh in searches:
        try:
            url = f"https://www.halstead.com/rent/ny/brooklyn/{slug}/?beds_min={MIN_BEDS}&baths_min={int(MIN_BATHS)}&price_max={MAX_PRICE}&sqft_min={MIN_SQFT}"
            resp = SESSION.get(url, headers=HEADERS_BROWSER, timeout=15)
            if resp.ok:
                results.extend(_extract_jsonld_listings(resp.text, "halstead", neigh))
                results.extend(_extract_next_data(resp.text, "halstead", neigh))
                results.extend(_extract_embedded_json(resp.text, "halstead", neigh))
            _delay(1, 2)
        except Exception as e:
            log.warning(f"    Halstead {slug} failed: {e}")
    return results


# ── BOND New York ─────────────────────────────────────────────────────────────

def fetch_bond() -> list[dict]:
    results = []
    try:
        # BOND has a JSON API for their search
        url = "https://www.bondnewyork.com/api/listings/search"
        payload = {
            "listingType": "rental",
            "neighborhoods": ["Brooklyn Heights", "Williamsburg", "Cobble Hill", "Park Slope", "Hoboken"],
            "minBeds": MIN_BEDS,
            "minBaths": MIN_BATHS,
            "maxPrice": MAX_PRICE,
            "minSqft": MIN_SQFT,
            "page": 1,
            "perPage": 50,
        }
        headers = {**HEADERS_JSON, "Referer": "https://www.bondnewyork.com/"}
        resp = SESSION.post(url, json=payload, headers=headers, timeout=15)
        if resp.ok and "json" in resp.headers.get("content-type", ""):
            data = resp.json()
            for item in (data.get("listings") or data.get("results") or []):
                price = _safe_int(item.get("price") or item.get("rent"))
                address = item.get("address") or item.get("streetAddress") or ""
                if not price or not address:
                    continue
                listing_url = item.get("url") or item.get("listingUrl") or ""
                if listing_url and not listing_url.startswith("http"):
                    listing_url = "https://www.bondnewyork.com" + listing_url
                amenities = [str(a).lower() for a in (item.get("amenities") or [])]
                results.append({
                    "source": "bond",
                    "primary_url": listing_url,
                    "address": address,
                    "unit": str(item.get("unit") or ""),
                    "price": price,
                    "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
                    "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
                    "sqft": _safe_int(item.get("sqft") or item.get("squareFeet")),
                    "floor": _safe_int(item.get("floor")),
                    "in_unit_laundry": _has(amenities, ["washer", "laundry"]),
                    "dishwasher":       _has(amenities, ["dishwasher"]),
                    "parking":          _has(amenities, ["parking", "garage"]),
                    "storage":          _has(amenities, ["storage"]),
                    "gym":              _has(amenities, ["gym", "fitness"]),
                    "description": (item.get("description") or "")[:2000],
                    "available_date": item.get("availableDate"),
                    "neighborhood": (item.get("neighborhood") or "brooklyn").lower(),
                    "scraped_at": datetime.utcnow().isoformat(),
                })
        else:
            # Fallback: scrape HTML search pages
            for slug, neigh in [("brooklyn-heights", "brooklyn heights"), ("williamsburg", "williamsburg"), ("hoboken-nj", "hoboken")]:
                try:
                    page_url = f"https://www.bondnewyork.com/rent/{slug}/?beds_min={MIN_BEDS}&price_max={MAX_PRICE}"
                    r = SESSION.get(page_url, headers=HEADERS_BROWSER, timeout=15)
                    if r.ok:
                        results.extend(_extract_jsonld_listings(r.text, "bond", neigh))
                        results.extend(_extract_next_data(r.text, "bond", neigh))
                    _delay(1, 2)
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"    BOND failed: {e}")
    return results


# ── Nest Seekers International ────────────────────────────────────────────────

def fetch_nestseekers() -> list[dict]:
    results = []
    searches = [
        ("brooklyn-heights-brooklyn", "brooklyn heights"),
        ("williamsburg-brooklyn",     "williamsburg"),
        ("cobble-hill-brooklyn",      "cobble hill"),
        ("hoboken-new-jersey",        "hoboken"),
    ]
    for slug, neigh in searches:
        try:
            url = f"https://www.nestseekers.com/Rentals/{slug}?beds={MIN_BEDS}&bath={int(MIN_BATHS)}&pricemax={MAX_PRICE}"
            resp = SESSION.get(url, headers=HEADERS_BROWSER, timeout=15)
            if resp.ok:
                results.extend(_extract_jsonld_listings(resp.text, "nestseekers", neigh))
                results.extend(_extract_next_data(resp.text, "nestseekers", neigh))
                results.extend(_extract_embedded_json(resp.text, "nestseekers", neigh))
            _delay(1, 2)
        except Exception as e:
            log.warning(f"    Nest Seekers {slug} failed: {e}")
    return results


# ── Level Group (Brooklyn focused) ────────────────────────────────────────────

def fetch_level() -> list[dict]:
    results = []
    try:
        url = "https://www.levelgroup.com/rentals"
        params = {
            "neighborhood": "Brooklyn Heights,Williamsburg,Cobble Hill,Park Slope,DUMBO",
            "min_beds": MIN_BEDS,
            "min_baths": int(MIN_BATHS),
            "max_price": MAX_PRICE,
            "min_sqft": MIN_SQFT,
        }
        resp = SESSION.get(url, params=params, headers=HEADERS_BROWSER, timeout=15)
        if resp.ok:
            results.extend(_extract_jsonld_listings(resp.text, "level", "brooklyn"))
            results.extend(_extract_next_data(resp.text, "level", "brooklyn"))
            results.extend(_extract_embedded_json(resp.text, "level", "brooklyn"))
    except Exception as e:
        log.warning(f"    Level Group failed: {e}")
    return results


# ── Direct building websites ──────────────────────────────────────────────────

def fetch_direct_buildings() -> list[dict]:
    """
    Scrape individual building/development websites.
    Uses JSON-LD, __NEXT_DATA__, and text pattern extraction.
    Add new buildings to DIRECT_BUILDING_URLS at top of file.
    """
    results = []
    for url, neighborhood, building_name in DIRECT_BUILDING_URLS:
        try:
            resp = SESSION.get(url, headers=HEADERS_BROWSER, timeout=15)
            if not resp.ok:
                log.debug(f"    Building {building_name} returned {resp.status_code}")
                continue

            html = resp.text
            extracted = []
            extracted.extend(_extract_jsonld_listings(html, "direct", neighborhood))
            extracted.extend(_extract_next_data(html, "direct", neighborhood))
            extracted.extend(_extract_embedded_json(html, "direct", neighborhood))
            extracted.extend(_extract_text_listings(html, "direct", neighborhood))

            # Tag all with building name
            for item in extracted:
                item["building_name"] = building_name
                item["primary_url"] = item.get("primary_url") or url

            log.debug(f"    {building_name}: {len(extracted)} listings")
            results.extend(extracted)
            _delay(1, 2)

        except Exception as e:
            log.debug(f"    Building {building_name} failed: {e}")

    return results


# ── StreetEasy ────────────────────────────────────────────────────────────────

def fetch_streeteasy() -> list[dict]:
    results = []
    for area_slug, area_name in STREETEASY_AREAS:
        try:
            url = f"https://streeteasy.com/for-rent/{area_slug}"
            params = {"price": f"-{MAX_PRICE}", "beds": str(MIN_BEDS), "baths": str(int(MIN_BATHS)), "size": str(MIN_SQFT)}
            resp = SESSION.get(url, params=params, headers=HEADERS_BROWSER, timeout=15)
            if resp.ok:
                results.extend(_extract_next_data(resp.text, "streeteasy", area_name))
                results.extend(_extract_jsonld_listings(resp.text, "streeteasy", area_name))
            _delay(2, 4)
        except Exception as e:
            log.warning(f"    StreetEasy {area_slug} failed: {e}")
    return results


# ── Zillow ────────────────────────────────────────────────────────────────────

def fetch_zillow() -> list[dict]:
    results = []
    searches = [
        ("brooklyn", {
            "usersSearchTerm": "Brooklyn, NY",
            "mapBounds": {"west": -74.0479, "east": -73.8765, "south": 40.5771, "north": 40.7396},
        }),
        ("hoboken", {
            "usersSearchTerm": "Hoboken, NJ",
            "mapBounds": {"west": -74.0700, "east": -74.0100, "south": 40.7300, "north": 40.7700},
        }),
    ]
    for label, extra in searches:
        try:
            state = {
                **extra,
                "pagination": {},
                "filterState": {
                    "fr":   {"value": True}, "fsba": {"value": False}, "fsbo": {"value": False},
                    "nc":   {"value": False}, "cmsn": {"value": False}, "auc":  {"value": False},
                    "fore": {"value": False}, "mp":   {"max": MAX_PRICE}, "beds": {"min": MIN_BEDS},
                },
                "isListVisible": True, "isMapVisible": False,
            }
            params = {
                "searchQueryState": json.dumps(state),
                "wants": json.dumps({"cat1": ["listResults"]}),
                "requestId": 2,
            }
            headers = {**HEADERS_JSON, "Referer": "https://www.zillow.com/"}
            resp = SESSION.get("https://www.zillow.com/search/GetSearchPageState.htm", params=params, headers=headers, timeout=20)
            if resp.ok:
                items = resp.json().get("cat1", {}).get("searchResults", {}).get("listResults", [])
                for item in items:
                    n = _normalize_zillow(item, label)
                    if n:
                        results.append(n)
            _delay(2, 4)
        except Exception as e:
            log.warning(f"    Zillow {label} failed: {e}")
    return results


def _normalize_zillow(item: dict, neighborhood: str) -> Optional[dict]:
    price = _safe_int(item.get("unformattedPrice")) or _parse_price(item.get("price", ""))
    if not price:
        return None
    url = item.get("detailUrl", "")
    if url and not url.startswith("http"):
        url = "https://www.zillow.com" + url
    return {
        "source": "zillow", "primary_url": url,
        "address": item.get("address", ""), "unit": item.get("unit", ""),
        "price": price,
        "bedrooms": _safe_int(item.get("beds")),
        "bathrooms": _safe_float(item.get("baths")),
        "sqft": _safe_int(item.get("area")),
        "floor": None,
        "in_unit_laundry": False, "dishwasher": False,
        "parking": False, "storage": False, "gym": False,
        "description": item.get("statusText", ""),
        "available_date": None, "neighborhood": neighborhood,
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Redfin ────────────────────────────────────────────────────────────────────

def fetch_redfin() -> list[dict]:
    results = []
    try:
        headers = {**HEADERS_JSON, "Referer": "https://www.redfin.com/"}
        params = {
            "al": 1, "isRentals": "true", "min_beds": MIN_BEDS,
            "max_price": MAX_PRICE, "region_id": 20274, "region_type": 6,
            "sf": "1,2,3,5,6,7", "start": 0, "count": 100, "v": 8,
        }
        resp = SESSION.get("https://www.redfin.com/stingray/api/gis", params=params, headers=headers, timeout=20)
        if resp.ok:
            text = resp.text
            if text.startswith("{}&&"):
                text = text[4:]
            for item in json.loads(text).get("payload", {}).get("homes", []):
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
    addr = item.get("addressInfo", {})
    return {
        "source": "redfin",
        "primary_url": f"https://www.redfin.com{item.get('url', '')}",
        "address": f"{addr.get('street', '')} {addr.get('city', '')}".strip(),
        "unit": addr.get("unitNumber", ""), "price": price,
        "bedrooms": _safe_int(item.get("beds")),
        "bathrooms": _safe_float(item.get("baths")),
        "sqft": _safe_int(item.get("sqft")),
        "floor": None,
        "in_unit_laundry": False, "dishwasher": False,
        "parking": False, "storage": False, "gym": False,
        "description": "", "available_date": None,
        "neighborhood": "brooklyn",
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Generic extraction helpers ────────────────────────────────────────────────

def _extract_jsonld_listings(html: str, source: str, neighborhood: str) -> list[dict]:
    """Extract listings from JSON-LD structured data blocks."""
    results = []
    for m in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(m)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Apartment", "ApartmentComplex", "Residence", "Product", "RealEstateListing"):
                    continue
                price = _parse_price(str(item.get("price") or item.get("priceRange") or ""))
                address = item.get("address", {})
                addr_str = address.get("streetAddress", "") if isinstance(address, dict) else str(address)
                if not price or not addr_str:
                    continue
                results.append({
                    "source": source, "primary_url": item.get("url", ""),
                    "address": addr_str, "unit": "", "price": price,
                    "bedrooms": _safe_int(item.get("numberOfRooms")),
                    "bathrooms": None, "sqft": None, "floor": None,
                    "in_unit_laundry": False, "dishwasher": False,
                    "parking": False, "storage": False, "gym": False,
                    "description": (item.get("description") or "")[:1000],
                    "available_date": None, "neighborhood": neighborhood,
                    "scraped_at": datetime.utcnow().isoformat(),
                })
        except Exception:
            continue
    return results


def _extract_next_data(html: str, source: str, neighborhood: str) -> list[dict]:
    """Extract listings from Next.js __NEXT_DATA__ JSON blob."""
    results = []
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return results
    try:
        data = json.loads(m.group(1))
        # Walk common paths where listings appear
        candidates = []
        props = data.get("props", {}).get("pageProps", {})
        for key in ("listings", "searchResults", "results", "properties", "rentals"):
            val = props.get(key)
            if isinstance(val, list):
                candidates.extend(val)
            elif isinstance(val, dict):
                for subkey in ("listings", "results", "items"):
                    if isinstance(val.get(subkey), list):
                        candidates.extend(val[subkey])

        for item in candidates:
            price = _safe_int(item.get("price") or item.get("rent") or item.get("listPrice"))
            address = (
                item.get("address") or item.get("streetAddress") or
                item.get("building_address") or
                (item.get("location") or {}).get("address") or ""
            )
            if not price or not address:
                continue
            amenities = [str(a).lower() for a in (item.get("amenities") or [])]
            url = item.get("url") or item.get("listingUrl") or item.get("detailUrl") or ""
            results.append({
                "source": source, "primary_url": url,
                "address": str(address), "unit": str(item.get("unit") or ""),
                "price": price,
                "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
                "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
                "sqft": _safe_int(item.get("sqft") or item.get("squareFeet") or item.get("square_footage")),
                "floor": _safe_int(item.get("floor")),
                "in_unit_laundry": _has(amenities, ["washer", "laundry"]),
                "dishwasher":       _has(amenities, ["dishwasher"]),
                "parking":          _has(amenities, ["parking", "garage"]),
                "storage":          _has(amenities, ["storage"]),
                "gym":              _has(amenities, ["gym", "fitness"]),
                "description": (item.get("description") or "")[:2000],
                "available_date": item.get("availableDate") or item.get("available_at"),
                "neighborhood": neighborhood,
                "scraped_at": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        log.debug(f"    __NEXT_DATA__ parse failed for {source}/{neighborhood}: {e}")
    return results


def _extract_embedded_json(html: str, source: str, neighborhood: str) -> list[dict]:
    """
    Find JSON arrays embedded in <script> tags (common on property sites).
    Looks for patterns like: window.__data = [...] or var listings = [...]
    """
    results = []
    patterns = [
        r'window\.__(?:data|listings|state|props)\s*=\s*(\{.*?\});',
        r'var\s+(?:listings|properties|rentals|units)\s*=\s*(\[.*?\]);',
        r'"listings"\s*:\s*(\[.*?\])',
        r'"units"\s*:\s*(\[.*?\])',
        r'"availableUnits"\s*:\s*(\[.*?\])',
        r'"floorPlans"\s*:\s*(\[.*?\])',
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, html, re.DOTALL):
            try:
                data = json.loads(m.group(1))
                items = data if isinstance(data, list) else data.get("listings") or data.get("units") or []
                for item in items:
                    price = _safe_int(item.get("price") or item.get("rent") or item.get("monthlyRent"))
                    address = item.get("address") or item.get("streetAddress") or ""
                    if not price:
                        continue
                    results.append({
                        "source": source, "primary_url": item.get("url", ""),
                        "address": str(address), "unit": str(item.get("unit") or item.get("unitNumber") or ""),
                        "price": price,
                        "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
                        "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
                        "sqft": _safe_int(item.get("sqft") or item.get("squareFeet")),
                        "floor": _safe_int(item.get("floor")),
                        "in_unit_laundry": False, "dishwasher": False,
                        "parking": False, "storage": False, "gym": False,
                        "description": (item.get("description") or "")[:1000],
                        "available_date": item.get("availableDate"),
                        "neighborhood": neighborhood,
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
            except Exception:
                continue
    return results


def _extract_text_listings(html: str, source: str, neighborhood: str) -> list[dict]:
    """
    Last-resort: scan raw HTML text for price + address patterns.
    Catches building sites that don't use structured data.
    """
    results = []
    # Find price mentions
    price_blocks = re.finditer(r'\$\s*([\d,]+)\s*/\s*(?:mo|month)', html, re.IGNORECASE)
    for pm in price_blocks:
        price = _safe_int(pm.group(1).replace(",", ""))
        if not price or price > MAX_PRICE or price < 500:
            continue
        # Look for bed/bath near the price mention
        context = html[max(0, pm.start()-300):pm.end()+300]
        beds = _safe_int((re.search(r'(\d+)\s*(?:bed|bd|BR)', context, re.IGNORECASE) or type('', (), {'group': lambda *a: None})()).group(1))
        baths = _safe_float((re.search(r'([\d.]+)\s*(?:bath|ba)', context, re.IGNORECASE) or type('', (), {'group': lambda *a: None})()).group(1))
        sqft = _safe_int((re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft)', context, re.IGNORECASE) or type('', (), {'group': lambda *a: None})()).group(1))
        if price and (beds is None or beds >= MIN_BEDS):
            results.append({
                "source": source, "primary_url": "",
                "address": "", "unit": "", "price": price,
                "bedrooms": beds, "bathrooms": baths, "sqft": sqft,
                "floor": None,
                "in_unit_laundry": False, "dishwasher": False,
                "parking": False, "storage": False, "gym": False,
                "description": re.sub(r'<[^>]+>', ' ', context)[:500],
                "available_date": None, "neighborhood": neighborhood,
                "scraped_at": datetime.utcnow().isoformat(),
            })
    return results


# ── Shared utilities ──────────────────────────────────────────────────────────

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

def _has(amenities: list, keywords: list) -> bool:
    return any(kw in a for a in amenities for kw in keywords)

def _delay(lo: float, hi: float):
    time.sleep(random.uniform(lo, hi))
