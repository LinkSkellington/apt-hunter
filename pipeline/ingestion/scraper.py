"""
ingestion/scraper.py

Headless Playwright scraper for:
  - StreetEasy  (primary, best NYC coverage)
  - Zillow      (secondary, catches additional listings)
  - Redfin      (tertiary, catches stragglers)

Anti-detection strategy:
  - Randomized delays between requests
  - Realistic viewport + user-agent
  - Stealth mode via playwright-stealth
  - Rotates through a small set of realistic UA strings

If a source fails entirely, the run continues with partial data.
"""

import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

# ── Search targets ─────────────────────────────────────────────────────────────

STREETEASY_SEARCHES = [
    "https://streeteasy.com/for-rent/brooklyn-heights?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/williamsburg?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/cobble-hill?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/red-hook?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/greenpoint?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/park-slope?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/dumbo?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/carroll-gardens?price=-8000&beds=2&baths=2&size=1200",
    "https://streeteasy.com/for-rent/boerum-hill?price=-8000&beds=2&baths=2&size=1200",
]

ZILLOW_SEARCHES = [
    # Pre-filtered Zillow rental URLs for Brooklyn + Hoboken, 2+ beds, $0-8000
    "https://www.zillow.com/brooklyn-ny/rentals/?searchQueryState=%7B%22filterState%22%3A%7B%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22max%22%3A8000%7D%2C%22beds%22%3A%7B%22min%22%3A2%7D%7D%7D",
    "https://www.zillow.com/hoboken-nj/rentals/?searchQueryState=%7B%22filterState%22%3A%7B%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22max%22%3A8000%7D%2C%22beds%22%3A%7B%22min%22%3A2%7D%7D%7D",
]

REDFIN_SEARCHES = [
    "https://www.redfin.com/city/301/NY/Brooklyn/filter/property-type=apartment,min-beds=2,max-price=8000/apartments-for-rent",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


# ── Public entry point ─────────────────────────────────────────────────────────

def fetch_all_sources(sources=None) -> list[dict]:
    """
    Fetch and normalize listings from all configured sources.
    Returns a flat list of normalized listing dicts.
    """
    targets = sources or ["streeteasy", "zillow", "redfin"]
    all_listings = []

    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import stealth_sync
    except ImportError:
        log.error("playwright or playwright-stealth not installed. Run: pip install playwright playwright-stealth && playwright install chromium")
        return []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        for source in targets:
            try:
                log.info(f"  Scraping {source}...")
                context = browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent=random.choice(USER_AGENTS),
                    locale="en-US",
                    timezone_id="America/New_York",
                )
                page = context.new_page()
                stealth_sync(page)

                if source == "streeteasy":
                    listings = scrape_streeteasy(page, STREETEASY_SEARCHES)
                elif source == "zillow":
                    listings = scrape_zillow(page, ZILLOW_SEARCHES)
                elif source == "redfin":
                    listings = scrape_redfin(page, REDFIN_SEARCHES)
                else:
                    listings = []

                context.close()
                log.info(f"  → {len(listings)} listings from {source}")
                all_listings.extend(listings)

            except Exception as e:
                log.error(f"  Source {source} failed: {e}")
                try:
                    context.close()
                except Exception:
                    pass

        browser.close()

    return all_listings


# ── StreetEasy scraper ─────────────────────────────────────────────────────────

def scrape_streeteasy(page, urls: list) -> list[dict]:
    results = []
    for url in urls:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            _random_delay(2, 4)
            page.wait_for_selector('[data-testid="listing-card"], .listing-item, article', timeout=10000)

            # Scroll to trigger lazy loading
            for _ in range(3):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                _random_delay(0.8, 1.5)

            # Extract structured data from JSON-LD if available
            json_ld = page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    return Array.from(scripts).map(s => {
                        try { return JSON.parse(s.textContent); } catch { return null; }
                    }).filter(Boolean);
                }
            """)

            # Extract from listing cards
            cards = page.query_selector_all('[data-testid="listing-card"], .listingCard, [class*="ListingCard"]')
            for card in cards:
                raw = extract_streeteasy_card(card, page)
                if raw:
                    normalized = normalize_streeteasy(raw)
                    if normalized:
                        results.append(normalized)

            # Paginate once (page 2)
            next_btn = page.query_selector('a[rel="next"], [data-testid="next-page"]')
            if next_btn:
                _random_delay(2, 4)
                next_btn.click()
                page.wait_for_load_state("domcontentloaded")
                _random_delay(2, 3)
                cards2 = page.query_selector_all('[data-testid="listing-card"], .listingCard, [class*="ListingCard"]')
                for card in cards2:
                    raw = extract_streeteasy_card(card, page)
                    if raw:
                        normalized = normalize_streeteasy(raw)
                        if normalized:
                            results.append(normalized)

            _random_delay(3, 6)

        except Exception as e:
            log.warning(f"    StreetEasy URL failed ({url[:60]}...): {e}")

    return results


def extract_streeteasy_card(card, page) -> Optional[dict]:
    """Extract raw fields from a StreetEasy listing card element."""
    try:
        text = card.inner_text()
        href = ""
        link = card.query_selector("a")
        if link:
            href = link.get_attribute("href") or ""
            if href and not href.startswith("http"):
                href = "https://streeteasy.com" + href

        return {
            "url": href,
            "raw_text": text,
            "html": card.inner_html(),
        }
    except Exception:
        return None


def normalize_streeteasy(raw: dict) -> Optional[dict]:
    """Parse StreetEasy card text into structured fields."""
    text = raw.get("raw_text", "")
    url = raw.get("url", "")

    price = _extract_price(text)
    if not price:
        return None

    beds = _extract_beds(text)
    baths = _extract_baths(text)
    sqft = _extract_sqft(text)
    address, unit = _extract_address_unit(text, url)

    return {
        "source": "streeteasy",
        "primary_url": url,
        "address": address,
        "unit": unit,
        "price": price,
        "bedrooms": beds,
        "bathrooms": baths,
        "sqft": sqft,
        "floor": _extract_floor(text, unit),
        "in_unit_laundry": _has_feature(text, ["washer/dryer", "in-unit laundry", "laundry in unit", "w/d in unit"]),
        "dishwasher": _has_feature(text, ["dishwasher"]),
        "parking": _has_feature(text, ["parking", "garage"]),
        "storage": _has_feature(text, ["storage"]),
        "gym": _has_feature(text, ["gym", "fitness", "health club"]),
        "description": text[:2000],
        "available_date": _extract_date(text),
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ── Zillow scraper ─────────────────────────────────────────────────────────────

def scrape_zillow(page, urls: list) -> list[dict]:
    results = []
    for url in urls:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            _random_delay(3, 5)

            # Zillow loads listings into a JSON store on the page
            # Extract from __NEXT_DATA__ or window.__data
            data_json = page.evaluate("""
                () => {
                    try {
                        const el = document.getElementById('__NEXT_DATA__');
                        if (el) return JSON.parse(el.textContent);
                    } catch {}
                    return null;
                }
            """)

            if data_json:
                listings = _extract_zillow_json(data_json)
                results.extend(listings)
            else:
                # Fallback: scrape visible cards
                page.wait_for_selector('[data-test="property-card"], article[class*="StyledCard"]', timeout=10000)
                cards = page.query_selector_all('[data-test="property-card"]')
                for card in cards:
                    raw = normalize_zillow_card(card)
                    if raw:
                        results.append(raw)

            _random_delay(4, 7)

        except Exception as e:
            log.warning(f"    Zillow URL failed: {e}")

    return results


def _extract_zillow_json(data: dict) -> list[dict]:
    """Extract listings from Zillow's __NEXT_DATA__ JSON structure."""
    results = []
    try:
        # Path varies — walk common paths
        props = (
            data.get("props", {})
                .get("pageProps", {})
                .get("searchPageState", {})
                .get("cat1", {})
                .get("searchResults", {})
                .get("listResults", [])
        )
        for p in props:
            try:
                price = _parse_price_str(p.get("price", "") or p.get("formattedPrice", ""))
                if not price:
                    price = p.get("unformattedPrice")
                addr = p.get("address", "") or p.get("streetAddress", "")
                beds = _safe_int(p.get("beds"))
                baths = _safe_float(p.get("baths"))
                sqft = _safe_int(p.get("area") or p.get("livingArea"))
                url = p.get("detailUrl", "")
                if url and not url.startswith("http"):
                    url = "https://www.zillow.com" + url

                if not price or not addr:
                    continue

                results.append({
                    "source": "zillow",
                    "primary_url": url,
                    "address": addr,
                    "unit": p.get("unit", ""),
                    "price": price,
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "sqft": sqft,
                    "floor": None,
                    "in_unit_laundry": False,
                    "dishwasher": False,
                    "parking": False,
                    "storage": False,
                    "gym": False,
                    "description": p.get("statusText", "") or "",
                    "available_date": None,
                    "scraped_at": datetime.utcnow().isoformat(),
                })
            except Exception:
                continue
    except Exception as e:
        log.debug(f"    Zillow JSON parse error: {e}")
    return results


def normalize_zillow_card(card) -> Optional[dict]:
    """Fallback: parse visible Zillow card HTML."""
    try:
        text = card.inner_text()
        href = ""
        link = card.query_selector("a")
        if link:
            href = link.get_attribute("href") or ""
            if not href.startswith("http"):
                href = "https://www.zillow.com" + href

        price = _extract_price(text)
        if not price:
            return None

        return {
            "source": "zillow",
            "primary_url": href,
            "address": _extract_address_from_text(text),
            "unit": "",
            "price": price,
            "bedrooms": _extract_beds(text),
            "bathrooms": _extract_baths(text),
            "sqft": _extract_sqft(text),
            "floor": None,
            "in_unit_laundry": False,
            "dishwasher": False,
            "parking": False,
            "storage": False,
            "gym": False,
            "description": text[:1000],
            "available_date": None,
            "scraped_at": datetime.utcnow().isoformat(),
        }
    except Exception:
        return None


# ── Redfin scraper ─────────────────────────────────────────────────────────────

def scrape_redfin(page, urls: list) -> list[dict]:
    results = []
    for url in urls:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            _random_delay(2, 4)
            page.wait_for_selector(".HomeCardContainer, [class*='HomeCard']", timeout=10000)

            cards = page.query_selector_all(".HomeCardContainer, [class*='HomeCard']")
            for card in cards:
                try:
                    text = card.inner_text()
                    href = ""
                    link = card.query_selector("a")
                    if link:
                        href = link.get_attribute("href") or ""
                        if href and not href.startswith("http"):
                            href = "https://www.redfin.com" + href

                    price = _extract_price(text)
                    if not price:
                        continue

                    results.append({
                        "source": "redfin",
                        "primary_url": href,
                        "address": _extract_address_from_text(text),
                        "unit": "",
                        "price": price,
                        "bedrooms": _extract_beds(text),
                        "bathrooms": _extract_baths(text),
                        "sqft": _extract_sqft(text),
                        "floor": None,
                        "in_unit_laundry": _has_feature(text, ["washer", "laundry"]),
                        "dishwasher": _has_feature(text, ["dishwasher"]),
                        "parking": _has_feature(text, ["parking"]),
                        "storage": _has_feature(text, ["storage"]),
                        "gym": _has_feature(text, ["gym", "fitness"]),
                        "description": text[:1500],
                        "available_date": _extract_date(text),
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
                except Exception:
                    continue

            _random_delay(3, 5)

        except Exception as e:
            log.warning(f"    Redfin URL failed: {e}")

    return results


# ── Field extraction helpers ───────────────────────────────────────────────────

def _extract_price(text: str) -> Optional[int]:
    matches = re.findall(r"\$[\s]?([\d,]+)(?:/mo|/month|\s*per\s*month)?", text, re.IGNORECASE)
    for m in matches:
        val = int(m.replace(",", ""))
        if 500 < val <= 20000:
            return val
    return None


def _parse_price_str(s: str) -> Optional[int]:
    if not s:
        return None
    nums = re.findall(r"[\d,]+", str(s))
    for n in nums:
        val = int(n.replace(",", ""))
        if 500 < val <= 20000:
            return val
    return None


def _extract_beds(text: str) -> Optional[int]:
    m = re.search(r"(\d+)\s*(?:bed|bd|br|bedroom)", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    if re.search(r"\bstudio\b", text, re.IGNORECASE):
        return 0
    return None


def _extract_baths(text: str) -> Optional[float]:
    m = re.search(r"([\d.]+)\s*(?:bath|ba|bathroom)", text, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def _extract_sqft(text: str) -> Optional[int]:
    m = re.search(r"([\d,]+)\s*(?:sq\.?\s*ft\.?|sqft|square\s*feet)", text, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def _extract_floor(text: str, unit: str) -> Optional[int]:
    # Try unit number as floor hint (e.g. "4A" → floor 4)
    if unit:
        m = re.match(r"^(\d+)", str(unit))
        if m:
            n = int(m.group(1))
            if 1 <= n <= 80:
                return n
    m = re.search(r"(\d+)(?:st|nd|rd|th)\s*floor", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _extract_address_unit(text: str, url: str) -> tuple[str, str]:
    # Try to parse from URL first (streeteasy URLs often contain address)
    url_match = re.search(r"/rental/(\d+-[a-z0-9-]+)/", url or "")
    if url_match:
        slug = url_match.group(1).replace("-", " ").title()
        return slug, ""

    return _extract_address_from_text(text), ""


def _extract_address_from_text(text: str) -> str:
    # Simple heuristic: first line that looks like a street address
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    for line in lines[:5]:
        if re.search(r"\d+\s+\w+\s+(St|Street|Ave|Avenue|Blvd|Road|Dr|Pl|Lane|Way|Terrace)", line, re.IGNORECASE):
            return line
    return lines[0] if lines else ""


def _extract_date(text: str) -> Optional[str]:
    m = re.search(
        r"available\s+(?:on\s+|from\s+|starting\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s+\d{4})?|\d{1,2}/\d{1,2}(?:/\d{2,4})?)",
        text,
        re.IGNORECASE,
    )
    return m.group(1) if m else None


def _has_feature(text: str, keywords: list) -> bool:
    t = text.lower()
    return any(kw in t for kw in keywords)


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


def _random_delay(lo: float, hi: float):
    time.sleep(random.uniform(lo, hi))
