"""
storage/supabase_client.py

All database operations against Supabase (PostgreSQL via REST API).
Uses the supabase-py client — no raw SQL needed.

Table: listings (see schema in supabase_schema.sql)
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]          # https://xxxx.supabase.co
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service_role key (server-only)

TABLE = "listings"


def _client():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


class SupabaseClient:
    def __init__(self):
        self._sb = _client()

    # ── Reads ──────────────────────────────────────────────────────────────────

    def get_all_listings(self) -> list[dict]:
        """
        Fetch all non-rejected listings for deduplication lookup.
        Only pulls the fields needed for dedupe comparison.
        """
        resp = (
            self._sb.table(TABLE)
            .select(
                "id, dedupe_key, address_normalized, unit, bedrooms, bathrooms, sqft, "
                "price, price_min_seen, price_max_seen, sources, source_urls, "
                "status, first_seen, last_seen, score_tier, heat, neighborhood"
            )
            .neq("status", "rejected")
            .execute()
        )
        return resp.data or []

    def get_dashboard_listings(
        self,
        status_exclude: Optional[list] = None,
        tier_exclude: Optional[list] = None,
        limit: int = 500,
    ) -> list[dict]:
        """Used by the dashboard API endpoint."""
        q = self._sb.table(TABLE).select("*").order("score_raw", desc=True).limit(limit)
        if status_exclude:
            for s in status_exclude:
                q = q.neq("status", s)
        if tier_exclude:
            for t in tier_exclude:
                q = q.neq("score_tier", t)
        return q.execute().data or []

    # ── Writes ─────────────────────────────────────────────────────────────────

    def upsert_listings(self, listings: list[dict]):
        """Insert new listings. Skips if dedupe_key already exists."""
        if not listings:
            return
        rows = [_to_row(l) for l in listings]
        # upsert on dedupe_key — safe to re-run
        resp = self._sb.table(TABLE).upsert(rows, on_conflict="dedupe_key").execute()
        log.info(f"  Upserted {len(rows)} rows")
        return resp

    def apply_updates(self, updates: list[dict]):
        """Apply last_seen / price / source updates to existing rows."""
        for upd in updates:
            row_id = upd.pop("id", None)
            if not row_id:
                continue
            try:
                self._sb.table(TABLE).update(upd).eq("id", row_id).execute()
            except Exception as e:
                log.warning(f"  Update failed for id={row_id}: {e}")

    def update_status(self, listing_id: str, status: str):
        self._sb.table(TABLE).update({"status": status}).eq("id", listing_id).execute()

    def update_notes(self, listing_id: str, notes: str):
        self._sb.table(TABLE).update({"notes": notes}).eq("id", listing_id).execute()

    def mark_stale(self):
        """
        Any listing not seen in 5+ days → heat = '🧊 Stale'.
        Runs after every pipeline cycle.
        """
        cutoff = (date.today() - timedelta(days=5)).isoformat()
        try:
            resp = (
                self._sb.table(TABLE)
                .update({"heat": "🧊 Stale"})
                .lt("last_seen", cutoff)
                .neq("heat", "🧊 Stale")
                .execute()
            )
            if resp.data:
                log.info(f"  Marked {len(resp.data)} listings as Stale")
        except Exception as e:
            log.warning(f"  mark_stale failed: {e}")

    def get_dupes(self) -> list[dict]:
        """Return listings where price_min_seen != price_max_seen (price discrepancy)."""
        try:
            # Supabase doesn't support column != column directly; use RPC or filter in Python
            all_rows = (
                self._sb.table(TABLE)
                .select("id, address_normalized, unit, price, price_min_seen, price_max_seen, sources, source_urls, neighborhood")
                .neq("status", "rejected")
                .execute()
                .data or []
            )
            return [r for r in all_rows if _has_discrepancy(r)]
        except Exception as e:
            log.warning(f"  get_dupes failed: {e}")
            return []


# ── Schema mapping ─────────────────────────────────────────────────────────────

def _to_row(l: dict) -> dict:
    today = date.today().isoformat()

    sources = l.get("sources") or []
    if not sources and l.get("source"):
        sources = [l["source"]]

    source_urls = l.get("source_urls") or []
    if not source_urls and l.get("primary_url"):
        source_urls = [l["primary_url"]]

    return {
        "dedupe_key":              l.get("dedupe_key", ""),
        "address":                 (l.get("address") or "")[:500],
        "address_normalized":      (l.get("address_normalized") or "")[:500],
        "unit":                    (l.get("unit") or "")[:50],
        "neighborhood":            (l.get("neighborhood") or "")[:100],
        "neighborhood":            (l.get("neighborhood") or "")[:100],
        "price":                   l.get("price"),
        "price_min_seen":          l.get("price"),
        "price_max_seen":          l.get("price"),
        "bedrooms":                l.get("bedrooms"),
        "bathrooms":               l.get("bathrooms"),
        "sqft":                    l.get("sqft"),
        "floor":                   l.get("floor"),
        "is_ground_floor":         bool(l.get("is_ground_floor", False)),
        "in_unit_laundry":         bool(l.get("in_unit_laundry", False)),
        "dishwasher":              bool(l.get("dishwasher", False)),
        "parking":                 bool(l.get("parking", False)),
        "storage":                 bool(l.get("storage", False)),
        "gym":                     bool(l.get("gym", False)),
        "natural_light_confidence": l.get("natural_light_confidence", "Unknown"),
        "available_date":          l.get("available_date"),
        "commute_minutes":         l.get("commute_minutes"),
        "commute_ok":              bool(l.get("commute_ok", True)),
        "subway_walk_minutes":     l.get("subway_walk_minutes"),
        "primary_url":             (l.get("primary_url") or "")[:1000],
        "source_urls":             source_urls,           # stored as jsonb
        "sources":                 sources,               # stored as text[]
        "description":             (l.get("description") or "")[:5000],
        "building_name":           (l.get("building_name") or "")[:200],
        "building_reviews":        l.get("building_reviews", "Unknown"),
        "score_raw":               l.get("score_raw", 0),
        "score_tier":              l.get("score_tier", "🤔 Backup"),
        "heat":                    l.get("heat", "🔥 Hot"),
        "status":                  l.get("status", "new"),
        "first_seen":              today,
        "last_seen":               today,
        "broker_fee":              bool(l.get("broker_fee", False)),
        "notes":                   l.get("notes", ""),
    }


def _has_discrepancy(row: dict) -> bool:
    mn = row.get("price_min_seen")
    mx = row.get("price_max_seen")
    if mn is None or mx is None:
        return False
    return mx - mn > 100  # ignore <$100 rounding noise
