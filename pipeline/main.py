"""
Apartment Hunter — Main Pipeline Orchestrator
Invoked by GitHub Actions cron every 6 hours.

Usage:
  python main.py                         # full run, all sources
  python main.py --source streeteasy     # single source
  python main.py --dry-run               # no DB writes, prints results
"""

import argparse
import logging
import sys
from datetime import datetime

from ingestion.scraper import fetch_all_sources
from processing.filter import apply_hard_filters
from processing.dedupe import deduplicate
from processing.score import score_listing
from storage.supabase_client import SupabaseClient
from alerts.email_alert import send_must_tour_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("run.log"),
    ],
)
log = logging.getLogger(__name__)


def run(sources=None, dry_run=False):
    log.info("=" * 50)
    log.info(f"Pipeline start — {datetime.utcnow().isoformat()}Z")
    t0 = datetime.utcnow()

    # ── 1. INGEST ──────────────────────────────────────
    log.info("Step 1 › Ingesting from sources...")
    raw = fetch_all_sources(sources=sources)
    log.info(f"         {len(raw)} raw listings fetched")

    # ── 2. HARD FILTER ─────────────────────────────────
    log.info("Step 2 › Applying hard filters...")
    passed = apply_hard_filters(raw)
    log.info(f"         {len(passed)} passed / {len(raw) - len(passed)} rejected")

    # ── 3. DEDUPLICATE ─────────────────────────────────
    log.info("Step 3 › Deduplicating...")
    db = SupabaseClient()
    existing = db.get_all_listings() if not dry_run else []
    new_listings, updates = deduplicate(passed, existing)
    log.info(f"         {len(new_listings)} new · {len(updates)} updates")

    # ── 4. SCORE ───────────────────────────────────────
    log.info("Step 4 › Scoring...")
    scored_new = [score_listing(l) for l in new_listings]
    scored_upd = [score_listing(l) for l in updates]

    # ── 5. PERSIST ─────────────────────────────────────
    if dry_run:
        log.info("Step 5 › DRY RUN — skipping DB writes")
        for l in scored_new[:10]:
            log.info(
                f"  [{l['score_tier']}] {l['address_normalized']} "
                f"${l['price']:,} — {l['score_raw']}/100"
            )
    else:
        log.info("Step 5 › Writing to Supabase...")
        db.upsert_listings(scored_new)
        db.apply_updates(scored_upd)
        db.mark_stale()

    # ── 6. ALERT ───────────────────────────────────────
    must_tour_new = [l for l in scored_new if l["score_tier"] == "🔥 Must Tour"]
    if must_tour_new and not dry_run:
        log.info(f"Step 6 › Alerting on {len(must_tour_new)} Must Tour listing(s)...")
        send_must_tour_alert(must_tour_new)

    elapsed = (datetime.utcnow() - t0).seconds
    log.info(f"Pipeline done in {elapsed}s — {len(must_tour_new)} Must Tour alerts sent")
    log.info("=" * 50)

    return {
        "raw": len(raw),
        "passed": len(passed),
        "new": len(scored_new),
        "updated": len(scored_upd),
        "must_tour": len(must_tour_new),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", nargs="+")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(sources=args.source, dry_run=args.dry_run)
