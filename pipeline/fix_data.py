"""
One-time script to backfill missing data on existing listings:
1. Extract floor from unit number (e.g. "8E" -> 8, "19T" -> 19)
2. Set commute_minutes from neighborhood lookup table
3. Re-score all listings with updated data
Run from: pipeline/ directory with .env loaded
"""
import os, re, sys
sys.path.insert(0, '.')
from supabase import create_client
from processing.score import _COMMUTE_EST

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])

rows = sb.table('listings').select(
    'id, unit, neighborhood, floor, commute_minutes, score_raw, score_tier, heat, address_normalized'
).execute().data

print(f"Processing {len(rows)} listings...")

updated = 0
for row in rows:
    changes = {}

    # ── Extract floor from unit number ──────────────────
    unit = row.get('unit') or ''
    if not row.get('floor') and unit:
        m = re.match(r'^(\d+)', str(unit).strip())
        if m:
            floor_num = int(m.group(1))
            if 1 <= floor_num <= 80:
                changes['floor'] = floor_num
                changes['is_ground_floor'] = floor_num <= 1

    # ── Set commute from neighborhood estimate ───────────
    if not row.get('commute_minutes'):
        neigh = (row.get('neighborhood') or '').lower().strip()
        est = _COMMUTE_EST.get(neigh)
        if est:
            changes['commute_minutes'] = est
            changes['commute_ok'] = est <= 60

    if changes:
        sb.table('listings').update(changes).eq('id', row['id']).execute()
        updated += 1

print(f"Updated {updated} listings")

# ── Now re-score everything with better data ─────────────
print("Re-scoring all listings...")
from processing.score import score_listing

rows2 = sb.table('listings').select('*').execute().data
rescored = 0
for row in rows2:
    scored = score_listing(row)
    sb.table('listings').update({
        'score_raw':   scored['score_raw'],
        'score_tier':  scored['score_tier'],
        'heat':        scored['heat'],
        'natural_light_confidence': scored.get('natural_light_confidence', 'Unknown'),
        'commute_ok':  scored.get('commute_ok', True),
    }).eq('id', row['id']).execute()
    rescored += 1

print(f"Re-scored {rescored} listings")
print("Done — refresh your dashboard")
