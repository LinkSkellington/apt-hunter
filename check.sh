#!/bin/bash

# =============================================================
# Apt Hunter — Pre-flight Check
# Run before triggering GitHub Actions pipeline.
# Usage: bash ~/Desktop/apt-hunter/check.sh
# =============================================================

REPO="$HOME/Desktop/apt-hunter"
PASS=0
FAIL=0

green()  { echo "  ✅  $1"; ((PASS++)); }
red()    { echo "  ❌  $1"; ((FAIL++)); }
yellow() { echo "  ⚠️   $1"; }
header() { echo; echo "── $1 ──────────────────────────────────"; }

echo ""
echo "╔════════════════════════════════════════╗"
echo "║      Apt Hunter Pre-flight Check       ║"
echo "╚════════════════════════════════════════╝"

# ── 1. Folder structure ───────────────────────────────────────
header "Folder structure"

REQUIRED_FILES=(
  "pipeline/main.py"
  "pipeline/requirements.txt"
  "pipeline/ingestion/scraper.py"
  "pipeline/ingestion/__init__.py"
  "pipeline/processing/filter.py"
  "pipeline/processing/dedupe.py"
  "pipeline/processing/score.py"
  "pipeline/processing/__init__.py"
  "pipeline/storage/supabase_client.py"
  "pipeline/storage/__init__.py"
  "pipeline/alerts/email_alert.py"
  "pipeline/alerts/__init__.py"
  ".github/workflows/pipeline.yml"
  "dashboard/index.html"
  "index.html"
  "supabase_schema.sql"
)

for f in "${REQUIRED_FILES[@]}"; do
  if [ -f "$REPO/$f" ]; then
    green "$f"
  else
    red "$f — MISSING"
  fi
done

# ── 2. Scraper has all 13 sources ─────────────────────────────
header "Scraper sources (expecting 13)"

SCRAPER="$REPO/pipeline/ingestion/scraper.py"
SOURCES=(
  "fetch_corcoran"
  "fetch_compass"
  "fetch_elliman"
  "fetch_bhs"
  "fetch_sothebys"
  "fetch_halstead"
  "fetch_bond"
  "fetch_nestseekers"
  "fetch_level"
  "fetch_direct_buildings"
  "fetch_streeteasy"
  "fetch_zillow"
  "fetch_redfin"
)

for fn in "${SOURCES[@]}"; do
  if grep -q "def $fn" "$SCRAPER" 2>/dev/null; then
    green "$fn"
  else
    red "$fn — NOT FOUND in scraper.py"
  fi
done

# ── 3. No placeholder credentials in code ─────────────────────
header "Credential safety (no real secrets in code)"

if grep -r "sb_secret_\|ghp_" "$REPO" --include="*.py" --include="*.html" --include="*.yml" -l 2>/dev/null | grep -v ".git" | grep -q .; then
  red "Real credentials found in code files — do not push!"
  grep -r "sb_secret_\|ghp_" "$REPO" --include="*.py" --include="*.html" --include="*.yml" -l 2>/dev/null | grep -v ".git"
else
  green "No real secrets found in code"
fi

# ── 4. Dashboard has Supabase keys filled in ──────────────────
header "Dashboard configuration"

INDEX="$REPO/index.html"
if grep -q "YOUR_PROJECT\|YOUR_ANON_KEY" "$INDEX" 2>/dev/null; then
  red "index.html still has placeholder keys — fill in SUPABASE_URL and SUPABASE_ANON_KEY"
else
  URL_LINE=$(grep "SUPABASE_URL" "$INDEX" | head -1)
  KEY_LINE=$(grep "SUPABASE_ANON_KEY" "$INDEX" | head -1)
  if echo "$URL_LINE" | grep -q "https://"; then
    green "SUPABASE_URL is set"
  else
    red "SUPABASE_URL looks wrong: $URL_LINE"
  fi
  if echo "$KEY_LINE" | grep -q "sb_publishable_\|eyJ"; then
    green "SUPABASE_ANON_KEY is set"
  else
    yellow "SUPABASE_ANON_KEY may be wrong — check: $KEY_LINE"
  fi
fi

# Check both index.html copies match
if diff "$REPO/index.html" "$REPO/dashboard/index.html" > /dev/null 2>&1; then
  green "Root index.html and dashboard/index.html are in sync"
else
  yellow "Root index.html and dashboard/index.html differ — run: cp index.html dashboard/index.html"
fi

# ── 5. Git status ─────────────────────────────────────────────
header "Git status"

cd "$REPO" || exit 1

BRANCH=$(git branch --show-current 2>/dev/null)
if [ "$BRANCH" = "main" ]; then
  green "On branch: main"
else
  yellow "On branch: $BRANCH (expected main)"
fi

UNCOMMITTED=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
if [ "$UNCOMMITTED" = "0" ]; then
  green "No uncommitted changes"
else
  red "$UNCOMMITTED uncommitted file(s) — run git add . && git commit && git push"
  git status --short
fi

AHEAD=$(git log origin/main..HEAD --oneline 2>/dev/null | wc -l | tr -d ' ')
if [ "$AHEAD" = "0" ]; then
  green "All commits pushed to GitHub"
else
  red "$AHEAD commit(s) not pushed — run: git push"
fi

LAST_COMMIT=$(git log --oneline -1 2>/dev/null)
echo "  Last commit: $LAST_COMMIT"

# ── 6. Required Python packages in requirements.txt ──────────
header "requirements.txt packages"

REQUIRED_PKGS=(
  "requests"
  "supabase"
  "rapidfuzz"
  "scikit-learn"
  "numpy"
)

for pkg in "${REQUIRED_PKGS[@]}"; do
  if grep -q "$pkg" "$REPO/pipeline/requirements.txt" 2>/dev/null; then
    green "$pkg"
  else
    red "$pkg — missing from requirements.txt"
  fi
done

# ── 7. Workflow file sanity ────────────────────────────────────
header "GitHub Actions workflow"

WORKFLOW="$REPO/.github/workflows/pipeline.yml"
if [ -f "$WORKFLOW" ]; then
  green "pipeline.yml exists"
  if grep -q "SUPABASE_URL" "$WORKFLOW"; then
    green "SUPABASE_URL referenced in workflow"
  else
    red "SUPABASE_URL not found in workflow — secrets won't be passed"
  fi
  if grep -q "SUPABASE_SERVICE_KEY" "$WORKFLOW"; then
    green "SUPABASE_SERVICE_KEY referenced in workflow"
  else
    red "SUPABASE_SERVICE_KEY not found in workflow"
  fi
  if grep -q "workflow_dispatch" "$WORKFLOW"; then
    green "Manual trigger (workflow_dispatch) enabled"
  else
    yellow "Manual trigger not enabled — you can only run on schedule"
  fi
  if grep -q "playwright" "$WORKFLOW"; then
    red "Playwright still referenced in workflow — this causes install failures"
  else
    green "No Playwright dependency in workflow"
  fi
else
  red "pipeline.yml MISSING"
fi

# ── 8. Scraper file size sanity check ─────────────────────────
header "Scraper file size"

SCRAPER_LINES=$(wc -l < "$SCRAPER" 2>/dev/null | tr -d ' ')
if [ -z "$SCRAPER_LINES" ]; then
  red "scraper.py not found"
elif [ "$SCRAPER_LINES" -lt 200 ]; then
  red "scraper.py only $SCRAPER_LINES lines — likely the old version, not the expanded one"
elif [ "$SCRAPER_LINES" -gt 400 ]; then
  green "scraper.py has $SCRAPER_LINES lines — looks like the full expanded version"
else
  yellow "scraper.py has $SCRAPER_LINES lines — may be partial"
fi

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "╔════════════════════════════════════════╗"
if [ "$FAIL" = "0" ]; then
  echo "║   ✅  All checks passed — ready to run ║"
else
  echo "║   ❌  $FAIL check(s) failed — fix before running  ║"
fi
echo "╚════════════════════════════════════════╝"
echo "   Passed: $PASS   Failed: $FAIL"
echo ""

if [ "$FAIL" = "0" ]; then
  echo "Next step: GitHub → Actions → Apartment Pipeline → Run workflow"
else
  echo "Fix the issues above, then re-run this script."
fi
echo ""
