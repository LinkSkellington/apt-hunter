# Apt Hunter — Setup Checklist
# Complete these steps in order. Total time: ~45 minutes.

## 1. Supabase (10 min)
   [ ] Go to supabase.com → New project → name it "apt-hunter"
   [ ] Settings → API → copy:
         Project URL   →  SUPABASE_URL
         anon key      →  paste into dashboard/index.html (SUPABASE_ANON_KEY)
         service_role  →  SUPABASE_SERVICE_KEY  (keep secret — never in frontend)
   [ ] SQL Editor → paste entire supabase_schema.sql → Run

## 2. Gmail App Password (5 min)
   [ ] Google Account → Security → enable 2-Step Verification
   [ ] Security → App passwords → create "Apt Hunter" → copy 16-char password

## 3. GitHub repo (10 min)
   [ ] Create private repo: apt-hunter
   [ ] Push this entire folder to main branch
   [ ] Settings → Secrets and variables → Actions → New repository secret:
         SUPABASE_URL          = https://xxx.supabase.co
         SUPABASE_SERVICE_KEY  = eyJ...
         ALERT_EMAIL           = you@gmail.com
         ALERT_EMAIL_PASSWORD  = xxxx xxxx xxxx xxxx

## 4. Dashboard (5 min)
   [ ] Open dashboard/index.html in a text editor
   [ ] Line 2 of <script>: set SUPABASE_URL
   [ ] Line 3 of <script>: set SUPABASE_ANON_KEY
   [ ] Commit + push
   [ ] Repo → Settings → Pages → Source: main → Folder: /dashboard → Save
   [ ] Your dashboard: https://YOUR_USERNAME.github.io/apt-hunter/

## 5. First pipeline run (10 min)
   [ ] GitHub → Actions → "Apartment Pipeline" → Run workflow → Run
   [ ] Watch logs — first run takes ~10–15 min (Playwright installs Chromium)
   [ ] Open your dashboard → click Refresh → listings should appear

## 6. Verify
   [ ] Dashboard shows listings with scores and heat status
   [ ] Status dropdowns save changes (check Supabase table for updates)
   [ ] Trigger a Must Tour listing manually to test email alert:
         python pipeline/main.py --dry-run   (locally with .env)

## Ongoing
   Pipeline runs automatically at 7am / 12pm / 5pm / 10pm ET.
   Dashboard auto-refreshes every 5 minutes.
   You only need to open the dashboard and manage status/notes.

## Troubleshooting
   Pipeline fails → check Actions logs → most common: Playwright timeout
     Fix: increase timeout in scraper.py _random_delay calls
   No listings → source site structure changed → check scraper selectors
   Supabase 401 → check SUPABASE_SERVICE_KEY is the service_role key, not anon
   Email not sending → verify App Password (not your regular Gmail password)
