[README.md](https://github.com/user-attachments/files/27904099/README.md)
# Apt Hunter

**Automated rental listing pipeline with real-time scored dashboard**

Apt Hunter is a fully automated apartment search tool that pulls live NYC rental listings twice daily, scores each one against a custom set of criteria, stores results in a cloud database, and surfaces the best options on an interactive map dashboard — with no manual intervention required.

Built for personal use during an apartment search in Brooklyn. Architecture is general-purpose and could extend to property management, portfolio monitoring, or market research.

[Apt Hunter Dashboard](assets:/Apt_Hunter_Dashboard.png)

---

## How It Works

```
RentCast API → Python pipeline → Supabase (PostgreSQL) → GitHub Pages dashboard
```

GitHub Actions runs the pipeline on a cron schedule (4x/day). No server required — entirely serverless.

1. **Ingest** — queries RentCast API across target NYC zip codes, filtering by beds, baths, price, and neighborhood
2. **Deduplicate** — matches incoming listings against existing database records by normalized address; updates timestamps instead of re-inserting
3. **Score** — assigns each listing a 0–100 composite score across 8 weighted criteria
4. **Store** — writes scored listings to Supabase; soft-deletes rejected listings after 12 days
5. **Display** — dashboard reads from Supabase in real time; no backend required

---

## Scoring Model

Each listing receives a composite score (0–100) based on:

| Criteria | Max Points |
|---|---|
| Hard criteria (price, beds, baths) | 30 |
| Commute time to Midtown | 20 |
| Neighborhood priority tier | 15 |
| Square footage | 10 |
| Natural light (inferred from listing text) | 10 |
| Amenities (laundry, dishwasher, gym, etc.) | 10 |
| Building quality signal | 5 |
| 3BR+ bonus | +8 |

Score tiers: **Elite** (68–69) · **Top Picks** (65–67) · **So Close** (45–64)

> Note: RentCast omits sqft and amenity data for most NYC listings, which caps practical scores around 69. Scores would be higher with richer source data.

---

## Dashboard Features

- **Map view** — Leaflet.js with CartoDB dark tiles; price pins color-coded by score tier
- **Listing panel** — card view with inline save / flag for review / dislike actions
- **Filters** — by tab (Favorites, Elite, New Today, etc.), neighborhood chip, commute toggle, and sort
- **Detail drawer** — full score breakdown, amenity checklist, status dropdown, and persistent notes
- **Real-time sync** — all actions (save, review, dislike, notes) write directly to Supabase and sync across devices

---

## Stack

| Layer | Technology |
|---|---|
| Pipeline | Python 3.11 |
| Scheduler | GitHub Actions (cron) |
| Data source | RentCast API (REST/JSON) |
| Database | Supabase — managed PostgreSQL with RLS |
| Dashboard | Vanilla JS / HTML — hosted on GitHub Pages |
| Map | Leaflet.js + CartoDB tile layer |
| Alerts | Python `smtplib` + Gmail SMTP |

---

## Cost

| Component | Cost |
|---|---|
| RentCast API (Starter — ~360 calls/month) | $35/mo |
| Supabase (Free tier) | $0 |
| GitHub Actions + Pages | $0 |
| **Total** | **~$35/month** |

---

## Setup

**Prerequisites:** Python 3.11+, a [RentCast API key](https://rentcast.io), a [Supabase](https://supabase.com) project

```bash
git clone https://github.com/LinkSkellington/apt-hunter.git
cd apt-hunter
pip install -r requirements.txt
cp .env.example .env   # add your API keys
```

**Environment variables required:**
```
RENTCAST_API_KEY=
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
SUPABASE_ANON_KEY=
GMAIL_USER=
GMAIL_APP_PASSWORD=
```

**Run the pipeline manually:**
```bash
python pipeline/run.py
```

**Deploy the dashboard:**
The `index.html` in the root works as a static GitHub Pages site. Enable Pages in repo settings → deploy from `main` branch root. Requires an active RentCast API subscription to populate data.

**Configure GitHub Actions:**
Add the environment variables above as repository secrets. The workflow in `.github/workflows/` runs automatically on the cron schedule.

See `SETUP.md` for full configuration details including Supabase schema setup and RLS policy configuration.

---

## Extending

The scoring weights and filter criteria live in one Python config file. To adapt to a different city, data source, or use case:

- Swap RentCast for another listings API by updating the ingestion module
- Adjust scoring weights or add new criteria in the scoring module
- Modify the database schema to capture different fields
- Re-skin the dashboard for a different audience (property managers, tenants, etc.)
