-- ============================================================
-- Apartment Hunter — Supabase Schema
-- Run this entire file once in the Supabase SQL Editor.
-- ============================================================

-- Enable UUID extension
create extension if not exists "pgcrypto";

-- ── Main listings table ──────────────────────────────────────────────────────
create table if not exists listings (
  id                      uuid primary key default gen_random_uuid(),
  dedupe_key              text unique not null,          -- SHA-256 fingerprint

  -- Address
  address                 text,
  address_normalized      text,
  unit                    text,
  neighborhood            text,

  -- Price
  price                   integer,
  price_min_seen          integer,
  price_max_seen          integer,

  -- Physical
  bedrooms                integer,
  bathrooms               numeric(3,1),
  sqft                    integer,
  floor                   integer,
  is_ground_floor         boolean default false,

  -- Amenities
  in_unit_laundry         boolean default false,
  dishwasher              boolean default false,
  parking                 boolean default false,
  storage                 boolean default false,
  gym                     boolean default false,

  -- Qualitative
  natural_light_confidence  text default 'Unknown',
  available_date            text,
  commute_minutes           integer,
  commute_ok                boolean default true,
  subway_walk_minutes       integer,

  -- Sources
  primary_url             text,
  source_urls             jsonb default '[]'::jsonb,
  sources                 text[] default array[]::text[],

  -- Content
  description             text,
  building_name           text,
  building_reviews        text default 'Unknown',
  broker_fee              boolean default false,

  -- Scoring
  score_raw               integer default 0,
  score_tier              text default '🤔 Backup',
  heat                    text default '🔥 Hot',

  -- Workflow
  status                  text default 'new',     -- new | reviewing | saved | touring | contacted | rejected
  notes                   text default '',
  contacted_on            date,
  toured_on               date,

  -- Timestamps
  first_seen              date not null default current_date,
  last_seen               date not null default current_date,
  created_at              timestamptz default now(),
  updated_at              timestamptz default now()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────
create index if not exists idx_listings_score     on listings (score_raw desc);
create index if not exists idx_listings_heat      on listings (heat);
create index if not exists idx_listings_status    on listings (status);
create index if not exists idx_listings_tier      on listings (score_tier);
create index if not exists idx_listings_first     on listings (first_seen desc);
create index if not exists idx_listings_last      on listings (last_seen desc);
create index if not exists idx_listings_neigh     on listings (neighborhood);
create index if not exists idx_listings_price     on listings (price);

-- ── Auto-update updated_at ────────────────────────────────────────────────────
create or replace function update_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists set_updated_at on listings;
create trigger set_updated_at
  before update on listings
  for each row execute function update_updated_at();

-- ── Row-Level Security (RLS) ──────────────────────────────────────────────────
-- The pipeline uses the service_role key (bypasses RLS).
-- The dashboard uses the anon key — read-only.

alter table listings enable row level security;

-- Anonymous users can only READ (dashboard)
create policy "anon_read" on listings
  for select to anon using (true);

-- Service role has full access (pipeline writes)
-- service_role bypasses RLS by default — no extra policy needed.

-- ── Dashboard view (pre-joined, ordered) ─────────────────────────────────────
create or replace view dashboard_listings as
  select
    id,
    address_normalized,
    unit,
    neighborhood,
    price,
    price_min_seen,
    price_max_seen,
    bedrooms,
    bathrooms,
    sqft,
    floor,
    is_ground_floor,
    in_unit_laundry,
    dishwasher,
    parking,
    storage,
    gym,
    natural_light_confidence,
    available_date,
    commute_minutes,
    commute_ok,
    primary_url,
    source_urls,
    sources,
    building_name,
    building_reviews,
    broker_fee,
    score_raw,
    score_tier,
    heat,
    status,
    notes,
    first_seen,
    last_seen,
    (current_date - first_seen) as days_on_market,
    (price_max_seen - price_min_seen) as price_discrepancy
  from listings
  where status != 'rejected'
  order by score_raw desc, first_seen desc;

-- ── Stale listing function (called by pipeline) ───────────────────────────────
create or replace function mark_stale_listings()
returns void language plpgsql as $$
begin
  update listings
  set heat = '🧊 Stale'
  where last_seen < current_date - interval '5 days'
    and heat != '🧊 Stale'
    and status not in ('rejected', 'touring', 'contacted');
end;
$$;

-- ── Price discrepancy view ─────────────────────────────────────────────────────
create or replace view price_discrepancies as
  select
    id,
    address_normalized,
    unit,
    neighborhood,
    price,
    price_min_seen,
    price_max_seen,
    (price_max_seen - price_min_seen) as discrepancy,
    sources,
    source_urls,
    score_tier
  from listings
  where price_max_seen - price_min_seen > 100
    and status != 'rejected'
  order by discrepancy desc;
