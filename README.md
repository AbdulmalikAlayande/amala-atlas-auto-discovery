# Amala Atlas — Auto-Discovery Service (Archived)

[![Scrapy](https://img.shields.io/badge/Scrapy-2.x-green.svg)](https://scrapy.org)
[![Status](https://img.shields.io/badge/Status-Archived-lightgrey.svg)](#)

> **This module has been archived.** Its useful components (keyword lists, geocoding, deduplication logic) have been salvaged into the [Backend API](https://github.com/AbdulmalikAlayande/amala-atlas-backend-api) as lightweight utility modules, and its web scraping functionality has been replaced by a simple `seed_from_blogs` management command. See the [Architecture Decision](#why-archived) section below for the full reasoning.

## What This Was

A Scrapy-based autonomous web crawler designed to discover Amala food spots by scraping Nigerian food blogs, sitemaps, and social media. It used an 8-stage NLP pipeline to extract, classify, score, and publish candidate spots.

### Pipeline Stages

```
Source Queue → Scrapy Spider → Content Extraction → NLP Gate (keyword/entity check)
    → Field Extraction → Geocoding/Enrichment → Deduplication → Scoring → Publisher
```

### Components

```
apps/
├── core/
│   ├── nlp/            # Food keyword matching, entity recognition, field hints
│   │   ├── gate.py     # FOOD_KEYWORDS, VENUE_KEYWORDS, NIGERIAN_CITIES
│   │   ├── fields.py   # Named entity extraction for spot names
│   │   └── hint.py     # Heuristic indicators for spot-like content
│   ├── enrichment/     # Geocoding, Google Maps enrichment, social media lookup
│   ├── dedupe/         # Name normalization, phone matching, geohash proximity
│   ├── extractor/      # Readability + JSON-LD structured data extraction
│   ├── scoring/        # Evidence-based confidence scoring rules
│   ├── publisher/      # HTTP client to POST candidates to the Backend API
│   └── utils/          # Data extraction helpers
├── crawler/            # Scrapy project
│   └── spiders/        # Site-specific + generic spiders
│       ├── foodieinlagosdotcom.py
│       ├── ofadaadotcom.py
│       ├── generic.py
│       ├── genericarticle.py
│       ├── sitemap.py
│       └── feed.py
├── sources/            # Source URL management (SQLAlchemy models)
└── worker/             # Celery task definitions
```

### Tech Stack
- **Scrapy** — Web crawling framework
- **spaCy** — NLP entity recognition
- **Trafilatura** — Article content extraction
- **SQLAlchemy** — Source URL persistence
- **Geopy** — Geocoding via Nominatim
- **Docker** — Containerized deployment

## Why Archived

After a thorough analysis of the project's direction, we concluded that web scraping was the wrong primary strategy for this problem:

1. **Data ceiling**: Only ~5 Nigerian food blogs exist with relevant content, totaling ~50-100 articles. An 8-stage NLP pipeline across 2000+ lines of code to scrape this is over-engineered.

2. **Wrong data source**: Nigeria's best Amala spots don't have websites. They're informal businesses known by word-of-mouth — "Iya Basira in Mokola" or "that buka behind the market." This knowledge lives in WhatsApp groups and people's heads, not on web pages.

3. **Better alternatives**: WhatsApp bot submissions with GPS location pins provide higher-quality data (exact coordinates, photos, direct from someone who was physically there) than anything a web scraper can produce.

## What Was Salvaged

The following components were extracted into the Backend API as lightweight utility modules:

| Auto-Discovery Source | Backend Destination | What It Does |
|----------------------|-------------------|--------------|
| `apps/core/nlp/gate.py` | `places/nlp_utils.py` | FOOD_KEYWORDS, VENUE_KEYWORDS, NIGERIAN_CITIES |
| `apps/core/enrichment/geocoding.py` | `places/geocoding.py` | Nominatim geocoder + city centroid fallback |
| `apps/core/dedupe/resolve.py` | `places/dedupe.py` | Name normalization, phone matching, fuzzy dedup |
| `apps/core/extractor/functions.py` | `ingestion/extractors.py` | HTML readability + JSON-LD extraction |
| Blog scraping (all spiders) | `manage.py seed_from_blogs` | Single ~150-line management command |

## Running (For Historical Reference)

```bash
# Install dependencies
pip install -e .

# Run a specific spider
scrapy crawl foodieinlagosdotcom

# Run all spiders via CLI
python cli.py crawl --source-file sources/seedsource.csv
```

---

*Part of the [Amala Atlas](https://github.com/AbdulmalikAlayande/Amala-Atlas) ecosystem. Replaced by community-driven discovery channels in the [Backend API](https://github.com/AbdulmalikAlayande/amala-atlas-backend-api).*
