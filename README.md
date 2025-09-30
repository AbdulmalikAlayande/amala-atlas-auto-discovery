# Amala Atlas – Auto-Discovery Service

The **Auto-Discovery Service** is the data ingestion and discovery subsystem of the **Amala Atlas** platform. It continuously finds, extracts, deduplicates, and scores potential *Amala joints & related venues* from the web, and publishes them as **candidates** to the Amala Atlas Backend API for human or automated verification.

---

## Architecture

The Amala Atlas platform has three main parts:

1. **Frontend** – User-facing web/mobile client.
2. **Backend (Django APIs)** – Core application that stores venues, candidates, and verification flows.
3. **Auto-Discovery Service (this repo)** – Independent crawler + extractor + NLP pipelines, built as a containerized service, communicating with the backend over REST.

---

## Features

### Source-driven discovery
- Seed blogs, directories, and social handles.
- Sitemap & RSS feed crawling.
- Search (SERP) integration via query dictionary.

### Generic article extraction
- Clean HTML → readable text (title, body, date).
- JSON-LD schema detection (`Restaurant`, `LocalBusiness`).
- Outlink detection (Google Maps, Instagram, Facebook, etc).

### Entity & field extraction
- Restaurant name, address, city, phone, hours, price, socials.
- Regex + spaCy + lightweight heuristics.

### Geocoding
- Address → lat/lng.
- City-only fallback (centroid + geo_precision flag).

### Deduplication
- Geohash proximity + fuzzy name/phone similarity.

### Scoring & gating
- Rule-based 0..1 score with transparent signals `{why}`.
- Drops junk; accepts only if score ≥ threshold and has key fact (phone, JSON-LD, maps link).

### Publishing
- Candidates POSTed to Backend `/api/ingest/candidate`.
- Idempotent by `candidate_key`.
- Evidence (snippets, source URL, JSON-LD, outlinks) included.

### Scheduling
- Celery Beat + Redis queue.
- Feeds daily, sitemaps weekly, SERPs bi-weekly.
- Frontier queue for continuous draining.

---

## Repository Layout

```
auto-discovery/
├── apps/
│   ├── crawler/         # Scrapy project (spiders, pipelines)
│   ├── core/            # Extractors, NLP, scoring, publisher
│   └── ...
├── configs/
│   ├── sources/priority_seed_sources_v1.csv
│   ├── hints/hint_boost_list_v1.csv
│   └── queries/query_dict.json
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .env.example
└── README.md
```

---

## Pipelines (Scrapy → Core → Backend)

```
Sitemap/Feed/SERP Spider
      ↓
PersistRawHTMLPipeline
      ↓
ExtractSignalsPipeline
      ↓
ScoreAndGatePipeline
      ↓
DedupePipeline
      ↓
PublishPipeline → Backend /api/ingest/candidate
```

---

## Environment Variables

```ini
API_BASE_URL=http://backend:8000
API_TOKEN=<your_api_token>
ACCEPT_THRESHOLD=0.45
TARGET_CITIES=Lagos,Ibadan,Ogun,Osun,Kwara
REDIS_URL=redis://redis:6379/0
```

---

## Running Locally

```bash
# 1. Clone repo
git clone <repo_url>
cd auto-discovery

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create .env
cp .env.example .env
# edit values

# 4. Run a single crawl
scrapy crawl generic_article -a url="https://foodieinlagos.com/review-ajoke-alamala-plus-lagos/"
```

---

## Deployment

- Containerized with Docker.
- Runs as separate service from Backend.
- Communicates via HTTP/REST only.
- Celery Beat + Workers scheduled to run discovery jobs.
- Redis used as URL frontier + dedupe cache.

---

## References

- **Backend API Spec**: [link-to-backend-docs-or-openapi]
- **Priority Seed Sources v1**: `configs/sources/priority_seed_sources_v1.csv`
- **Hint Boost List v1**: `configs/hints/hint_boost_list_v1.csv`
- **Query Dictionary**: `configs/queries/query_dict.json`
- **Frontend Repo**: [link-to-frontend-repo]

---

## Roadmap

- ☐ Expand query dictionary for more regions.
- ☐ Integrate advanced NLP models for address/price extraction.
- ☐ Add YouTube transcript ingestion.
- ☐ Enhance scoring with ML instead of rules.

---

## License

[Insert your license here]

---

**⚠️ Replace placeholders**: `<repo_url>`, `<your_api_token>`, links to backend/frontend repos, API specs, and license.
