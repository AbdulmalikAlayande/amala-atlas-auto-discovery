import hashlib
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from scrapy.exceptions import DropItem

from apps.core.dedupe.resolve import dedupe_key, is_dup  # NEW
from apps.core.extractor.functions import extract_readable, extract_jsonld, extract_outlinks
from apps.core.nlp.fields import find_phone, find_city_hits
from apps.core.nlp.gate import passes_candidate_gate  # NEW
from apps.core.publisher.init import ClientConfig, SyncBackendClient
from apps.core.scoring.rules import score as score_page

log = logging.getLogger(__name__)
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_TOKEN = os.getenv("API_TOKEN", "dev-token")
ACCEPT_THRESHOLD = float(os.getenv("ACCEPT_THRESHOLD", "0.45"))
TARGET_CITIES = [c.strip() for c in os.getenv("TARGET_CITIES", "Lagos,Ibadan").split(",") if c.strip()]


class NLPGatePipeline:
    """
    Stage 3: Candidate Gate - Filter non-venue content early.
    
    This prevents wasting resources on recipe posts, generic food articles,
    and other non-venue content.
    """

    def process_item(self, item, spider):
        html = item.get("html") or ""

        # Extract text for gate analysis
        readable = extract_readable(html)
        text = readable.get("text", "")
        title = readable.get("title", "")

        # Run NLP gate
        passes, gate_signals = passes_candidate_gate(text, title)

        if not passes:
            fail_reason = gate_signals.get("fail_reason", "unknown")
            log.info(f"[NLP GATE] REJECT: {fail_reason} - {item.get('url')}")
            raise DropItem(f"Failed NLP gate: {fail_reason}")

        # Store gate signals for later use
        item['gate_signals'] = gate_signals
        log.info(f"[NLP GATE] PASS - {item.get('url')}")

        return item


class ExtractSignalsPipeline:
    """
    Stage 4: Field Extraction - Extract structured data from HTML.
    """

    def process_item(self, item, spider):
        html = item.get("html") or ""
        final_url = item.get("final_url") or item.get("url")

        readable = extract_readable(html)
        jsonld = extract_jsonld(html)
        outlinks = extract_outlinks(html, final_url)
        phone = find_phone(readable.get("text", ""))
        city_hits = find_city_hits(readable.get("text", ""), TARGET_CITIES)

        signals = self.build_signals(jsonld, outlinks, readable, phone, city_hits)
        log.info(f"[EXTRACT] Signals: {signals}")
        item['signals'] = signals
        return item

    def build_signals(self, jsonld, outlinks, readable, phone, city_hits):
        return {
            "has_jsonld_restaurant": bool(jsonld and jsonld.get("name") and jsonld.get("address")),
            "has_maps_link": bool(outlinks.get("maps_links")),
            "has_phone": bool(phone),
            "recent_content": bool(readable.get("is_recent")),
            "city_hit_near_food_terms": bool(city_hits),
        }


class ScorePipeline:
    """
    Stage 7: Scoring - Compute confidence score based on signals.
    """

    def process_item(self, item, spider):
        html = item.get("html") or ""
        final_url = item.get("final_url") or item.get("url")
        source_id = item.get("source_id")
        discovery_type = item.get("discovery_type")
        fetched_at = item.get("fetched_at")

        readable = extract_readable(html)
        jsonld = extract_jsonld(html)
        outlinks = extract_outlinks(html, final_url)
        phone = find_phone(readable.get("text", ""))
        city_hits = find_city_hits(readable.get("text", ""), TARGET_CITIES)

        # Build candidate fields
        fields = {
            "name": (jsonld.get("name") if jsonld else None),
            "address": (jsonld.get("address") if jsonld else None),
            "area": None,
            "city": city_hits[0] if city_hits else None,
            "state": None,
            "country": "Nigeria",
            "phone": phone,
            "hours": (jsonld.get("openingHours") if jsonld else None),
            "price": None,
            "socials": outlinks.get("social_links", {}),
            "lat": None,
            "lng": None,
            "geo_precision": "unknown"
        }

        candidate = self.build_candidate(
            discovery_type, fetched_at, fields, final_url,
            jsonld, outlinks, readable, source_id
        )

        # Score the candidate
        score, why = score_page(signals=item['signals'], hint_boost_names=[])
        candidate["score"] = score
        candidate["signals"] = {**item['signals'], "why": why}

        item['score'] = score
        item['candidate'] = candidate

        log.info(f"[SCORE] {score:.2f} - {final_url}")

        return item

    def build_candidate(self, discovery_type, fetched_at, fields, final_url,
                        jsonld, outlinks, readable, source_id):
        return {
            "fields": fields,
            "score": 0.0,
            "signals": {},
            "evidence": {
                "source_url": final_url,
                "title": readable.get("title"),
                "excerpt": (readable.get("text", "")[:500] or "").strip(),
                "jsonld": jsonld,
                "maps_links": outlinks.get("maps_links", []),
            },
            "provenance": {
                "source_id": source_id,
                "discovery_type": discovery_type,
                "fetched_at": fetched_at,
                "extractor_version": "v0.1.0",
            },
            "candidate_key": hashlib.sha256((final_url or "").encode("utf-8")).hexdigest(),
        }


class DedupePipeline:
    """
    Stage 6: Deduplication - Prevent publishing the same venue twice.
    
    Uses simple file-based cache for now. In production, this would be
    a database query against already-published candidates.
    """

    def __init__(self):
        self.cache_path = Path(".seen_cache")
        self.cache_path.mkdir(exist_ok=True)

        # In-memory cache for this crawl session
        self.seen_keys = {}

    def process_item(self, item, spider):
        candidate = item.get('candidate', {})

        # Extract dedupe key
        dedupe_data = dedupe_key(candidate)

        # Generate cache key (phone or geohash+name)
        cache_key = self._generate_cache_key(dedupe_data)

        # Check if we've seen this before
        if self._is_duplicate(cache_key, dedupe_data):
            log.info(f"[DEDUPE] Duplicate found: {item.get('final_url')}")
            raise DropItem("Duplicate candidate")

        # Mark as seen
        self._mark_seen(cache_key, dedupe_data)

        return item

    def _generate_cache_key(self, dedupe_data: dict) -> str | None:
        """Generate a cache key from dedupe data."""
        phone = dedupe_data.get('phone')
        if phone:
            return f"phone_{phone}"

        geohash = dedupe_data.get('geohash5')
        name = dedupe_data.get('name_norm', '').replace(' ', '_')[:30]
        if geohash and name:
            return f"geo_{geohash}_{name}"

        # Fallback: no reliable key
        log.warning(f"[DEDUPE] No cache key generated for candidate: {dedupe_data}")
        # FIXME: Implementation of a fallback is advised
        return None

    def _is_duplicate(self, cache_key: str, dedupe_data: dict) -> bool:
        """Check if this candidate is a duplicate."""
        if not cache_key:
            return False

        # Check in-memory cache first
        if cache_key in self.seen_keys:
            # Additional check: verify it's actually the same via is_dup()
            cached_data = self.seen_keys[cache_key]
            if is_dup(dedupe_data, cached_data):
                return True

        # Check file cache
        key_file = self.cache_path / cache_key
        if key_file.exists():
            return True

        return False

    def _mark_seen(self, cache_key: str, dedupe_data: dict):
        """Mark this candidate as seen."""
        if not cache_key:
            return

        # Store in memory
        self.seen_keys[cache_key] = dedupe_data

        # Store on disk
        key_file = self.cache_path / cache_key
        key_file.write_text("1")


class PublishPipeline:
    """
    Stage 8: Publishing - Send candidates to backend.
    
    Only publishes if:
    1. Score >= threshold
    2. Has at least one key fact (JSON-LD, phone, or maps link)
    """

    def process_item(self, item, spider):
        candidate = item.get('candidate', {})
        score = item.get('score', 0.0)
        signals = item.get('signals', {})

        # Check minimum requirements
        has_key_fact = (
                signals.get("has_jsonld_restaurant") or
                signals.get("has_phone") or
                signals.get("has_maps_link")
        )

        if not has_key_fact:
            log.info(f"[PUBLISH] DROP: No key facts - {item.get('final_url')}")
            raise DropItem("No key facts (JSON-LD, phone, or maps)")

        if score < ACCEPT_THRESHOLD:
            log.info(f"[PUBLISH] DROP: Score {score:.2f} < {ACCEPT_THRESHOLD} - {item.get('final_url')}")
            raise DropItem(f"Score below threshold: {score:.2f}")

        # Publish to backend
        try:
            config = ClientConfig(base_url=API_BASE_URL, token=API_TOKEN)
            client = SyncBackendClient(config)
            resp = client.publish_candidate(candidate)

            if resp.success:
                log.info(f"[PUBLISH] SUCCESS: score={score:.2f} url={item.get('final_url')}")
            else:
                log.error(f"[PUBLISH] FAILED: {item.get('final_url')} - {resp.error}")
                raise DropItem(f"Publish failed: {resp.error}")

        except Exception as e:
            log.exception(f"[PUBLISH] ERROR: {item.get('final_url')} - {e}")
            raise DropItem(f"Publish error: {e}")

        return item
