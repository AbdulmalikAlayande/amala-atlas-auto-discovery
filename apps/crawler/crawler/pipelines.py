import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from scrapy.exceptions import DropItem

from apps.core.dedupe.resolve import dedupe_key, is_dup
from apps.core.enrichment.geocoding import enrich_location
from apps.core.enrichment.maps import enrich_from_maps_links
from apps.core.enrichment.social import enrich_from_socials
from apps.core.extractor.functions import extract_readable, extract_jsonld, extract_outlinks
from apps.core.nlp.fields import find_phone, find_city_hits
from apps.core.nlp.gate import passes_candidate_gate, CandidateGate
from apps.core.publisher.init import ClientConfig, SyncBackendClient
from apps.core.scoring.rules import score as score_page
from apps.sources.source_manager import SourceManager

log = logging.getLogger(__name__)
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_TOKEN = os.getenv("API_TOKEN", "dev-token")
ACCEPT_THRESHOLD = float(os.getenv("ACCEPT_THRESHOLD", "0.10"))
TARGET_CITIES = [c.strip() for c in os.getenv("TARGET_CITIES", "Lagos,Ibadan").split(",") if c.strip()]


class NLPGatePipeline:
    """Stage 3: Candidate Gate - Filter non-venue content early."""

    def process_item(self, item, spider):
        html = item.get("html") or ""

        readable = extract_readable(html)
        text = readable.get("text", "")
        title = readable.get("title", "")

        # Use the gate with softened word count
        gate = CandidateGate()
        passes, gate_signals = gate.passes_gate(text, title)

        if not passes:
            fail_reason = gate_signals.get("fail_reason", "unknown")
            log.info(f"[NLP GATE] REJECT: {fail_reason} - {item.get('url')}")
            raise DropItem(f"Failed NLP gate: {fail_reason}")

        item['gate_signals'] = gate_signals
        log.info(f"[NLP GATE] PASS - {item.get('url')}")

        return item


class ExtractSignalsPipeline:
    """Stage 4: Field Extraction - Extract structured data from HTML."""

    def process_item(self, item, spider):
        html = item.get("html") or ""
        final_url = item.get("final_url") or item.get("url")

        readable = extract_readable(html)
        jsonld = extract_jsonld(html)
        outlinks = extract_outlinks(html, final_url)
        phone_data = find_phone(readable.get("text", ""))
        city_hits = find_city_hits(readable.get("text", ""), TARGET_CITIES)

        signals = self.build_signals(jsonld, outlinks, readable, phone_data, city_hits)

        # Store extracted data for enrichment stage
        item['extracted'] = {
            'readable': readable,
            'jsonld': jsonld,
            'outlinks': outlinks,
            'phone_data': phone_data,
            'city_hits': city_hits,
        }

        log.info(f"[EXTRACT] Signals: {signals}")
        item['signals'] = signals
        return item

    def build_signals(self, jsonld, outlinks, readable, phone_data, city_hits):
        return {
            "has_jsonld_restaurant": bool(jsonld and jsonld.get("name") and jsonld.get("address")),
            "has_maps_link": bool(outlinks.get("maps_links")),
            "has_phone": bool(phone_data),
            "recent_content": bool(readable.get("is_recent")),
            "city_hit_near_food_terms": bool(city_hits),
        }


class EnrichmentPipeline:
    """
    Stage 5: Enrichment - Add geocoding, maps data, and social metadata.

    This runs AFTER field extraction but BEFORE scoring.
    Enrichment can improve scores by adding lat/lng and better data.
    """

    def process_item(self, item, spider):
        extracted = item.get('extracted', {})

        # Build initial fields from extraction
        jsonld = extracted.get('jsonld', {})
        phone_data = extracted.get('phone_data')
        phone = phone_data.get('value') if phone_data else None
        city_hits = extracted.get('city_hits', [])
        city = city_hits[0].get('value') if city_hits else None
        outlinks = extracted.get('outlinks', {})

        fields = {
            "name": jsonld.get("name") if jsonld else None,
            "address": jsonld.get("address") if jsonld else None,
            "city": city,
            "country": "Nigeria",
            "phone": phone,
            "hours": jsonld.get("openingHours") if jsonld else None,
            "socials": outlinks.get("social_links", {}),
        }

        # Enrichment Stage 1: Try to get location from maps links first
        maps_links = outlinks.get("maps_links", [])
        if maps_links:
            log.info(f"[ENRICH] Processing {len(maps_links)} maps links")
            fields = enrich_from_maps_links(fields, maps_links)

        # Enrichment Stage 2: Geocode address if we still don't have coordinates
        if not fields.get('lat'):
            log.info(f"[ENRICH] Attempting geocoding")
            fields = enrich_location(fields)

        # Enrichment Stage 3: Fetch social metadata (optional, can be slow)
        if os.getenv("ENRICH_SOCIALS", "false").lower() == "true":
            if fields.get('socials'):
                log.info(f"[ENRICH] Fetching social metadata")
                fields = enrich_from_socials(fields, fields['socials'])

        # Store enriched fields
        item['enriched_fields'] = fields

        log.info(
            f"[ENRICH] Complete: lat={fields.get('lat')}, lng={fields.get('lng')}, precision={fields.get('geo_precision')}")

        return item


class ScorePipeline:
    """Stage 7: Scoring - Compute confidence score based on signals."""

    def process_item(self, item, spider):
        final_url = item.get("final_url") or item.get("url")
        source_id = item.get("source_id")
        discovery_type = item.get("discovery_type")
        fetched_at = item.get("fetched_at")

        # Get enriched fields
        fields = item.get('enriched_fields', {})
        extracted = item.get('extracted', {})

        # Build candidate
        candidate = self.build_candidate(
            discovery_type, fetched_at, fields, final_url,
            extracted, source_id
        )

        # Score the candidate
        score, why = score_page(signals=item['signals'], hint_boost_names=[])
        candidate["score"] = score
        candidate["signals"] = {**item['signals'], "why": why}

        item['score'] = score
        item['candidate'] = candidate

        log.info(f"[SCORE] {score:.2f} - {final_url} - reasons: {why.get('reasons')}")

        return item

    def build_candidate(self, discovery_type, fetched_at, fields, final_url,
                        extracted, source_id):
        readable = extracted.get('readable', {})
        jsonld = extracted.get('jsonld', {})
        outlinks = extracted.get('outlinks', {})
        phone_data = extracted.get('phone_data', {})
        city_hits = extracted.get('city_hits', [])

        return {
            "fields": fields,
            "score": 0.0,
            "signals": {},
            "evidence": {
                "source_url": final_url,
                "title": readable.get("title"),
                "excerpt": (readable.get("text", "")[:500] or "").strip(),
                "phone_snippet": phone_data.get('snippet') if phone_data else None,
                "city_snippets": [c.get('snippet') for c in city_hits],
                "jsonld": jsonld,
                "maps_links": outlinks.get("maps_links", []),
            },
            "provenance": {
                "source_id": source_id,
                "discovery_type": discovery_type,
                "fetched_at": fetched_at,
                "extractor_version": "v0.3.0",
            },
            "candidate_key": hashlib.sha256((final_url or "").encode("utf-8")).hexdigest(),
        }


class DedupePipeline:
    """Stage 6: Deduplication - Prevent publishing the same venue twice."""

    def __init__(self):
        self.cache_path = Path(".seen_cache")
        self.cache_path.mkdir(exist_ok=True)
        self.seen_keys = {}

    def process_item(self, item, spider):
        candidate = item.get('candidate', {})

        dedupe_data = dedupe_key(candidate)
        cache_key = self._generate_cache_key(dedupe_data)

        if self._is_duplicate(cache_key, dedupe_data):
            log.info(f"[DEDUPE] Duplicate found: {item.get('final_url')}")
            raise DropItem("Duplicate candidate")

        self._mark_seen(cache_key, dedupe_data)

        return item

    def _generate_cache_key(self, dedupe_data: dict) -> str | None:
        phone = dedupe_data.get('phone')
        if phone:
            return f"phone_{phone}"

        geohash = dedupe_data.get('geohash5')
        name = dedupe_data.get('name_norm', '').replace(' ', '_')[:30]
        if geohash and name:
            return f"geo_{geohash}_{name}"

        # FIXME: Should have a fallback that is not `returning None`
        return None

    def _is_duplicate(self, cache_key: str, dedupe_data: dict) -> bool:
        if not cache_key:
            return False

        if cache_key in self.seen_keys:
            cached_data = self.seen_keys[cache_key]
            if is_dup(dedupe_data, cached_data):
                return True

        key_file = self.cache_path / cache_key
        if key_file.exists():
            return True

        return False

    def _mark_seen(self, cache_key: str, dedupe_data: dict):
        if not cache_key:
            return

        self.seen_keys[cache_key] = dedupe_data

        key_file = self.cache_path / cache_key
        key_file.write_text("1")


class PublishPipeline:
    """Stage 8: Publishing - Send candidates to backend."""

    def process_item(self, item, spider):
        candidate = item.get('candidate', {})
        score = item.get('score', 0.0)
        signals = item.get('signals', {})

        has_key_fact = (
                signals.get("has_jsonld_restaurant") or
                signals.get("has_phone") or
                signals.get("has_maps_link") or
                signals.get("city_hit_near_food_terms")
        )

        if not has_key_fact:
            log.info(f"[PUBLISH] DROP: No key facts - {item.get('final_url')}")
            # raise DropItem("No key facts (JSON-LD, phone, or maps)")
            return item

        if score < ACCEPT_THRESHOLD:
            log.info(f"[PUBLISH] DROP: Score {score:.2f} < {ACCEPT_THRESHOLD} - {item.get('final_url')}")
            # raise DropItem(f"Score below threshold: {score:.2f}")
            return item

        def sanitize(obj):
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitize(i) for i in obj]
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            if callable(obj):
                return str(obj)
            return obj

        try:
            config = ClientConfig(base_url=API_BASE_URL, token=API_TOKEN)
            client = SyncBackendClient(config)
            
            publish_payload = sanitize(candidate)
            
            resp = client.publish_candidate(publish_payload)

            if resp.success:
                log.info(f"[PUBLISH] SUCCESS: score={score:.2f} url={item.get('final_url')}")
            else:
                log.error(f"[PUBLISH] FAILED: {item.get('final_url')} - {resp.error}")
                # Don't drop item if publish failed, just log it
                # raise DropItem(f"Publish failed: {resp.error}")

        except Exception as e:
            log.exception(f"[PUBLISH] ERROR: {item.get('final_url')} - {e}")
            raise DropItem(f"Publish error: {e}")

        return item


class StatsRecordingPipeline:
    """
    Record crawl statistics to database.

    This should run LAST so we know the final outcome of each page.
    """

    def __init__(self):
        self.manager = None
        self.crawl_run_id = None
        self.source_id = None

    def open_spider(self, spider):
        """Initialize on spider start."""
        # Get crawl_run_id from spider args
        self.crawl_run_id = getattr(spider, 'crawl_run_id', None)
        self.source_id = getattr(spider, 'source_id', None)

        if self.crawl_run_id:
            log.info(f"Stats recording enabled for crawl_run_id={self.crawl_run_id}")
            self.manager = SourceManager()
        else:
            log.warning("No crawl_run_id provided - stats recording disabled")

    def close_spider(self, spider):
        """Clean up on spider close."""
        if self.manager:
            # Mark crawl run as completed
            if self.crawl_run_id:
                self.manager.complete_crawl_run(self.crawl_run_id, success=True)

            self.manager.close()

    def process_item(self, item, spider):
        """Record page result if stats tracking is enabled."""
        if not self.manager or not self.crawl_run_id:
            return item

        # Extract info from item
        url = item.get('url') or item.get('final_url')
        status_code = item.get('status_code', 0)

        # Determine outcome
        gate_signals = item.get('gate_signals', {})
        passed_gate = gate_signals.get('gate_decision') == 'PASS'

        candidate = item.get('candidate', {})
        score = candidate.get('score') or item.get('score')

        signals = item.get('signals', {})

        # Check if published (if we got this far, it was published)
        published = True
        duplicate = False
        drop_reason = None

        # Prepare signals for recording - ensuring it's JSON serializable if needed
        # (The manager might already handle this, but we should be clean)

        # Record to database
        try:
            self.manager.record_page_result(
                crawl_run_id=self.crawl_run_id,
                url=url,
                status_code=status_code,
                passed_gate=passed_gate,
                score=score,
                published=published,
                duplicate=duplicate,
                drop_reason=drop_reason,
                signals=signals,
            )
        except Exception as e:
            log.error(f"Failed to record stats for {url}: {e}")

        return item

    def process_exception(self, request, exception, spider):
        """Record when a page fails."""
        if not self.manager or not self.crawl_run_id:
            return

        # Record failed page
        try:
            self.manager.record_page_result(
                crawl_run_id=self.crawl_run_id,
                url=request.url,
                status_code=0,
                passed_gate=False,
                drop_reason=f"Exception: {type(exception).__name__}",
            )
        except Exception as e:
            log.error(f"Failed to record exception for {request.url}: {e}")


class DropItemRecordingPipeline:
    """
    Catch dropped items and record why they were dropped.

    This needs to wrap other pipelines to capture their DropItem exceptions.
    """

    def __init__(self):
        self.manager = None
        self.crawl_run_id = None

    def open_spider(self, spider):
        self.crawl_run_id = getattr(spider, 'crawl_run_id', None)
        if self.crawl_run_id:
            self.manager = SourceManager()

    def close_spider(self, spider):
        if self.manager:
            self.manager.close()

    def process_item(self, item, spider):
        """Let items pass through."""
        return item

    # Example: Update generic spider to accept crawl_run_id
    """
    # In generic.py:
    
    def __init__(self, start_urls=None, discovery_type="manual", source_id=None, 
                 crawl_run_id=None, *args, **kwargs):
        super(GenericSpider, self).__init__(*args, **kwargs)
        if start_urls:
            self.start_urls = start_urls.split(',')
        else:
            self.start_urls = []
        self.discovery_type = discovery_type
        self.source_id = source_id
        self.crawl_run_id = crawl_run_id  # NEW
    """