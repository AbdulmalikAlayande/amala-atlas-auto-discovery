import hashlib, logging, os
from pathlib import Path

from dotenv import load_dotenv

from apps.core.publisher.client import publish_candidate
from apps.core.extractor.article import extract_readable, extract_jsonld, extract_outlinks
from apps.core.nlp.fields import find_phone, find_address_tokens, find_city_hits
from apps.core.scoring.rules import score as score_page

log = logging.getLogger(__name__)
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_TOKEN    = os.getenv("API_TOKEN", "dev-token")
ACCEPT_THRESHOLD = float(os.getenv("ACCEPT_THRESHOLD", "0.45"))
TARGET_CITIES = [c.strip() for c in os.getenv("TARGET_CITIES","Lagos,Ibadan").split(",") if c.strip()]

class ExtractSignalsPipeline:

    def process_item(self, item, spider):
        html = item.get("html") or ""
        final_url = item.get("final_url") or item.get("url")

        readable = extract_readable(html)
        jsonld = extract_jsonld(html)
        outlinks = extract_outlinks(html, final_url)
        phone = find_phone(readable.get("text", ""))
        city_hits = find_city_hits(readable.get("text", ""), TARGET_CITIES)

        signals = self.build_signals(jsonld, outlinks, readable, phone, city_hits)
        log.info(f"Signals: {signals}")
        item['signals'] = signals
        return item

    def build_signals(self, jsonld, outlinks, readable, phone, city_hits):
        print(self.__repr__())
        return {
            "has_jsonld_restaurant": bool(jsonld and jsonld.get("name") and jsonld.get("address")),
            "has_maps_link": outlinks.get("maps_links"),
            "has_phone": bool(phone),
            "recent_content": bool(readable.get("is_recent")),
            "city_hit_near_food_terms": bool(city_hits),
            "listicle_penalty": "list" in (readable.get("title", "").lower()),
        }


class ScorePipeline:

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
        addr_tokens = find_address_tokens(readable.get("text", ""))
        city_hits = find_city_hits(readable.get("text", ""), TARGET_CITIES)

        # Fields: Best-effort, To be improved later from JSON-LD/regex
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
            "lat": None, "lng": None, "geo_precision": "unknown"
        }

        candidate = self.build_candidate(discovery_type, fetched_at, fields, final_url, jsonld, outlinks, readable, source_id)
        score, why = score_page(signals=item.signals, hint_boost_names=[])
        candidate["score"] = score
        candidate["signals"] = {**item.signals, "why": why}

        item['score'] = score
        item['candidate'] = candidate
        return item

    def build_candidate(self, discovery_type, fetched_at, fields, final_url, jsonld, outlinks, readable,
                        source_id):
        print(self.__repr__())
        return {
            "fields": fields,
            "score": 0.0,
            "signals": {},
            "evidence": {
                "source_url": final_url,
                "title": readable.get("title"),
                "excerpt": (readable.get("text","")[:500] or "").strip(),
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

    def process_item(self, item, spider):
        seen_key = f"seen_{item.candidate['candidate_key']}"
        cache_path = Path(".seen_cache")
        cache_path.mkdir(exist_ok=True)

        key_file = cache_path / seen_key
        if key_file.exists():
            log.info(f"[DEDUPE] already published {item.final_url}")
            return item
        key_file.write_text("1")
        return item


class PublishPipeline:

    def process_item(self, item, spider):

        has_key_fact = item.signals["has_jsonld_restaurant"] or item.signals["has_phone"] or item.signals["has_maps_link"]
        if not (has_key_fact and item.score >= ACCEPT_THRESHOLD):
            log.info(f"[DROP] score: {item.score:.2f}, url: {item.final_url}")
            return item

        try:
            resp = publish_candidate(API_BASE_URL, API_TOKEN, item.candidate)
            log.info(f"[PUBLISH OK] score={item.score:.2f} url={item.final_url} id={resp.get('id')}")
        except Exception as e:
            log.exception(f"[PUBLISH FAIL] {item.final_url}: {e}")

        return item