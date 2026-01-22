"""
Deduplication Engine - Identifies when the same venue appears across sources

Nigerian Context Challenges:
- Same place: "Iya Toyin", "Mama Toyin", "Mama T"
- Phone numbers change
- Addresses are informal and inconsistent
- No guaranteed unique identifiers

Strategy:
- Phone exact match (strongest signal)
- Geographic proximity + name similarity (weaker but common)
- Conservative: only merge when confident
"""

import re
import logging
from typing import Dict, Optional, List, Tuple
from difflib import SequenceMatcher
import geohash2

log = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """
    Normalize restaurant names for comparison.

    Handles:
    - Case differences
    - Common prefixes (Iya, Mama, Buka)
    - Punctuation
    - Extra whitespace
    """
    if not name:
        return ""

    # Lowercase
    name = name.lower().strip()

    # Remove common prefixes that vary
    prefixes = ['iya ', 'mama ', 'buka ', 'mr ', 'mrs ', 'chef ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]

    # Remove punctuation and extra spaces
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def normalize_phone(phone: str) -> Optional[str]:
    """
    Normalize Nigerian phone numbers to comparable format.

    Formats handled:
    - 08012345678
    - +2348012345678
    - 0801 234 5678
    - +234 801 234 5678
    """
    if not phone:
        return None

    # Remove all non-digits
    digits = re.sub(r'\D', '', phone)

    # Convert to standard 11-digit Nigerian format
    if digits.startswith('234') and len(digits) == 13:
        # +234 8012345678 -> 08012345678
        digits = '0' + digits[3:]
    elif digits.startswith('234') and len(digits) == 14:
        # +234 08012345678 -> 08012345678
        digits = digits[4:]

    # Valid Nigerian mobile: 11 digits starting with 0
    if len(digits) == 11 and digits[0] == '0':
        return digits

    return None


def extract_geohash(lat: float, lng: float, precision: int = 5) -> Optional[str]:
    """
    Generate geohash for location.

    Precision 5 = ~5km accuracy (good for same neighborhood)
    Precision 6 = ~1km accuracy (too strict for informal addresses)
    """
    if not lat or not lng:
        return None

    try:
        return geohash2.encode(lat, lng, precision=precision)
    except Exception as e:
        log.error(f"Geohash error: {e}")
        return None


def name_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity between two names (0.0 to 1.0).

    Uses SequenceMatcher for fuzzy matching.
    Handles Nigerian name variations well.
    """
    if not name1 or not name2:
        return 0.0

    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    if not norm1 or not norm2:
        return 0.0

    return SequenceMatcher(None, norm1, norm2).ratio()


def dedupe_key(candidate: dict) -> dict:
    """
    Extract dedupe-relevant fields from candidate.

    Args:
        candidate: Full candidate dict with fields, evidence, etc.

    Returns:
        dict with: {phone, name_norm, geohash5, lat, lng}
    """
    fields = candidate.get("fields", {})

    # Extract phone
    phone_raw = fields.get("phone")
    phone_norm = normalize_phone(phone_raw) if phone_raw else None

    # Extract name
    name_raw = fields.get("name")
    name_norm = normalize_name(name_raw) if name_raw else ""

    # Extract location
    lat = fields.get("lat")
    lng = fields.get("lng")
    geohash5 = extract_geohash(lat, lng, precision=5) if lat and lng else None

    return {
        "phone": phone_norm,
        "name_norm": name_norm,
        "geohash5": geohash5,
        "lat": lat,
        "lng": lng,
    }


def is_dup(a: dict, b: dict,
           phone_match_threshold: float = 1.0,
           name_match_threshold: float = 0.85,
           require_geohash: bool = True) -> bool:
    """
    Determine if two candidates represent the same venue.

    Rules (in priority order):
    1. Exact phone match = duplicate (strongest signal)
    2. Same geohash + similar name (>85%) = duplicate
    3. Otherwise = not duplicate

    Args:
        a, b: Candidate dedupe keys (from dedupe_key())
        phone_match_threshold: Must be 1.0 (exact match only)
        name_match_threshold: Minimum similarity for name match
        require_geohash: If True, requires location for geo+name match

    Returns:
        True if duplicate, False otherwise
    """

    # Rule 1: Phone exact match (most reliable in Nigerian context)
    if a.get("phone") and b.get("phone"):
        if a["phone"] == b["phone"]:
            log.info(f"Duplicate found: phone match {a['phone']}")
            return True

    # Rule 2: Geographic proximity + name similarity
    # Both must have location data
    if a.get("geohash5") and b.get("geohash5"):
        # Same geohash = within ~5km
        if a["geohash5"] == b["geohash5"]:
            # Check name similarity
            name_sim = name_similarity(a.get("name_norm", ""),
                                       b.get("name_norm", ""))

            if name_sim >= name_match_threshold:
                log.info(f"Duplicate found: geo+name (sim={name_sim:.2f})")
                return True
            else:
                log.debug(f"Same geohash but names differ (sim={name_sim:.2f})")

    elif require_geohash:
        # No location data = can't determine via geo
        # Conservative: assume not duplicate
        log.debug("No geohash available, cannot verify geo proximity")

    return False


def find_duplicates_in_batch(candidates: List[dict]) -> List[Tuple[int, int]]:
    """
    Find all duplicate pairs in a batch of candidates.

    Args:
        candidates: List of full candidate dicts

    Returns:
        List of (index_a, index_b) tuples representing duplicates
    """
    duplicates = []
    keys = [dedupe_key(c) for c in candidates]

    # O(n²) comparison (fine for batch sizes < 1000)
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            if is_dup(keys[i], keys[j]):
                duplicates.append((i, j))

    return duplicates


def merge_candidates(primary: dict, duplicate: dict) -> dict:
    """
    Merge duplicate candidate into primary.

    Strategy:
    - Keep primary's fields
    - Aggregate evidence from both
    - Track all source URLs
    - Update provenance

    Args:
        primary: The candidate to keep
        duplicate: The candidate to merge in

    Returns:
        Merged candidate dict
    """
    merged = primary.copy()

    # Aggregate evidence
    primary_evidence = merged.get("evidence", {})
    dup_evidence = duplicate.get("evidence", {})

    # Collect all source URLs
    sources = set()
    if primary_evidence.get("source_url"):
        sources.add(primary_evidence["source_url"])
    if dup_evidence.get("source_url"):
        sources.add(dup_evidence["source_url"])

    primary_evidence["all_sources"] = list(sources)
    primary_evidence["duplicate_count"] = primary_evidence.get("duplicate_count", 0) + 1

    # Update provenance
    provenance = merged.get("provenance", {})
    provenance["merged_from"] = provenance.get("merged_from", [])
    provenance["merged_from"].append({
        "source_id": duplicate.get("provenance", {}).get("source_id"),
        "url": dup_evidence.get("source_url"),
    })

    merged["evidence"] = primary_evidence
    merged["provenance"] = provenance

    return merged


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test name normalization
    print("\n=== Name Normalization Tests ===")
    test_names = [
        ("Iya Toyin's Place", "iya toyins place"),
        ("Mama T Buka", "t buka"),
        ("Mr. Bigg's", "biggs"),
    ]
    for original, expected in test_names:
        normalized = normalize_name(original)
        print(f"'{original}' -> '{normalized}' (expected: '{expected}')")

    # Test phone normalization
    print("\n=== Phone Normalization Tests ===")
    test_phones = [
        ("08012345678", "08012345678"),
        ("+2348012345678", "08012345678"),
        ("0801 234 5678", "08012345678"),
        ("+234 801 234 5678", "08012345678"),
    ]
    for original, expected in test_phones:
        normalized = normalize_phone(original)
        status = "✓" if normalized == expected else "✗"
        print(f"{status} '{original}' -> '{normalized}' (expected: '{expected}')")

    # Test name similarity
    print("\n=== Name Similarity Tests ===")
    test_pairs = [
        ("Iya Toyin", "Mama Toyin", 0.7),  # Should be similar
        ("Mama T", "Mama Toyin", 0.5),  # Partial match
        ("Buka Stop", "Chicken Republic", 0.1),  # Different
    ]
    for name1, name2, min_sim in test_pairs:
        sim = name_similarity(name1, name2)
        status = "✓" if sim >= min_sim else "✗"
        print(f"{status} '{name1}' vs '{name2}': {sim:.2f} (min: {min_sim})")

    # Test deduplication
    print("\n=== Deduplication Tests ===")

    candidate_a = {
        "fields": {
            "name": "Iya Toyin's Place",
            "phone": "08012345678",
            "lat": 6.5244,
            "lng": 3.3792,
        }
    }

    candidate_b = {
        "fields": {
            "name": "Mama Toyin Buka",
            "phone": "08012345678",  # Same phone
            "lat": 6.5240,
            "lng": 3.3790,
        }
    }

    candidate_c = {
        "fields": {
            "name": "Mama T",
            "phone": "08099999999",  # Different phone
            "lat": 6.5243,  # Same area
            "lng": 3.3791,
        }
    }

    key_a = dedupe_key(candidate_a)
    key_b = dedupe_key(candidate_b)
    key_c = dedupe_key(candidate_c)

    print(f"A vs B (same phone): {is_dup(key_a, key_b)} (expected: True)")
    print(f"A vs C (same area, similar name): {is_dup(key_a, key_c)} (expected: True)")
    print(f"B vs C (different phone, different name): {is_dup(key_b, key_c)} (expected: False)")
