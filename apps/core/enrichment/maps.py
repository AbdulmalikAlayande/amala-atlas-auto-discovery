"""
Google Maps Link Parser - Extract coordinates and place info from maps URLs

Handles various formats:
- https://maps.google.com/?q=6.5244,3.3792
- https://www.google.com/maps/place/Name/@6.5244,3.3792,17z
- https://maps.app.goo.gl/abc123 (short links)
- https://goo.gl/maps/xyz (legacy short links)

Strategy:
1. Parse coordinates directly from URL if present
2. Expand short links and extract
3. Extract place_id for future enrichment
4. Never make expensive API calls during crawling
"""

import re
import logging
from typing import Optional, Dict
from urllib.parse import urlparse, parse_qs
import requests

log = logging.getLogger(__name__)


class MapsLinkParser:
    """Parse Google Maps links to extract location data."""

    # Regex patterns for different map URL formats
    PATTERNS = {
        # https://maps.google.com/?q=6.5244,3.3792
        'query_latlong': re.compile(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)'),

        # https://www.google.com/maps/place/Name/@6.5244,3.3792,17z
        'place_latlong': re.compile(r'@(-?\d+\.?\d*),(-?\d+\.?\d*),(\d+)z'),

        # https://www.google.com/maps/@6.5244,3.3792,17z
        'simple_latlong': re.compile(r'/@(-?\d+\.?\d*),(-?\d+\.?\d*),(\d+)z'),

        # Extract place_id
        'place_id': re.compile(r'place/([^/]+)'),

        # CID (legacy place identifier)
        'cid': re.compile(r'cid=(\d+)'),
    }

    # Short link domains
    SHORT_LINK_DOMAINS = {
        'maps.app.goo.gl',
        'goo.gl',
        'g.page',
    }

    def __init__(self,
                 expand_short_links: bool = True,
                 timeout: int = 5):
        """
        Args:
            expand_short_links: Whether to follow redirects for short links
            timeout: Request timeout for short link expansion
        """
        self.expand_short_links = expand_short_links
        self.timeout = timeout

    def parse(self, url: str) -> Optional[Dict]:
        """
        Parse a Google Maps URL.

        Args:
            url: Google Maps URL (any format)

        Returns:
            dict with: {lat, lng, precision, place_id, original_url}
            or None if parsing fails
        """
        if not url:
            return None

        # Expand short links first
        if self._is_short_link(url):
            if self.expand_short_links:
                expanded = self._expand_short_link(url)
                if expanded:
                    url = expanded
                else:
                    log.warning(f"Failed to expand short link: {url}")
                    return None
            else:
                log.debug(f"Short link expansion disabled: {url}")
                return None

        # Try each pattern
        for pattern_name, pattern in self.PATTERNS.items():
            match = pattern.search(url)
            if match and pattern_name in ('query_latlong', 'place_latlong', 'simple_latlong'):
                lat = float(match.group(1))
                lng = float(match.group(2))

                # Extract zoom level for precision estimation
                zoom = int(match.group(3)) if len(match.groups()) >= 3 else None
                precision = self._estimate_precision(zoom)

                # Try to extract place_id
                place_id = self._extract_place_id(url)

                log.info(f"Parsed maps link: ({lat}, {lng}) from {pattern_name}")

                return {
                    'lat': lat,
                    'lng': lng,
                    'precision': precision,
                    'place_id': place_id,
                    'source': 'maps_link',
                    'original_url': url,
                }

        log.warning(f"Could not parse maps link: {url}")
        return None

    def _is_short_link(self, url: str) -> bool:
        """Check if URL is a Google Maps short link."""
        try:
            parsed = urlparse(url)
            return parsed.netloc in self.SHORT_LINK_DOMAINS
        except Exception as ex:
            log.error(f"Failed to parse maps link: {url}: {ex}")
            return False

    def _expand_short_link(self, url: str) -> Optional[str]:
        """
        Expand a short link by following redirects.

        This is necessary because short links don't contain coordinates.
        """
        try:
            log.debug(f"Expanding short link: {url}")

            response = requests.head(
                url,
                allow_redirects=True,
                timeout=self.timeout
            )

            final_url = response.url
            log.debug(f"Expanded to: {final_url}")

            return final_url

        except requests.RequestException as e:
            log.error(f"Short link expansion failed: {e}")
            return None

    def _extract_place_id(self, url: str) -> Optional[str]:
        """
        Extract Google Place ID from URL.

        Format: ChIJN1t_tDeuEmsRUsoyG83frY4
        This can be used later for enrichment via Places API.
        """
        # Try place_id parameter
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'place_id' in params:
            return params['place_id'][0]

        # Try extracting from place/ path
        match = self.PATTERNS['place_id'].search(url)
        if match:
            place_name_or_id = match.group(1)
            # Place IDs start with ChIJ
            if place_name_or_id.startswith('ChIJ'):
                return place_name_or_id

        return None

    def _estimate_precision(self, zoom: Optional[int]) -> str:
        """
        Estimate precision from zoom level.

        Google Maps zoom levels:
        20-21 = building level (~1m)
        17-19 = street level (~10m)
        14-16 = neighborhood (~100m)
        11-13 = city level (~1km)
        """
        if not zoom:
            return "unknown"

        if zoom >= 20:
            return "building"
        elif zoom >= 17:
            return "street"
        elif zoom >= 14:
            return "neighborhood"
        elif zoom >= 11:
            return "city"
        else:
            return "region"


def parse_maps_links(links: list) -> Optional[Dict]:
    """
    Parse multiple maps links and return the best result.

    Args:
        links: List of Google Maps URLs

    Returns:
        Best parsing result (highest precision) or None
    """
    if not links:
        return None

    parser = MapsLinkParser()
    results = []

    for link in links:
        result = parser.parse(link)
        if result:
            results.append(result)

    if not results:
        return None

    # Return result with best precision
    precision_order = {
        'building': 5,
        'street': 4,
        'neighborhood': 3,
        'city': 2,
        'region': 1,
        'unknown': 0,
    }

    best = max(results, key=lambda r: precision_order.get(r['precision'], 0))
    log.info(f"Best maps result: ({best['lat']}, {best['lng']}) [{best['precision']}]")

    return best


def enrich_from_maps_links(fields: dict, maps_links: list) -> dict:
    """
    Enrich candidate fields with data from Google Maps links.

    Only updates fields if they're currently empty and maps data is better.

    Args:
        fields: Candidate fields dict (modified in-place)
        maps_links: List of Google Maps URLs from evidence

    Returns:
        Updated fields dict
    """
    if not maps_links:
        return fields

    result = parse_maps_links(maps_links)
    if not result:
        return fields

    # Only update if we don't have location data, or maps data is more precise
    current_precision = fields.get('geo_precision', 'unknown')
    maps_precision = result['precision']

    precision_order = {
        'building': 5, 'street': 4, 'neighborhood': 3,
        'city': 2, 'region': 1, 'unknown': 0,
    }

    current_score = precision_order.get(current_precision, 0)
    maps_score = precision_order.get(maps_precision, 0)

    if maps_score > current_score:
        fields['lat'] = result['lat']
        fields['lng'] = result['lng']
        fields['geo_precision'] = result['precision']
        fields['maps_place_id'] = result.get('place_id')

        log.info(f"Updated location from maps link: {maps_precision} precision")

    return fields


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser_ = MapsLinkParser()

    test_urls = [
        # Direct coordinates
        "https://maps.google.com/?q=6.5244,3.3792",

        # Place with coordinates
        "https://www.google.com/maps/place/Iya+Toyin/@6.5244,3.3792,17z",

        # Simple format
        "https://www.google.com/maps/@6.5244,3.3792,15z",

        # Short link (will fail without expansion in test)
        "https://maps.app.goo.gl/abc123",

        # Invalid
        "https://www.google.com/search?q=amala",
    ]

    print("\n=== Maps Link Parsing Tests ===\n")

    for url_ in test_urls:
        print(f"URL: {url_}")
        result_ = parser_.parse(url_)

        if result_:
            print(f"Lat: {result_['lat']}")
            print(f"Lng: {result_['lng']}")
            print(f"Precision: {result_['precision']}")
            if result_.get('place_id'):
                print(f"    Place ID: {result_['place_id']}")
        else:
            print(f"  ✗ Could not parse")
        print()
