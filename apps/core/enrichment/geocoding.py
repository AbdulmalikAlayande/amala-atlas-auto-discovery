"""
Geocoding Service - Convert addresses to coordinates

Nigerian Context:
- Informal addresses ("behind Total station")
- Missing street numbers
- Landmarks as addresses
- City inference often needed

Strategy:
1. Try a structured address first
2. Fallback to city centroid
3. Track precision level
4. Never fail - always return something
"""

import logging
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple

import requests

log = logging.getLogger(__name__)


@dataclass
class GeocodingResult:
    """Geocoding result with precision tracking."""
    lat: float
    lng: float
    precision: str  # "exact", "street", "city", "fallback"
    source: str  # "nominatim", "city_centroid", "manual"
    formatted_address: Optional[str] = None
    place_id: Optional[str] = None


class GeocodingService:
    """
    Geocoding service with fallback strategy.

    Uses OpenStreetMap Nominatim (free, no API key needed).
    Falls back to city centroids for Nigerian cities.
    """

    # Nigerian city centroids (fallback)
    CITY_CENTROIDS = {
        'lagos': (6.5244, 3.3792),
        'ibadan': (7.3775, 3.9470),
        'abuja': (9.0765, 7.3986),
        'port harcourt': (4.8156, 7.0498),
        'kano': (12.0022, 8.5919),
        'abeokuta': (7.1475, 3.3619),
        'ilorin': (8.4966, 4.5426),
        'oshogbo': (7.7667, 4.5667),
        'ikeja': (6.6018, 3.3515),
        'surulere': (6.5027, 3.3597),
        'yaba': (6.5158, 3.3704),
        'victoria island': (6.4281, 3.4219),
        'lekki': (6.4474, 3.5617),
        'ikorodu': (6.6194, 3.5105),
    }

    def __init__(self,
                 nominatim_url: str = "https://nominatim.openstreetmap.org/search",
                 timeout: int = 10,
                 rate_limit_delay: float = 1.0):
        """
        Args:
            nominatim_url: Nominatim API endpoint
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between requests (Nominatim policy)
        """
        self.nominatim_url = nominatim_url
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0

    def geocode(self,
                address: Optional[str] = None,
                city: Optional[str] = None,
                country: str = "Nigeria") -> Optional[GeocodingResult]:
        """
        Geocode an address with fallback strategy.

        Args:
            address: Street address (can be informal)
            city: City name
            country: Country (default: Nigeria)

        Returns:
            GeocodingResult or None if all methods fail
        """

        # Strategy 1: Try full address if provided
        if address:
            result = self._geocode_nominatim(address, city, country)
            if result:
                return result

        # Strategy 2: Try city-level geocoding
        if city:
            result = self._geocode_city(city, country)
            if result:
                return result

        # Strategy 3: Fallback to known city centroid
        if city:
            result = self._get_city_centroid(city)
            if result:
                return result

        log.warning(f"Geocoding failed for address='{address}', city='{city}'")
        return None

    def _geocode_nominatim(self,
                           address: str,
                           city: Optional[str],
                           country: str) -> Optional[GeocodingResult]:
        """
        Use Nominatim to geocode an address.

        Nominatim usage policy requires:
        - User-Agent header
        - Max 1 request per second
        - No heavy querying
        """

        # Respect rate limit
        self._rate_limit()

        # Build query
        query_parts = [address]
        if city:
            query_parts.append(city)
        query_parts.append(country)
        query = ", ".join(query_parts)

        params = {
            'q': query,
            'format': 'json',
            'limit': 1,
            'addressdetails': 1,
        }

        headers = {
            'User-Agent': 'AmalaAtlas/1.0 (Auto-Discovery Service)'
        }

        try:
            log.debug(f"Geocoding with Nominatim: {query}")
            response = requests.get(
                self.nominatim_url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()

            results = response.json()
            if not results:
                log.debug(f"Nominatim: No results for '{query}'")
                return None

            # Take first result
            result = results[0]
            lat = float(result['lat'])
            lng = float(result['lon'])

            # Determine precision based on OSM type
            osm_type = result.get('type', '')
            precision = self._determine_precision(osm_type, result.get('address', {}))

            log.info(f"Geocoded: {query} -> ({lat}, {lng}) [{precision}]")

            return GeocodingResult(
                lat=lat,
                lng=lng,
                precision=precision,
                source="nominatim",
                formatted_address=result.get('display_name'),
                place_id=result.get('place_id')
            )

        except requests.RequestException as e:
            log.error(f"Nominatim request failed: {e}")
            return None
        except (KeyError, ValueError) as e:
            log.error(f"Nominatim response parsing failed: {e}")
            return None

    def _geocode_city(self, city: str, country: str) -> Optional[GeocodingResult]:
        """
        Geocode at city level (less precise but reliable).
        """
        query = f"{city}, {country}"

        self._rate_limit()

        params = {
            'q': query,
            'format': 'json',
            'limit': 1,
        }

        headers = {
            'User-Agent': 'AmalaAtlas/1.0 (Auto-Discovery Service)'
        }

        try:
            response = requests.get(
                self.nominatim_url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()

            results = response.json()
            if not results:
                return None

            result = results[0]
            lat = float(result['lat'])
            lng = float(result['lon'])

            log.info(f"City-level geocoding: {city} -> ({lat}, {lng})")

            return GeocodingResult(
                lat=lat,
                lng=lng,
                precision="city",
                source="nominatim_city",
                formatted_address=result.get('display_name')
            )

        except Exception as e:
            log.error(f"City geocoding failed for {city}: {e}")
            return None

    def _get_city_centroid(self, city: str) -> Optional[GeocodingResult]:
        """
        Use hardcoded city centroid as last resort.
        """
        city_lower = city.lower().strip()

        if city_lower in self.CITY_CENTROIDS:
            lat, lng = self.CITY_CENTROIDS[city_lower]
            log.info(f"Using city centroid for {city}: ({lat}, {lng})")

            return GeocodingResult(
                lat=lat,
                lng=lng,
                precision="city_centroid",
                source="fallback",
                formatted_address=f"{city}, Nigeria"
            )

        return None

    def _determine_precision(self, osm_type: str, address_components: dict) -> str:
        """
        Determine precision level from OSM type and address components.
        """
        # Check if we have house number
        if address_components.get('house_number'):
            return "exact"

        # Street-level
        if osm_type in ('road', 'residential', 'footway', 'path'):
            return "street"

        # Neighborhood/suburb
        if osm_type in ('suburb', 'neighbourhood', 'quarter'):
            return "neighborhood"

        # City-level
        if osm_type in ('city', 'town', 'village'):
            return "city"

        # Default
        return "approximate"

    def _rate_limit(self):
        """Enforce rate limiting for Nominatim."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()


@lru_cache(maxsize=1000)
def geocode_cached(address: str, city: str, country: str = "Nigeria") -> Optional[Tuple]:
    """
    Cached geocoding to avoid repeated API calls.

    Returns: (lat, lng, precision, source) or None
    """
    service = GeocodingService()
    result = service.geocode(address, city, country)

    if result:
        return (result.lat, result.lng, result.precision, result.source)
    return None


# Convenience function for pipeline use
def enrich_location(fields: dict) -> dict:
    """
    Enrich candidate fields with geocoding data.

    Args:
        fields: Candidate fields dict (modified in-place)

    Returns:
        Updated fields dict with lat, lng, geo_precision
    """
    address = fields.get("address")
    city = fields.get("city")

    if not address and not city:
        log.debug("No address or city for geocoding")
        return fields

    service = GeocodingService()
    result = service.geocode(address=address, city=city)

    if result:
        fields["lat"] = result.lat
        fields["lng"] = result.lng
        fields["geo_precision"] = result.precision
        fields["formatted_address"] = result.formatted_address

        log.info(f"Enriched location: {city or address} -> ({result.lat}, {result.lng})")
    else:
        log.warning(f"Could not geocode: address={address}, city={city}")

    return fields


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    service = GeocodingService()

    test_cases = [
        # Full address
        ("23 Adeniran Ogunsanya Street", "Surulere", "Nigeria"),

        # Informal address
        ("Behind Total Filling Station", "Lagos", "Nigeria"),

        # City only
        (None, "Ibadan", "Nigeria"),

        # Unknown city (should use centroid)
        (None, "Lagos", "Nigeria"),
    ]

    print("\n=== Geocoding Tests ===\n")
    for address, city, country in test_cases:
        print(f"Input: {address or '(none)'}, {city}")
        result = service.geocode(address, city, country)

        if result:
            print(f"  ✓ ({result.lat:.4f}, {result.lng:.4f})")
            print(f"    Precision: {result.precision}")
            print(f"    Source: {result.source}")
            print(f"    Address: {result.formatted_address}")
        else:
            print(f"  ✗ Failed to geocode")
        print()
