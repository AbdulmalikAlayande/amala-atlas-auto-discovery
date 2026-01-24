"""
Social Media Enrichment - Extract metadata from social profiles

Handles:
- Instagram profiles (via public HTML scraping)
- Facebook pages (via Open Graph tags)
- No API keys required (scraping only)

Philosophy:
- Lightweight metadata only (name, description, followers)
- No authentication required
- Respect rate limits
- Fail gracefully
"""

import logging
import re
import time
from typing import Optional, Dict
import requests
from lxml import html as lxml_html

log = logging.getLogger(__name__)


class SocialEnricher:
    """
    Extract public metadata from social media profiles.

    WARNING: This uses HTML scraping, not official APIs.
    - Instagram may block requests
    - Rate limiting is essential
    - Data structure may change
    """

    def __init__(self, timeout: int = 10, rate_limit_delay: float = 2.0):
        """
        Args:
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between requests
        """
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0

        # User agent to avoid bot detection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def enrich_instagram(self, url: str) -> Optional[Dict]:
        """
        Fetch basic info from an Instagram profile.

        Extracts:
        - Username
        - Full name
        - Bio
        - Follower count (approximate)

        Returns:
            dict or None if fetch fails
        """
        if not url or 'instagram.com' not in url:
            return None

        self._rate_limit()

        try:
            log.debug(f"Fetching Instagram: {url}")

            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()

            tree = lxml_html.fromstring(response.text)

            # Extract from Open Graph tags (most reliable)
            og_data = self._extract_og_tags(tree)

            # Try to extract JSON-LD data
            jsonld_data = self._extract_instagram_jsonld(response.text)

            result = {
                'platform': 'instagram',
                'url': url,
                'username': self._extract_instagram_username(url),
                'name': og_data.get('og:title') or jsonld_data.get('name'),
                'description': og_data.get('og:description') or jsonld_data.get('description'),
                'image': og_data.get('og:image'),
            }

            # Clean up
            result = {k: v for k, v in result.items() if v}

            log.info(f"Instagram enriched: {result.get('username')}")
            return result

        except requests.RequestException as e:
            log.error(f"Instagram fetch failed: {e}")
            return None
        except Exception as e:
            log.error(f"Instagram parsing error: {e}")
            return None

    def enrich_facebook(self, url: str) -> Optional[Dict]:
        """
        Fetch basic info from a Facebook page.

        Extracts:
        - Page name
        - Description
        - Category

        Returns:
            dict or None if fetch fails
        """
        if not url or 'facebook.com' not in url:
            return None

        self._rate_limit()

        try:
            log.debug(f"Fetching Facebook: {url}")

            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()

            tree = lxml_html.fromstring(response.text)

            # Extract Open Graph data
            og_data = self._extract_og_tags(tree)

            result = {
                'platform': 'facebook',
                'url': url,
                'name': og_data.get('og:title'),
                'description': og_data.get('og:description'),
                'type': og_data.get('og:type'),
                'image': og_data.get('og:image'),
            }

            # Clean up
            result = {k: v for k, v in result.items() if v}

            log.info(f"Facebook enriched: {result.get('name')}")
            return result

        except requests.RequestException as e:
            log.error(f"Facebook fetch failed: {e}")
            return None
        except Exception as e:
            log.error(f"Facebook parsing error: {e}")
            return None

    def _extract_og_tags(self, tree) -> Dict:
        """Extract Open Graph meta tags."""
        og_data = {}

        for meta in tree.xpath('//meta[@property]'):
            prop = meta.get('property')
            content = meta.get('content')

            if prop and content and prop.startswith('og:'):
                og_data[prop] = content

        return og_data

    def _extract_instagram_jsonld(self, html: str) -> Dict:
        """
        Try to extract JSON-LD data from Instagram page.

        Instagram sometimes embeds structured data we can use.
        """
        try:
            # Look for JSON-LD script tags
            match = re.search(r'<script type="application/ld\+json">(.+?)</script>',
                              html, re.DOTALL)
            if match:
                import json
                data = json.loads(match.group(1))
                return data
        except Exception as ex:
            log.error(f"Error extracting JSON-LD: {ex}")

        return {}

    def _extract_instagram_username(self, url: str) -> Optional[str]:
        """Extract username from Instagram URL."""
        match = re.search(r'instagram\.com/([^/?]+)', url)
        return match.group(1) if match else None

    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()


def enrich_social_links(socials: dict) -> dict:
    """
    Enrich social media links with metadata.

    Args:
        socials: dict with keys like 'instagram', 'facebook' containing URLs

    Returns:
        dict with enriched metadata for each platform
    """
    enricher = SocialEnricher()
    enriched = {}

    # Instagram
    if socials.get('instagram'):
        result = enricher.enrich_instagram(socials['instagram'])
        if result:
            enriched['instagram'] = result

    # Facebook
    if socials.get('facebook'):
        result = enricher.enrich_facebook(socials['facebook'])
        if result:
            enriched['facebook'] = result

    return enriched


def enrich_from_socials(fields: dict, socials: dict) -> dict:
    """
    Enrich candidate fields from social media data.

    Can fill in missing:
    - Name (if empty)
    - Description/bio
    - Profile image

    Args:
        fields: Candidate fields dict (modified in-place)
        socials: Social links dict

    Returns:
        Updated fields dict
    """
    if not socials:
        return fields

    enriched = enrich_social_links(socials)

    # Use social data to fill gaps
    for platform, data in enriched.items():
        # Fill the name if missing
        if not fields.get('name') and data.get('name'):
            fields['name'] = data['name']
            log.info(f"Name filled from {platform}: {data['name']}")

        # Store social metadata
        if 'social_metadata' not in fields:
            fields['social_metadata'] = {}

        fields['social_metadata'][platform] = {
            'name': data.get('name'),
            'description': data.get('description'),
        }

    return fields


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    enricher_ = SocialEnricher()

    test_profiles = [
        # Instagram (public profile)
        "https://instagram.com/foodieinlagos",

        # Facebook page
        "https://www.facebook.com/EatDrinkLagos",
    ]

    print("\n=== Social Media Enrichment Tests ===\n")

    for url_ in test_profiles:
        print(f"URL: {url_}")

        if 'instagram.com' in url_:
            result_ = enricher_.enrich_instagram(url_)
        elif 'facebook.com' in url_:
            result_ = enricher_.enrich_facebook(url_)
        else:
            result_ = None

        if result_:
            print(f"  ✓ Platform: {result_['platform']}")
            print(f"    Name: {result_.get('name', 'N/A')}")
            print(f"    Description: {result_.get('description', 'N/A')[:100]}...")
        else:
            print(f"  ✗ Could not enrich")
        print()
