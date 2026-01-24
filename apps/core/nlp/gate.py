"""
NLP Gate - Decides if a page is about an Amala/food venue

This is the critical filter that prevents junk from flowing through the pipeline.
It uses keyword detection + context analysis, not ML.

Returns: bool (pass/fail) + signals explaining why
"""

import re
import logging
from typing import Dict, Tuple, Any

log = logging.getLogger(__name__)


class CandidateGate:
    """
    Conservative gate that filters out non-restaurant content early.

    Philosophy: Better to miss a real venue than flood with noise.
    """

    # Core food keywords - Yoruba dishes and terms
    FOOD_KEYWORDS = {
        'amala', 'abula', 'ewedu', 'gbegiri', 'ila', 'ogunfe',
        'buka', 'mama put', 'iya', 'mama', 'joint', 'spot',
        'canteen', 'kitchen', 'restaurant', 'eatery', 'cafe'
    }

    # Context keywords that indicate a real venue (not a recipe)
    VENUE_KEYWORDS = {
        'location', 'address', 'phone', 'call', 'visit', 'located',
        'street', 'road', 'avenue', 'close', 'way', 'junction',
        'opposite', 'beside', 'near', 'behind', 'inside',
        'open', 'hours', 'monday', 'tuesday', 'wednesday', 'thursday',
        'friday', 'saturday', 'sunday', 'daily', 'weekday', 'weekend'
    }

    # Nigerian city names for location context
    NIGERIAN_CITIES = {
        'lagos', 'ibadan', 'abuja', 'port harcourt', 'kano',
        'abeokuta', 'ilorin', 'oshogbo', 'ogbomosho', 'ile-ife',
        'akure', 'benin', 'enugu', 'oyo', 'ikeja', 'surulere',
        'yaba', 'victoria island', 'lekki', 'ikorodu', 'agege'
    }

    # Patterns that suggest this is NOT a venue page
    RECIPE_PATTERNS = [
        r'\bingredients?\b',
        r'\bsteps?\b',
        r'\bprepare?\b',
        r'\bcook(ing)?\b',
        r'\brecipe\b',
        r'\bhow to make\b',
        r'\bminutes?\b.*\bhours?\b',  # cooking time
    ]

    # Patterns that suggest it IS a venue
    VENUE_PATTERNS = [
        r'google\.com/maps',
        r'maps\.app\.goo\.gl',
        r'instagram\.com',
        r'facebook\.com',
        r'\b0[7-9]\d{9}\b',  # Nigeria
        r'\+234\d{10}',  # International
    ]

    def __init__(self,
                 min_food_keywords: int = 1,
                 min_venue_keywords: int = 2,
                 min_total_words: int = 10):
        """
        Args:
            min_food_keywords: Minimum food-related keywords to pass
            min_venue_keywords: Minimum venue context keywords to pass
            min_total_words: Minimum content length
        """
        self.min_food_keywords = min_food_keywords
        self.min_venue_keywords = min_venue_keywords
        self.min_total_words = min_total_words

    def passes_gate(self, text: str, title: str = "") -> Tuple[bool, Dict]:
        """
        Decide if the content is about an Amala venue.

        Args:
            text: Main content text
            title: Page title (optional, but helps)

        Returns:
            tuple: (passes: bool, signals: dict)
        """
        if not text:
            return False, {"reason": "empty_content"}

        # Combine title and text for analysis
        full_text = f"{title} {text}".lower()
        words = full_text.split()

        # Early filter: too short
        if len(words) < self.min_total_words:
            return False, {
                "reason": "too_short",
                "word_count": len(words)
            }

        # Count the number of keyword hits
        food_hits = self._count_keywords(full_text, self.FOOD_KEYWORDS)
        venue_hits = self._count_keywords(full_text, self.VENUE_KEYWORDS)
        city_hits = self._count_keywords(full_text, self.NIGERIAN_CITIES)

        # Check if text matches recipe patterns
        has_recipe_pattern = any(
            re.search(p, full_text, re.IGNORECASE)
            for p in self.RECIPE_PATTERNS
        )

        # Check if text matches venue pattern
        has_venue_pattern = any(
            re.search(p, full_text, re.IGNORECASE)
            for p in self.VENUE_PATTERNS
        )

        # Build signals
        signals: Dict[str, Any] = {
            "food_keyword_count": food_hits,
            "venue_keyword_count": venue_hits,
            "city_mentions": city_hits,
            "has_venue_pattern": has_venue_pattern,
            "has_recipe_pattern": has_recipe_pattern,
            "word_count": len(words),
        }

        # Decision logic
        passes = self._evaluate(
            food_hits, venue_hits, city_hits,
            has_venue_pattern, has_recipe_pattern
        )

        if passes:
            signals["gate_decision"] = "PASS"
            log.info(f"Gate PASS: food={food_hits}, venue={venue_hits}, city={city_hits}")
        else:
            signals["gate_decision"] = "FAIL"
            signals["fail_reason"] = self._get_fail_reason(
                food_hits, venue_hits, has_recipe_pattern
            )
            log.debug(f"Gate FAIL: {signals['fail_reason']}")

        return passes, signals

    def _count_keywords(self, text: str, keywords: set) -> int:
        """Count how many keywords appear in text."""
        count = 0
        for keyword in keywords:
            if keyword in text:
                count += 1
        return count

    def _evaluate(self, food_hits, venue_hits, city_hits,
                  has_venue_pattern, has_recipe_pattern) -> bool:
        """
        The core decision logic. It passes or fails a candidate.

        Rules for passing a candidate:
        1. A potential candidate must have at least 1 food keyword.
        2. If a potential candidate has a venue pattern (maps/phone), the candidate is auto-passed.
        3. If a  potential candidate has a recipe pattern, the candidate requires stronger venue signals to pass.
        4. Otherwise, the potential candidate needs minimum venue + city context.
        """
        # Rule 1: Potential Candidate must mention food
        if food_hits < self.min_food_keywords:
            return False

        # Rule 2: For a potential candidate, strong venue evidence == instant pass
        if has_venue_pattern:
            return True

        # Rule 3: Recipe content needs extra venue proof
        if has_recipe_pattern:
            # Recipes mentioning venues are rare but valid
            # Require both venue keywords AND city
            return venue_hits >= 3 and city_hits >= 1

        # Rule 4: Standard case - need venue context
        has_venue_context = venue_hits >= self.min_venue_keywords
        has_location_context = city_hits >= 1

        return has_venue_context and has_location_context

    def _get_fail_reason(self, food_hits, venue_hits, has_recipe) -> str:
        """Explain why the gate failed."""
        if food_hits == 0:
            return "no_food_keywords"
        if has_recipe:
            return "recipe_content"
        if venue_hits < self.min_venue_keywords:
            return "insufficient_venue_context"
        return "insufficient_location_context"


# Convenience function for pipeline use
def passes_candidate_gate(text: str, title: str = "") -> Tuple[bool, Dict]:
    """
    Wrapper function for CandidateGate for easy use in pipelines.

    Example:
        passes, signals = passes_candidate_gate(text, title)
        if not passes:
            log.info(f"Dropped: {signals['fail_reason']}")
            return None
    """
    gate = CandidateGate()
    return gate.passes_gate(text, title)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test cases
    test_cases = [
        # Should PASS
        ("Mama Toyin's Amala Spot in Surulere. Located at 23 Adeniran Ogunsanya. Open daily. Call 08012345678",
         "Best Amala in Lagos", True),

        # Should PASS (has_maps_link)
        ("Check out this amala joint https://maps.google.com/12345",
         "New spot", True),

        # Should FAIL (recipe)
        ("How to make amala. Ingredients: yam flour, water. Steps: boil water, add flour...",
         "Amala Recipe", False),

        # Should FAIL (no venue context)
        ("I love amala so much. It's the best food ever.",
         "Amala thoughts", False),

        # Should FAIL (too short)
        ("Amala spot in Lagos", "Short", False),
    ]

    gate_ = CandidateGate()
    for text_, title_, expected in test_cases:
        passes_, signals_ = gate_.passes_gate(text_, title_)
        status = "✓" if passes_ == expected else "✗"
        print(f"{status} Expected: {expected}, Got: {passes_} - {signals_.get('fail_reason', 'PASS')}")
