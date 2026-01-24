import logging
import re
from apps.core.utils.dataextractor import DataExtractor, ExtractionPatterns


logger = logging.getLogger(__name__)

"""regex 0[7-9]\d{9}"""
def find_phone(text) -> dict | None:
    if not text:
        return None
    data_extractor = DataExtractor()
    matched_phone_numbers = data_extractor.extract_phone_numbers(text)

    if not matched_phone_numbers:
        return None

    phone = matched_phone_numbers[0]

    # Extract context snippet
    snippet = ""
    try:
        # Find first occurrence for snippet
        # We need to be careful as DataExtractor._clean_phone_number might have changed the format
        # For simplicity, we search for the first digit sequence of the phone in the text
        first_match = re.search(re.escape(phone.replace("-", "")), re.sub(r'\D', '', text))
        if not first_match:
            # Fallback to simple search if cleaning made it hard to find
            match = re.search(re.escape(phone), text)
        else:
            # This is a bit complex because the text is original and phone is cleaned
            # Let's just use a simpler approach: search for the raw matches from findall
            raw_matches = ExtractionPatterns.PHONE_PATTERNS.finditer(text)
            for m in raw_matches:
                start, end = m.span()
                snippet = text[max(0, start - 50):min(len(text), end + 50)].strip()
                break
    except Exception as ex:
        logger.error(f"Error extracting phone snippet: {ex}")

    return {"value": phone, "snippet": snippet}


"""returns [{"value": "street", "snippet": "..."}]"""


def find_address_tokens(text) -> list[dict]:
    ADDRESS_TOKENS = [
        "street", "st.", "road", "rd", "close", "avenue", "junction",
        "bus stop", "market", "opposite", "beside", "near", "way"
    ]
    if not text: return []
    low = text.lower()
    results = []
    for token in ADDRESS_TOKENS:
        if token in low:
            start = low.find(token)
            snippet = text[max(0, start - 50):min(len(text), start + len(token) + 50)].strip()
            results.append({"value": token, "snippet": snippet})
    return results


"""returns [{"value": "Lagos", "snippet": "..."}]"""


def find_city_hits(text, target_cities: list[str]) -> list[dict]:
    if not text: return []
    low = text.lower()
    results = []
    for city in target_cities:
        if city.lower() in low:
            start = low.find(city.lower())
            snippet = text[max(0, start - 50):min(len(text), start + len(city) + 50)].strip()
            results.append({"value": city, "snippet": snippet})
    return results
