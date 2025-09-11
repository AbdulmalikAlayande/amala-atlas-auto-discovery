from apps.core.utils.dataextractor import DataExtractor

"""regex 0[7-9]\d{9}"""
def find_phone(text) -> str|None:
    if not text:
        return None
    data_extractor = DataExtractor()
    matched_phone_numbers = data_extractor.extract_phone_numbers(text)
    return matched_phone_numbers if len(matched_phone_numbers) > 0 else None


"""returns [“street”, “road”, “bus stop”, “opposite”, ...]"""
def find_address_tokens(text) -> list[str]:
    ADDRESS_TOKENS = [
        "street", "st.", "road", "rd", "close", "avenue", "junction",
        "bus stop", "market", "opposite", "beside", "near", "way"
    ]
    if not text: return []
    low = text.lower()
    return [token for token in ADDRESS_TOKENS if token in low]


"""returns []"""
def find_city_hits(text, target_cities:list[str]) -> list[str]:
    if not text: return []
    low = text.lower()
    return [c for c in target_cities if c.lower() in low]
