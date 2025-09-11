
"""
**Rule weights**
JSON-LD addr +0.25,
maps +0.20,
phone +0.15,
city proximity +0.10,
recency +0.05,
hint name match +0.10,
listicle penalty -0.10;
max 1.0
"""
def score(signals:dict, hint_boost_names:list[str]) -> tuple[float, dict]:
    s = 0.0
    why = []

    def add(val, reason):
        nonlocal s, why
        s += val
        why.append(reason)

    if signals.get("has_jsonld_restaurant"): add(0.25, "jsonld_address")
    if signals.get("has_maps_link"):         add(0.20, "maps_link")
    if signals.get("has_phone"):             add(0.15, "phone_found")
    if signals.get("city_hit_near_food_terms"): add(0.10, "city_hit")
    if signals.get("recent_content"):        add(0.05, "recent")
    if signals.get("listicle_penalty"):      add(-0.10, "listicle_penalty")

    # clamp 0..1
    s = max(0.0, min(1.0, s))
    return s, {"reasons": why}
