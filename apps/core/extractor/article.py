"""returns {title: val, text: val, publish_date: val}"""
import datetime
import logging
import re
from datetime import timedelta

import dateparser
from lxml import html as lx
from readability import Document


log = logging.getLogger(__name__)

def extract_readable(html) -> dict:
    if not html:
        return {"title": None, "text": "", "publish_date": None, "is_recent": False}

    doc = Document(html)
    title = doc.short_title
    content_html = doc.summary(html_partial=True)
    tree = lx.fromstring(content_html)
    text = " ".join(tree.itertext()).strip()

    publish_date_match = re.search(r"(\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b|\b\d{1,2}\s+[A-Za-z]{3,}\s+20\d{2}\b)", html)
    publish_date = dateparser.parse(publish_date_match.group(0)).date().isoformat() if publish_date_match else None
    is_recent = False
    if publish_date:
        try:
            date = datetime.date.fromisoformat(publish_date)
            is_recent = datetime.date.today() - date <= timedelta(days=365)
        except Exception as e:
            log.error(e)
    return {"title": title, "text": text, "publish_date": publish_date, "is_recent": is_recent}


"""returns LocalBusiness if present"""
def extract_jsonld(html) -> dict|None:
    # very light JSON-LD grab (you can swap in extruct later)
    try:
        import extruct
        data = extruct.extract(html, syntaxes=["json-ld"], errors="ignore")
        for obj in data.get("json-ld", []):
            t = obj.get("@type")
            if t in ("Restaurant", "LocalBusiness") or (
                    isinstance(t, list) and any(x in ("Restaurant", "LocalBusiness") for x in t)):
                addr = obj.get("address")
                # address can be dict or string
                if isinstance(addr, dict):
                    addr_text = " ".join(
                        str(addr.get(k, "")) for k in ["streetAddress", "addressLocality", "addressRegion"])
                else:
                    addr_text = addr
                return {
                    "name": obj.get("name"),
                    "address": addr_text,
                    "openingHours": obj.get("openingHours"),
                    "telephone": obj.get("telephone"),
                }
    except Exception as e:
        log.error(e)
    return None


"""returns {maps_links: [...], social_links: {...}}"""
def extract_outlinks(html, base_url) -> dict:
    maps_links = []
    social_links = {}
    try:
        tree = lx.fromstring(html)
        tree.make_links_absolute(base_url)
        for element in tree.xpath("//a[@href]"):
            href = element.get("href")
            if not href:
                continue
            if "maps.google." in href or "google.com/maps" in href:
                maps_links.append(href)
            if "instagram.com" in href and "instagram" not in social_links:
                social_links["instagram"] = href
            if "facebook.com" in href and "facebook" not in social_links:
                social_links["facebook"] = href
    except Exception as exception:
        log.error(exception)

    return {"maps_links": list(set(maps_links)), "social_links": social_links}
