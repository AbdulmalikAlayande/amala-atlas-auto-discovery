from datetime import datetime, timezone
import scrapy
from scrapy.http import Response
from typing import Any
import spacy

from apps.crawler.crawler.items import PageItem


# from ..items import PageItem


class GenericArticleSpider(scrapy.Spider):

    name = "generic_article"
    allowed_domains = ["blog.fusion.ng", "foodieinlagos.com"]
    start_urls = [
        "https://foodieinlagos.com/review-nok-by-alara/",
        "https://foodieinlagos.com/review-amala-labule-lagos/",
        "https://foodieinlagos.com/mummy-moji-amala-surulere/",
        "https://foodieinlagos.com/review-ajoke-alamala-plus-lagos/",
        "https://foodieinlagos.com/reviewamalashonola/",
        "https://foodieinlagos.com/review-amala-tax-office-surulere/",
        "https://foodieinlagos.com/aduke-olowosibi-amala-spot/",
        "https://foodieinlagos.com/abeke-rooftop-restaurant-review-lekki-amala-abula-lagos/",
        "https://foodieinlagos.com/ibile-foods-lagos-amala-review/",
        "https://foodieinlagos.com/yakoyo-2/",
        "https://foodieinlagos.com/review-emi-oga/",
        "https://foodieinlagos.com/review-olasheu-canteen/fy",
        "https://foodieinlagos.com/review-olaoluwa-kitchen/",
        "https://foodieinlagos.com/review-ashabi-licking-fingers-food/",


    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.nlp = spacy.load("en_core_web_sm")

    def parse(self, response: Response, **kwargs: Any):
        item = PageItem()

        item['url'] = response.url
        item['final_url'] = response.url
        item['status_code'] = response.status
        item['html'] = response.text
        item['fetched_at'] = datetime.now(timezone.utc)
        item['discovery_type'] = getattr(self, 'discovery_type', 'manual')
        item['source_id'] = getattr(self, 'source_id', None)

        yield item

    def fetch_sources(self, response):
        pass
