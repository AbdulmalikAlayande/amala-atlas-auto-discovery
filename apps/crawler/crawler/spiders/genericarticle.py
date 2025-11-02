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

        img = response.xpath("//div[contains(@class, 'section-post-header')]//div[@class='image_wrapper']//img").attrib
        phone = None
        phone_data = response.xpath("//div[@class='section_wrapper']//div[@class='the_content_wrapper']//h3/span[contains(@style, 'font-size: 24pt;')]/span/strong//a[contains(@href, 'tel')]/@href").get()
        if phone_data:
            phone = phone_data.replace("tel:", "")

        item['candidate'] = {
            "price": response.xpath("string(//div[@class='entry-content-wrap clearfix']//table//tr[position()=4]//td[position()=3]//*[contains(translate(., 'TOTAL', 'total'), 'total')])").get(),
            "address": response.xpath("//div[@class='section_wrapper']//div[@class='the_content_wrapper']//h3/span[contains(@style, 'font-size: 24pt;')]/span/strong//a[contains(@href, 'maps.app.goo.gl')]/text()").get(),
            "title": response.xpath("//div[@class='section_wrapper']//div[@class='the_content_wrapper']//h3/span[contains(@style, 'font-size: 24pt;')]/text()").get(),
            "location_url": response.xpath("//div[@class='section_wrapper']//div[@class='the_content_wrapper']//h3/span[contains(@style, 'font-size: 24pt;')]/span/strong//a[contains(@href, 'maps.app.goo.gl')]/@href").get(),
            "phone": phone,
            "image_url": img.get('src') if 'https' in img.get('src', '') else img.get('data-src'),
        }
        item['url'] = response.url
        item['status_code'] = response.status
        item['fetched_at'] = datetime.now(timezone.utc)

        yield item


    def fetch_sources(self, response):
        pass