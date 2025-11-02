import scrapy
from trafilatura import feeds

class FeedSpider(scrapy.Spider):
    name = "feed"
    allowed_domains = ["example.com"]
    start_urls = ["https://example.com"]

    def parse(self, response):
        pass
