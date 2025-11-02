import scrapy


class OfadaaDotComSpider(scrapy.Spider):
    name = "ofadaadotcom"
    allowed_domains = ["ofadaa.com"]
    start_urls = ["https://ofadaa.com"]

    def parse(self, response):
        pass
