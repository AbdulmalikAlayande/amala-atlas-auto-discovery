import scrapy


class SitemapSpider(scrapy.Spider):
    name = "sitemap"
    allowed_domains = ["example.com"]
    start_urls = ["https://example.com"]

    def parse(self, response):
        pass
