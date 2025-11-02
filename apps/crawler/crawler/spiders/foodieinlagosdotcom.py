import scrapy


class FoodieInLagosDotComSpider(scrapy.Spider):
    name = "foodieinlagosdotcom"
    allowed_domains = ["foodieinlagos.com"]
    start_urls = ["https://foodieinlagos.com"]

    def parse(self, response):
        pass
