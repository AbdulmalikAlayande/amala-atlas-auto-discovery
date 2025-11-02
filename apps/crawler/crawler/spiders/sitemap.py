import scrapy


class SitemapSpider(scrapy.Spider):
    name = "sitemap"
    allowed_domains = ["blog.fusion.ng", "foodieinlagos.com"]
    start_urls = [
        "http://blog.fusion.ng/post-sitemap.xml"
        "http://blog.fusion.ng/page-sitemap.xml"
        "http://blog.fusion.ng/e-landing-page-sitemap.xml"
        "http://blog.fusion.ng/category-sitemap.xml"
        "http://blog.fusion.ng/post_tag-sitemap.xml"
        "http://blog.fusion.ng/post_tag-sitemap2.xml"
        "http://blog.fusion.ng/author-sitemap.xml"
    ]

    def parse(self, response):
        pass
