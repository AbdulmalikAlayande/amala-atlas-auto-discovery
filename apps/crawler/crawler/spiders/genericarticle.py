from datetime import datetime, timezone

import scrapy

from apps.crawler.crawler.items import PageItem


class GenericArticleSpider(scrapy.Spider):

    name = "generic_article"

    def __init__(self, url=None, source_id="SRC-MANUAL", discovery_type="manual", **kwargs):
        super().__init__(**kwargs)
        if not url:
            raise AttributeError("Pass -a url=<article-url>")
        self.start_urls = [url]
        self.source_id = source_id
        self.discovery_type = discovery_type

    def parse(self, response):

        item = PageItem()
        item['url'] = response.request.url
        item['final_url'] = str(response.url)
        item['status_code'] = response.status
        item['html'] = response.text
        item['fetched_at'] = datetime.now(timezone.utc).isoformat()
        item['source_id'] = self.source_id
        item['discovery_type'] = self.discovery_type

        yield item