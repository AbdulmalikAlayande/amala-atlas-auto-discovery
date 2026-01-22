import scrapy
from datetime import datetime
from apps.crawler.crawler.items import PageItem
from typing import Any

class GenericSpider(scrapy.Spider):
    name = "generic"

    def __init__(self, start_urls=None, discovery_type="manual", source_id=None, *args, **kwargs):
        super(GenericSpider, self).__init__(*args, **kwargs)
        if start_urls:
            self.start_urls = start_urls.split(',')
        else:
            self.start_urls = []
        self.discovery_type = discovery_type
        self.source_id = source_id

    def parse(self, response, **kwargs: Any):
        # 1. Yield the current page for extraction/scoring
        yield PageItem(
            url=response.url,
            final_url=response.url,
            status_code=response.status,
            html=response.text,
            fetched_at=datetime.utcnow().isoformat(),
            source_id=self.source_id,
            discovery_type=self.discovery_type
        )

        # 2. Simple Link Following (only on list/seed pages)
        # If the page looks like a blog or directory, follow links that might be restaurant pages
        # For now, we follow all internal links or links to known target domains
        # This can be made more sophisticated later
        for next_page in response.css('a::attr(href)').getall():
            if next_page:
                yield response.follow(next_page, self.parse)
