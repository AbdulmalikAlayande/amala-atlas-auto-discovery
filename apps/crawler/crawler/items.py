# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class PageItem(scrapy.Item):
    url = scrapy.Field()
    final_url = scrapy.Field()
    status_code = scrapy.Field()
    html = scrapy.Field()
    fetched_at = scrapy.Field()
    source_id = scrapy.Field()
    discovery_type = scrapy.Field()
    score = scrapy.Field()
    signals = scrapy.Field(default=dict)
    candidate = scrapy.Field(default=dict)

    gate_signals = scrapy.Field(default=dict)
    extracted = scrapy.Field(default=dict)
    enriched_fields = scrapy.Field(default=dict)

