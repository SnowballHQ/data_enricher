import scrapy

class WebsiteContentItem(scrapy.Item):
    url = scrapy.Field()
    domain = scrapy.Field()
    page_type = scrapy.Field()  # homepage, about, products, etc.
    title = scrapy.Field()
    content = scrapy.Field()
    meta_description = scrapy.Field()
    scraped_at = scrapy.Field()
    status = scrapy.Field()
    error_message = scrapy.Field()