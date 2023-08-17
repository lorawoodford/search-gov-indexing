import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class armymwr(CrawlSpider):
    name = "armymwr"
    allowed_domains = ["armymwr.com"]
    start_urls = ["https://www.armymwr.com/"]

    rules = (Rule(LinkExtractor(allow = (), deny = ["calendar", "location-contact"],unique = True), callback="parse_item", follow=True),)

    def parse_item(self, response):
        yield {
            "Link": response.url
        }
