import pprint
import sys
import urllib.parse

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

# sys.path.append('../../../')

starting_urls = '../../../utility-scripts/startingUrls.txt'
start_urls_list = []
with open(starting_urls) as file:
    while line := file.readline():
        # start_urls_list.append(urllib.parse.quote_plus(line.rstrip()))
        start_urls_list.append(line.rstrip())

pprint.pprint(start_urls_list)


# scrapy command for crawling domain/site
# scrapy crawl domain_spider -a desired_domain -a desired_url
# ex: scrapy crawl domain_spider -a travel.dod.mil -a https://travel.dod.mil

class DomainSpider(CrawlSpider):
    # name = "travelDodMil"
    name = "domain_spider"

    def __init__(self, domain=None, urls=None, *args, **kwargs):
        super(DomainSpider, self).__init__(*args, **kwargs)
        # allowed_domains = ["travel.dod.mil"]
        # self.allowed_domains = [domain]
        # start_urls = ["https://www.travel.dod.mil/"]
        # self.start_urls = [urls]
        self.start_urls = start_urls_list

    # file type exclusions
    rules = (Rule(LinkExtractor(allow=(),
                                deny=[
                                    "calendar",
                                    "location-contact",
                                    "DTMO-Site-Map/FileId/"
                                    # "\*redirect"
                                ],
                                deny_extensions=[
                                    'js',
                                    'xml',
                                    'gif',
                                    'wmv',
                                    'wav',
                                    'ibooks',
                                    'zip',
                                    'css',
                                    'mp3',
                                    'mp4',
                                    'cfm',
                                    'jpg',
                                    'jpeg',
                                    'png',
                                    'svg'
                                ], unique=True), callback="parse_item", follow=True),)

    @staticmethod
    def parse_item(response):
        """This function gathers the url and the status.

        @url http://quotes.toscrape.com/
        @returns items 1 2
        @returns requests 0 0
        @scrapes Status Link
        """
        yield {
            "Status": response.status,
            "Link": response.url
        }
