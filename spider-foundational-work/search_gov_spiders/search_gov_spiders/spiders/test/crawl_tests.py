# import pprint
# import os
# import sys
# from unittest.case import TestCase
#
# sys.path.append(os.path.abspath('../'))
#
# from domain_spider import DomainSpider
# from mocks.mock_response import return_response
#
#
# # from domain_spider import DomainSpider
# # from mocks.mock_response import return_response
#
#
# class SpiderCrawlTest(TestCase):
#     def setUp(self):
#         self.spider = DomainSpider()
#         pass
#
#     # tests spider crawl capabilities and parsing func in spider class
#     def test_parse(self):
#         print('PATHS:', sys.path)
#         response = return_response('mock_page.html', None)
#         item = self.spider.parse_item(response)
#         # convert to list from generative object for assertion
#         list_item = list(item)
#         self.assertEqual(list_item[0], {'Status': 200, 'Link': 'http://www.example.com'})
