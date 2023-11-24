import scrapy
from ..items import CommonCrawlItem
from scrapy.loader import ItemLoader
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


from bs4 import BeautifulSoup

import re
import hashlib
import sqlite3


class ConnectDatabase:
    def __init__(self):
        self.ids_seen = set()
        self.con = sqlite3.connect(
            '/Users/duongtrongchi/project/web-crawling/common_crawl/common_crawl.db')
        self.cur = self.con.cursor()

    def get_ids(self):
        ids = self.cur.execute("""SELECT id FROM common_crawl""").fetchall()
        self.ids_seen = set(i[0] for i in ids)
        return self.ids_seen


class TdtuSpider(CrawlSpider):
    name = "edu"
    rules = [
        Rule(
            LinkExtractor(allow=[r"/.*"]),
            callback="parse",
            follow=True,
        )
    ]

    # custom_settings = {
    #     'DEPTH_LIMIT': 10
    # }

    def __init__(self, *args, **kwargs):
        super(TdtuSpider, self).__init__(*args, **kwargs)
        self.start_urls = [self.surl]
        self.allowed_domains = [self.domain]
        self.ids_seen = ConnectDatabase().get_ids()

    @property
    def header(self):
        return {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'
        }

    def parse(self, response):
        id = hashlib.sha256(response.url.encode('utf-8')).hexdigest()

        if id in self.ids_seen:
            return
        

        soup = BeautifulSoup(response.body, 'html.parser')
        soup.attrs.clear()

        item = ItemLoader(item=CommonCrawlItem())
        item.add_value('id', response.url)
        item.add_value('url', response.url)
        item.add_value('title', soup.title)
        item.add_value('content', str(soup.body))

        yield item.load_item()
