import sys
import json
import mock
import redis
import unittest

import robotparser
from StringIO import StringIO
from bs4 import BeautifulSoup
from freezegun import freeze_time

from spiders.document import Document
import spiders.base_spider as cs
from tests.test_base import BaseTestClass


class FileObject():
    data = ["User-agent: *","Allow: /"]
    def __iter__(self):
        for i in self.data:
            yield i
    def close(self):
        pass


class TestCrawlSpider(BaseTestClass):
    def test_basenormalize(self):
        output = self.output_data()
        spider = cs.BaseSpider()
        for c, i in enumerate(self.input_data()):
            i = json.loads(i)
            url = i["url"]
            spider.normalize_params = i["param"]
            nurl = spider.normalize_url(url)
            self.assertEqual(nurl, output[c])

    def test_basegetcanonical(self):
        input = self.input_data()
        output = self.output_data()
        spider = cs.BaseSpider()
        for c, i in enumerate(input):
            i = json.loads(i)
            text = BeautifulSoup(i["raw_html"], "lxml")
            canonical = spider.get_canonical(text)
            self.assertEqual(canonical, output[c])

    def test_basegetdomain(self):
        input = self.input_data()
        output = self.output_data()
        spider = cs.BaseSpider()
        for c, i in enumerate(input):
            i = json.loads(i)
            url = i["url"]
            domain = spider.get_domain(url)
            self.assertEqual(domain, output[c])

    @mock.patch('robotparser.URLopener.open')
    def test_basegetlinks(self, opener):        
        opener.return_value = FileObject()
        input = self.input_data()
        spider = cs.BaseSpider()
        spider.allowed_domains = ["www.ilsole24ore.com"]
        for c, i in enumerate(input):
            i = json.loads(i)
            text = BeautifulSoup(i["raw_html"], "lxml")
            url = i["url"]
            links = spider.get_links(text, url)
            self.store_temporary(links)
        self.assertEqualTemporary()

    def test_setconfig(self):
        spider = cs.BaseSpider()
        spider.urllist_filename = self.input_data_file
        spider.allowed_domains = ["www.url.com"]
        spider.set_config()
        for i in self.input_data():
            json_obj = json.loads(i)
            if "notallowed" in json_obj["url"]:
                self.assertFalse(json_obj in spider.urllist)
            else:
                self.assertTrue(json_obj in spider.urllist)

    def test_check_and_normalize(self):
        spider = cs.BaseSpider()
        spider.allowed_domains = ["www.url.com"]
        spider.exclude_pages = [".*/1.*"]
        spider.normalize_params = ["a", "e"]
        
        output = self.output_data()
        for n, i in enumerate(self.input_data()):
            nurl, toremove = spider.check_and_normalize(i.strip())
            onurl, otoremove = output[n].strip().split("\t")
            self.assertEqual((onurl, otoremove), (nurl, str(toremove)))

    def test_getmeta(self):
        spider = cs.BaseSpider()
        for line in self.input_data():
            json_obj = json.loads(line)
            root = spider.get_tree_dom(json_obj["content"])
            data = spider.get_meta(root, json_obj["name"])
            self.store_temporary(data)
        self.assertEqualTemporary()

    @mock.patch('robotparser.URLopener.open')
    @freeze_time('2018-05-15')
    def test_parse(self, opener):
        opener.return_value = FileObject()
        import pickle
        spider = cs.BaseSpider()
        spider.allowed_domains = ["http://www.mediagol.it"]
        f = open(self.input_data_file, "r")
        while True:
            try:
                dm = pickle.load(f)
            except EOFError:
                break
            doc, meta = spider.parse(dm)
            self.store_temporary(json.dumps(doc.info))
        self.assertEqualTemporary()
        
if __name__ == '__main__':
    unittest.main()

    
