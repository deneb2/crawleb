import sys
import json
import mock
import unittest
import requests

import fetcher.fetcher as fetcher
from core.metadata import DocumentMetadata
from tests.test_base import BaseTestClass


class DataResponse():
    def __init__(self, content, status):
        self.content = content
        self.status = status

class MockResponse():
    def __init__(self, c, s):
        self.dr = DataResponse(c, s)
    def get(self, url):
        if self.dr.status == 301:
            raise requests.exceptions.TooManyRedirects
        elif self.dr.status == 500:
            raise requests.exceptions.HTTPError
        elif self.dr.status == 501:
            raise requests.exceptions.Timeout
        elif self.dr.status == 502:
            raise requests.exceptions.InvalidSchema
        elif self.dr.status == 0:
            raise requests.ConnectionError
        elif self.dr.status != 200:
            raise Exception("testing generic exception")
        return self.dr

class TestFetcher(BaseTestClass):
    @mock.patch("utils.requests_wrapper.requests_retry_session")
    def test_fetcher(self, session):
        for i in self.input_data():
            url, content, status = i.split("\t")
            status = int(status)
            dm = DocumentMetadata(url)
            mr = MockResponse(content, status)
            session.return_value = mr
            new_dm = fetcher.fetch({}, dm, 1)
            self.assertEqual(dm.url, url)
            if status == 200:
                self.assertEqual(dm.status, 0)
                self.assertEqual(new_dm.status, 0)
                self.assertEqual(new_dm.response.content, content)                
            elif status == 301:
                self.assertEqual(dm.status, fetcher.Status.SkipUrl)
            elif status == 0:
                self.assertEqual(dm.status, fetcher.Status.ConnectionError)
            elif status >= 500 and status < 510:
                self.assertEqual(dm.status, fetcher.Status.SkipUrl)
            else:
                self.assertEqual(dm.status, fetcher.Status.GenericError)

if __name__ == '__main__':
    unittest.main()

