import sys
import json
import mock
import redis
import unittest

import databases.document_store as ds
from spiders.document import Document
from tests.test_base import BaseTestClass, ordered


class TestDocumentStore(BaseTestClass):
    def test_standardstore(self):
        input = self.input_data()
        output = ds.StandardStore()
        
        for i in input:
            i = Document(json.loads(i))
            with self.captured_output() as (out, err):
                output.store(i)
            captured = eval(out.getvalue().strip())
            self.assertEqual(i.info, captured)

    def test_jsonstore(self):
        input = self.input_data()
        output = ds.JsonStore(self.tmp_file)
        
        for i in input:
            i = Document(json.loads(i))
            output.store(i)
        output.close()
        self.assertEqualTemporary()

    @mock.patch('redis.StrictRedis.hset')
    @mock.patch('redis.StrictRedis.hget')
    def test_redisstore(self, mock_redis_get, mock_redis_set):
        data = {}
        def setter(table, key, val):
            data[key] = val

        def getter(table, key):
            return data.get(key)

        mock_redis_set.side_effect = setter
        mock_redis_get.side_effect = getter
        
        input = self.input_data()
        output = ds.RedisStore("", None, 0, 0)
        input_dict = {}
        count = 0
        for i in input:
            i = Document(json.loads(i))
            output.store(i)
            d = i.info
            input_dict[d["url"]] = json.dumps(d)
            count += 1

        self.assertEqual(len(input), len(input_dict))
        self.assertEqual(len(input), count)

        count_reads = 0
        for i in data:
            d = json.loads(data[i])
            url = d["url"]
            count_reads += 1
            self.assertTrue(url in input_dict)

            tmpd = json.loads(input_dict[url])
            del tmpd["fetched_time"]
            del tmpd["status"]
            self.assertEqual(ordered(tmpd), ordered(d))

        self.assertEqual(count, count_reads)

    @mock.patch('databases.mongodb_datastore.MongoDB.add')
    @mock.patch('databases.mongodb_datastore.MongoDB.get')
    def test_mongodbstore(self, mock_mongo_get, mock_mongo_set):
        data = {}
        def setter(key, val):
            data[key] = val

        def getter(key):
            return data.get(key)

        mock_mongo_set.side_effect = setter
        mock_mongo_get.side_effect = getter
        
        input = self.input_data()
        output = ds.MongoDBStore("", None, 0, "void")
        input_dict = {}
        count = 0
        for i in input:
            i = Document(json.loads(i))
            output.store(i)
            d = i.info
            input_dict[d["url"]] = d
            count += 1

        self.assertEqual(len(input), len(input_dict))
        self.assertEqual(len(input), count)

        count_reads = 0
        for i in data:
            d = data[i]
            url = d["url"]
            count_reads += 1
            self.assertTrue(url in input_dict)
            tmpd = input_dict[url]
            del tmpd["fetched_time"]
            del tmpd["status"]
            self.assertEqual(tmpd, d)
            
        self.assertEqual(count, count_reads)

    @mock.patch('databases.mongodb_datastore.MongoDB.add')
    @mock.patch('databases.mongodb_datastore.MongoDB.get')
    def test_mongodbstore(self, mock_mongo_get, mock_mongo_set):
        data = {}
        def setter(key, val):
            data[key] = val

        def getter(key):
            return data.get(key)

        mock_mongo_set.side_effect = setter
        mock_mongo_get.side_effect = getter

        url = "www.daniele.it"
        d1 = Document({"url": url,
                       "status": 200,
                       "fetched_time": "2018-01-07T02:01",
                       "hash": 111111
        })

        d2 = Document({"url": url,
                       "status": 300,
                       "fetched_time": "2018-01-08T02:01",
                       "hash": 121212
        })

        d3 = Document({"url": url,
                       "status": 200,
                       "fetched_time": "2018-01-08T02:01",
                       "hash": 131313
        })

        output = ds.MongoDBStore("", None, 0, "void")

        output.store(d1)
        output.store(d2)
        
        self.assertEqual(1, len(data))
        self.assertEqual(data[url]["hash"], 111111)
        self.assertEqual(len(data[url]["history"]), 2)

        output.store(d3)

        self.assertEqual(1, len(data))
        self.assertEqual(data[url]["hash"], 131313)
        self.assertEqual(len(data[url]["history"]), 3)

if __name__ == '__main__':
    unittest.main()
