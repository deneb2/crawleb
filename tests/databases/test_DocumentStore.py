import sys
import json
import mock
import redis
import unittest

import databases.document_store as ds
from spiders.document import Document
from tests.test_base import BaseTestClass


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
    def test_redisstore(self, mock_redis_set):
        data = {}
        def setter(table, key, val):
            data[key] = val

        mock_redis_set.side_effect = setter
        
        input = self.input_data()
        output = ds.RedisStore("", None, 0, 0)
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
            d = json.loads(data[i])
            url = d["url"]
            count_reads += 1
            self.assertTrue(url in input_dict)
            self.assertEqual(input_dict[url], d)
            
        self.assertEqual(count, count_reads)

    @mock.patch('databases.mongodb_datastore.MongoDB.add')
    def test_mongodbstore(self, mock_mongo_set):
        data = {}
        def setter(key, val):
            data[key] = val

        mock_mongo_set.side_effect = setter
        
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
            self.assertEqual(input_dict[url], d)
            
        self.assertEqual(count, count_reads)

if __name__ == '__main__':
    unittest.main()
