import sys
import json
import mock
import redis
import unittest

import mongomock

import databases.mongodb_datastore as mongo
from spiders.document import Document
from tests.test_base import BaseTestClass

class TestMongodb(BaseTestClass):
    
    @mock.patch('pymongo.MongoClient')
    def test_mongo_add_and_get(self, mc):
        mc.return_value = mongomock.MongoClient()
        database = mongo.MongoDB("tes", "test", 111, "test")
        input = self.input_data()
        input_dict = {}
        
        # testing insert
        for i in input:
            i = json.loads(i)
            database.add(i["_id"], i)
            input_dict[i["_id"]] = i

        # testing getall
        result = database.getall()
        count = 0
        for i in result:
            id = i["_id"]
            self.assertEqual(input_dict[id], i)
            count += 1
        self.assertEqual(count, len(input_dict))
        
        # testing get and delete
        for i in input_dict:
            result = database.get(i)
            self.assertEqual(result, input_dict[i])
            database.delete(i)
            result = database.get(i)
            self.assertIsNone(result)

    @mock.patch('pymongo.MongoClient')
    def test_mongohash(self, mc):
        mc.return_value = mongomock.MongoClient()
        database = mongo.MongoDBPageHash("tes", "test", 111, "test")
        input = self.input_data()

        data_dict = {}
        for i in input:
            url, p_hash, count, alternatives = i.split("\t")
            database.add(url, p_hash, int(count), alternatives)
            data_dict[url] = (p_hash, int(count), alternatives)

        self.assertEqual(len(database), len(data_dict))
            
            
        for i in data_dict:
            self.assertTrue(i in database)
            self.assertFalse(i + "randomchar" in database)
            database.incr_n(i)
            result = database.get(i)
            self.assertEqual(result["page_hash"], data_dict[i][0])
            self.assertEqual(result["count"], data_dict[i][1] + 1)
            self.assertEqual(result["alternatives"], data_dict[i][2])


if __name__ == '__main__':
    unittest.main()
