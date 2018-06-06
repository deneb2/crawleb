import sys
import json
import mock
import redis
import unittest

from mockredis import mock_strict_redis_client

import databases.redis_queues as rq
from tests.test_base import BaseTestClass


class TestRedisPriority(BaseTestClass):
    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    def setUp(self):
        super(TestRedisPriority, self).setUp()

        # same data with same priority are inserted only one time
        # adding multiple eqaul entry will make the test fail
        self.input_data = [(100, {"data":1}),
                           (3, "data"),
                           (192, [0, 1, 3]),
                           (2000, "test_not_pickup")
        ]

        self.queue_name = "test"
        self.redis_client = rq.RedisPriorityQueue(self.queue_name, None, 0, 5)
        self.max_prio = 0
        # push the data into redis
        for i in self.input_data:
            self.redis_client.push(i, i[0])
            self.max_prio = i[0] > self.max_prio and i[0] or self.max_prio

        self.sorted_input = sorted(self.input_data, key=lambda x: x[0])
            
    def test_redis_pop(self):
        # try popping with early timestamp
        self.assertIsNone(self.redis_client.pop(2))
        for i in self.sorted_input:
            # in the process of storing in redis. Types can change
            tmp_obj = json.loads(json.dumps(i))            
            self.assertEqual(self.redis_client.pop(self.max_prio + 1), tmp_obj)
        self.assertIsNone(self.redis_client.pop(self.max_prio + 1))

    def test_redis_getall(self):
        results = self.redis_client.getall()
        self.assertEqual(len(results), len(self.input_data))
        for i, v in enumerate(results):
            # in the process of storing in redis. Types can change
            tmp_obj = json.loads(json.dumps(self.sorted_input[i]))
            self.assertEqual(v, tmp_obj)

    def test_delete(self):
        for i in self.sorted_input:
            self.redis_client.delete(i)
        self.assertIsNone(self.redis_client.pop(self.max_prio + 1))

    def test_clear(self):
        self.redis_client.clear()
        self.assertIsNone(self.redis_client.pop(self.max_prio + 1))


class TestRedisNormal(BaseTestClass):
    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    def setUp(self):
        super(TestRedisNormal, self).setUp()
        
        self.input_data = [(100, {"data":1}),
                           (3, "data"),
                           (3, "data"),
                           (192, [0, 1, 3]),
                           (2000, "test_not_pickup")
        ]
        self.queue_name = "test"
        self.redis_client = rq.RedisNormalQueue(self.queue_name, None, 0, 5)

        # push the data into redis
        for i in self.input_data:
            self.redis_client.push(i)
            
    def test_redis_pop(self):
        for i in self.input_data:
            # in the process of storing in redis. Types can change
            tmp_obj = json.loads(json.dumps(i))            
            self.assertEqual(self.redis_client.pop(), tmp_obj)
        self.assertIsNone(self.redis_client.pop())


class TestRedisHash(BaseTestClass):
    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    def setUp(self):
        super(TestRedisHash, self).setUp()
        
        self.input_data = [("k", "data"),
                           ("k1", "data"),
                           ("k1", "data"),
                           ("k2", "data3"),
                           ("k3", "data4")
        ]
        self.queue_name = "test"
        self.redis_client = rq.RedisHash(self.queue_name, None, 0, 5)
        self.data_dict = {}
        
        # push the data into redis and in a dictionary
        for i in self.input_data:
            key, value = i
            self.redis_client.add(key, value)
            self.data_dict[key] = value

    def test_redis_get(self):
        for i in self.input_data:
            key, value = i
            self.assertTrue(key in self.redis_client)
            self.assertFalse(key + "test" in self.redis_client)
            redis_value = self.redis_client.get(key)
            self.assertEqual(redis_value, value)
            
        self.assertEqual(len(self.redis_client), len(self.data_dict))

    def test_redis_getall(self):
        count = 0
        for i in self.redis_client.getall():
            self.assertEqual(self.data_dict[i], self.redis_client.get(i))
            count += 1
        self.assertEqual(count, len(self.data_dict))

    def test_redis_delete(self):
        count = 1
        self.assertEqual(len(self.redis_client), len(self.data_dict))
        for i in self.data_dict:
            self.redis_client.delete(i)
            self.assertEqual(len(self.redis_client), len(self.data_dict)-count)
            count += 1
        self.assertEqual(len(self.redis_client), 0)


class TestRedisPageHash(BaseTestClass):
    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    def setUp(self):
        super(TestRedisPageHash, self).setUp()
        # keys + "test" are used to test contains == False
        # do not use key ending with 'test'
        self.input_data = [("k", 1234, 0, "alt1"),
                           ("k1", 1235, 1, "alt3"),
                           ("k1", 1234, 3, "alt1"),
                           ("k2", 1334, 4, "alt3"),
                           ("k3", 1534, 5, "alt4"),
        ]
        self.queue_name = "test"
        self.redis_client = rq.RedisPageHash(self.queue_name, None, 0, 5)
        self.data_dict = {}
        
        # push the data into redis and in a dictionary
        for i in self.input_data:
            key, phash, count, alternatives = i
            self.redis_client.add(key, phash, count, alternatives)
            self.data_dict[key] = {
                "page_hash": phash,
                "count": count,
                "alternatives": alternatives,
            }

    def test_redis_get(self):
        for i in self.input_data:
            key, _, _, _ = i
            self.assertTrue(key in self.redis_client)
            self.assertFalse(key + "test" in self.redis_client)
            redis_value = self.redis_client.get(key)
            self.assertEqual(redis_value, self.data_dict[key])
            
        self.assertEqual(len(self.redis_client), len(self.data_dict))

    def test_redis_increase(self):
        for i, key in enumerate(self.data_dict):
            self.redis_client.incr_n(key, i)
            redis_value = self.redis_client.get(key)
            self.assertEqual(redis_value["count"], self.data_dict[key]["count"] + i)
            
        self.assertEqual(len(self.redis_client), len(self.data_dict))


if __name__ == '__main__':
    unittest.main()
