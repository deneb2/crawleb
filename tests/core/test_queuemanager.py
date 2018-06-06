import mock
import time
import unittest
import mongomock

from datetime import datetime
from mockredis import mock_strict_redis_client
from freezegun import freeze_time

from core.queue_manager import QueueManager
from core.metadata import DocumentMetadata, Source
from tests.test_base import BaseTestClass

# a fixed time usefull for some tests
CURRENT_TIME = "2018-05-31"

# a fixed time in the future for some tests
mock_time = mock.Mock()
mock_time.return_value = time.mktime(datetime(2018, 6, 1).timetuple())

# test configuration. Parameters are not really important since
# I am going to mock redis and mongo
REDISMONGOCONF = {
    'host': 'localhost',
    'port': 42,
    'db': 'test',
}
CONFIGURATION = {
    'queues':{'refetching-delay':3},
    "realtime": False,
    "redis":REDISMONGOCONF,
    "mongodb":REDISMONGOCONF
}
START_DELAY = 2


class TestQueueManager(BaseTestClass):
    def setUp(self):
        super(TestQueueManager, self).setUp()

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def test_add_normal_urls(self, mc):
        '''testing insertion of new links'''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        dm = DocumentMetadata("http://random-url.com")
        dm.depth = 1
        dm.links = [
            "http://www.randomurl1.it",
            "http://www.randomurl2.it",
            "http://www.randomurl3.it",
            "http://www.randomurl4.it",
            "http://www.randomurl5.it",
            "http://www.randomurl6.it",
            "http://www.randomurl7.it",
        ]
        all_url_lenght = len(dm.links)
        # adding all the links found in the document to the normal list
        qm.add_normal_urls(dm)

        # checking if the urls are there:
        stored = qm.normal_store.getall()
        self.assertEqual(len(stored), all_url_lenght)

        links_set = set(dm.links)
        for s in stored:
            self.assertTrue(s[1] in links_set)
            # depth should be increased by 1
            self.assertEqual(s[0],  dm.depth+1)

        # checking that seen is still empty
        for u in dm.links:
            self.assertEqual(qm.seen.is_new(u), True)

        # adding a duplicate with same depth
        # entry should be replaced.
        dm.links = ["http://www.randomurl1.it"]
        qm.add_normal_urls(dm)
        stored = qm.normal_store.getall()
        self.assertEqual(len(stored), all_url_lenght)

        # adding a duplicate with different depth
        # we should have duplicate entries.
        dm.depth = 3
        dm.links = ["http://www.randomurl1.it"]
        qm.add_normal_urls(dm)
        stored = qm.normal_store.getall()
        self.assertEqual(len(stored), all_url_lenght + 1)

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def test_add_normal_urls_some_seen(self, mc):
        '''
        Tesiting insertion of some new and old links.
        '''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        
        ############################
        # insert some urls into seen
        dm_seen_1 = DocumentMetadata("http://www.randomurl1.it")
        # this two are cosidered the same page with different urls
        dm_seen_1.alternatives = [
            "http://www.randomurl1.it",
            "http://www.randomurl3.it",
        ]
        dm_seen_1.dhash = 12345
        dm_seen_2 = DocumentMetadata("http://www.randomurl4.it")
        dm_seen_2.alternatives = [
            "http://www.randomurl4.it",
        ]
        dm_seen_2.dhash = 98765
        already_seen_urls = set(dm_seen_1.alternatives).union(dm_seen_2.alternatives)
        qm.seen.add(dm_seen_1)
        qm.seen.add(dm_seen_2)
        
        ############################################
        # testing add normal url with some seen urls
        dm = DocumentMetadata("http://random-url.com")
        dm.depth = 1
        dm.links = [
            "http://www.randomurl1.it",
            "http://www.randomurl2.it",
            "http://www.randomurl3.it",
            "http://www.randomurl4.it",
            "http://www.randomurl5.it",
            "http://www.randomurl6.it",
            "http://www.randomurl7.it",
        ]
        
        # adding all the links found in the document to the normal list
        qm.add_normal_urls(dm)

        # checking if the urls are there (all except 3 because already seen):
        links_set = set(dm.links).difference(already_seen_urls)
        stored = qm.normal_store.getall()
        self.assertEqual(len(stored), len(links_set))

        # count for this urls should be 1
        for i in dm_seen_1.alternatives:
            data = qm.seen.get(i)
            # this is 3 becouse we inserted one in seen +
            # we tried to insert in normal www.randomurl1.com  +
            # we tried to insert in normal www.randomurl3.com
            self.assertEqual(data["count"], 3)

        # adding a duplicate that is already in seen.
        # should not be added in normal list, but counters of
        # all alternatives shouls be updated (+1)
        dm.links = [dm.links[0],]
        qm.add_normal_urls(dm)
        stored = qm.normal_store.getall()
        for i in dm_seen_1.alternatives:
            data = qm.seen.get(i)
            self.assertEqual(data["count"], 4)

        # adding again with different depth should not
        # change the behaviour
        dm.depth = 3
        dm.links = [dm.links[0],]
        qm.add_normal_urls(dm)

        for i in dm_seen_1.alternatives:
            # should be 2 because I inserted 2 times
            data = qm.seen.get(i)
            self.assertEqual(data["count"], 5)

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def test_remove_seen(self, mc):
        '''
        Test delete some seen
        '''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        
        ############################
        # insert some urls into seen
        dm_seen_1 = DocumentMetadata("http://www.randomurl1.it")
        # this two are cosidered the same page with different urls
        dm_seen_1.alternatives = [
            "http://www.randomurl1.it",
            "http://www.randomurl3.it",
        ]
        dm_seen_1.dhash = 12345
        dm_seen_2 = DocumentMetadata("http://www.randomurl4.it")
        dm_seen_2.alternatives = [
            "http://www.randomurl4.it",
        ]
        dm_seen_2.dhash = 98765
        already_seen_urls = set(dm_seen_1.alternatives).union(dm_seen_2.alternatives)
        qm.seen.add(dm_seen_1)
        qm.seen.add(dm_seen_2)

        self.assertTrue(qm.seen.get("www.randomurl1.it") is not None)
        self.assertTrue(qm.seen.get("www.randomurl3.it") is not None)
        self.assertTrue(qm.seen.get("www.randomurl4.it") is not None)
        # deleting url should remove all alternatives
        qm.remove_seen("www.randomurl1.it")

        self.assertTrue(qm.seen.get("www.randomurl1.it") is None)
        self.assertTrue(qm.seen.get("www.randomurl3.it") is None)
        self.assertTrue(qm.seen.get("www.randomurl4.it") is not None)

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def test_add_bootstrap(self, mc):
        '''
        Test adding some bootstrap urls
        '''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        burls = [
            {"url": "www.daniele.com", "depth": "3"},
            {"url": "www.daniele1.com", "depth": "2"},
            {"url": "www.daniele2.com", "depth": "1"},
            {"url": "www.daniele3.com", "depth": "3"},
            {"url": "www.daniele4.com", "depth": "5"},
            {"url": "www.daniele5.com", "depth": "1"},
        ]
        max_initial_depth = 5
        qm.add_bootstrap_urls(burls)

        # extracting from normal list should return urls in order of depth
        count = 0
        max_depth = 0
        while True:
            doc = qm.pop()
            if not doc.url:
                break
            self.assertTrue(doc.depth >= max_depth)
            max_depth = doc.depth
            count += 1

        self.assertEqual(count, len(burls))
        
        # when adding bootstrap, is added 2 to the depth (it is hardcoded for now)
        self.assertEqual(max_depth, max_initial_depth+2)

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def test_add_priority_urls(self, mc):
        '''
        Test adding some priority url
        '''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        urls = [
            "www.daniele.com",
            "www.daniele1.com",
            "www.daniele2.com",
            "www.daniele3.com",
        ]

        qm.init_priority_list(urls)

        count = 0
        while True:
            doc = qm.pop()
            if not doc.url:
                break
            self.assertEqual(doc.depth, 0)
            self.assertEqual(doc.source, Source.priority)
            count += 1

        self.assertEqual(count, len(urls))


    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    @freeze_time(CURRENT_TIME)
    def test_pop_ordering(self, mc):
        '''
        Test adding url to priority, normal and refetch
        and checking the ordering of popping is correct
        '''
        mc.return_value = mongomock.MongoClient()
        qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)

        # inserting a priority url.
        urls = [
            "www.daniele.com",
        ]
        qm.init_priority_list(urls)

        # inserting a normal url.
        burls = [
            {"url": "www.daniele1.com", "depth": "2"},
        ]
        qm.add_bootstrap_urls(burls)

        # inserting a refetch url
        dm = DocumentMetadata("http://www.randomurl8.it")
        dm.depth = 1
        dm.dhash = 121212
        dm.source = Source.normal
        dm.delay = 500
        dm.alternatives = [
            "http://www.randomurl8.it"
        ]
        qm.add_seen_and_reschedule(dm)

        # make sure all the inserted url are ready to be popped
        with mock.patch("time.time", mock_time):
            doc = qm.pop()
            #first one from priority
            self.assertEqual(doc.depth, 0)
            self.assertEqual(doc.source, Source.priority)
            
            doc = qm.pop()
            # second one from normal
            self.assertEqual(doc.source, Source.normal)
            
            doc = qm.pop()
            #third from refetching
            self.assertEqual(doc.source, Source.refetch)


class TestQueueRescheduling(BaseTestClass):

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    @mock.patch('pymongo.MongoClient')
    def setUp(self, mc):
        super(TestQueueRescheduling, self).setUp()
        mc.return_value = mongomock.MongoClient()

        self.qm = QueueManager("queues-names", START_DELAY, CONFIGURATION)
        ############################
        # insert some urls into seen
        dm_seen_1 = DocumentMetadata("http://www.randomurl1.it")
        # this two are cosidered the same page with different urls
        dm_seen_1.alternatives = [
            "http://www.randomurl1.it",
            "http://www.randomurl3.it",
        ]
        dm_seen_1.dhash = 12345
        dm_seen_2 = DocumentMetadata("http://www.randomurl4.it")
        dm_seen_2.alternatives = [
            "http://www.randomurl4.it",
        ]
        dm_seen_2.dhash = 98765
        already_seen_urls = set(dm_seen_1.alternatives).union(dm_seen_2.alternatives)
        self.qm.seen.add(dm_seen_1)
        self.qm.seen.add(dm_seen_2)
        self.qm.seen.incr_n(dm_seen_1.url) # increase counter to check it later
        counter = self.qm.seen.get(dm_seen_1.url).get("count")
        self.assertEqual(counter, 2)

    @freeze_time(CURRENT_TIME)
    def test_reschedule1(self):
        ############################################
        # testing rescheduling some urls
        # this one is in seen with the same hash and
        # not taken from priority queue
        # I expect: doublig the delay and set seen counter to 1
        dm = DocumentMetadata("http://www.randomurl1.it")
        dm.depth = 1
        dm.dhash = 12345
        dm.source = Source.normal
        dm.delay = 5000
        # alternatives contains always at least one url.
        dm.alternatives = [
            "http://www.randomurl1.it"
        ]
        # we want to check that former alternatives are also correctly
        # updated even if new alternatives field is different.
        alternatives = self.qm.seen.get(dm.url).get("alternatives")
        self.assertNotEqual(len(dm.alternatives), len(alternatives))
        
        self.qm.add_seen_and_reschedule(dm)

        # check all the parameters
        counter = self.qm.seen.get(dm.url).get("count")
        dhash = self.qm.seen.get(dm.url).get("page_hash")
        self.assertEqual(counter, 1)

        # check updated all the alternatives
        for urls in alternatives:
             counter = self.qm.seen.get(urls).get("count")
             dhash = self.qm.seen.get(dm.url).get("page_hash")
             self.assertEqual(counter, 1)
             self.assertEqual(dhash, dm.dhash)

        with mock.patch("time.time", mock_time):
            refetching_data = self.qm.pop()
        self.assertEqual(refetching_data.delay, dm.delay*2)
        self.assertEqual(refetching_data.source, Source.refetch)

    @freeze_time(CURRENT_TIME)
    def test_reschedule2(self):
        ############################################
        # this one is in seen with a different hash and
        # not taken from priority queue
        # I expect: halfing the delay and set seen counter to 1
        dm = DocumentMetadata("http://www.randomurl1.it")
        dm.depth = 1
        dm.dhash = 1936
        dm.source = Source.normal
        dm.delay = 5000
        dm.alternatives = [
            "http://www.randomurl1.it"
        ]

        self.qm.add_seen_and_reschedule(dm)

        # checking all the parameters
        counter = self.qm.seen.get(dm.url).get("count")
        dhash = self.qm.seen.get(dm.url).get("page_hash")
        self.assertEqual(counter, 1)
        self.assertEqual(dhash, dm.dhash)
        with mock.patch("time.time", mock_time):
            refetching_data = self.qm.pop()
        self.assertEqual(refetching_data.delay, dm.delay/2)
        self.assertEqual(refetching_data.source, Source.refetch)

    @freeze_time(CURRENT_TIME)
    def test_reschedule3(self):
        ############################################
        # as before but with a small delay.
        # cheking delay not changing
        dm = DocumentMetadata("http://www.randomurl1.it")
        dm.depth = 1
        dm.dhash = 121212
        dm.source = Source.normal
        dm.delay = 500
        dm.alternatives = [
            "http://www.randomurl1.it"
        ]

        self.qm.add_seen_and_reschedule(dm)

        # checking all the parameters
        counter = self.qm.seen.get(dm.url).get("count")
        dhash = self.qm.seen.get(dm.url).get("page_hash")
        self.assertEqual(counter, 1)
        self.assertEqual(dhash, dm.dhash)
        with mock.patch("time.time", mock_time):
            refetching_data = self.qm.pop()
        
        self.assertEqual(refetching_data.delay, dm.delay)
        self.assertEqual(refetching_data.source, Source.refetch)

    @freeze_time(CURRENT_TIME)
    def test_reschedule4(self):
        ############################################
        # inserting a new url
        dm = DocumentMetadata("http://www.randomurl8.it")
        dm.depth = 1
        dm.dhash = 121212
        dm.source = Source.normal
        dm.delay = 500
        dm.alternatives = [
            "http://www.randomurl8.it"
        ]

        self.qm.add_seen_and_reschedule(dm)

        # checking all the parameters
        counter = self.qm.seen.get(dm.url).get("count")
        dhash = self.qm.seen.get(dm.url).get("page_hash")
        self.assertEqual(counter, 1)
        self.assertEqual(dhash, dm.dhash)
        with mock.patch("time.time", mock_time):
            refetching_data = self.qm.pop()
        
        self.assertEqual(refetching_data.delay,
                         CONFIGURATION["queues"]["refetching-delay"])
        self.assertEqual(refetching_data.source, Source.refetch)

    @freeze_time(CURRENT_TIME)
    def test_reschedule5(self):
        ############################################
        # inserting a priority url
        dm = DocumentMetadata("http://www.randomurl8.it")
        dm.depth = 1
        dm.dhash = 121212
        dm.source = Source.priority
        dm.delay = 500
        dm.alternatives = [
            "http://www.randomurl8.it"
        ]

        self.qm.add_seen_and_reschedule(dm)

        # checking all the parameters
        counter = self.qm.seen.get(dm.url).get("count")
        dhash = self.qm.seen.get(dm.url).get("page_hash")
        self.assertEqual(counter, 1)
        self.assertEqual(dhash, dm.dhash)
        with mock.patch("time.time", mock_time):
            refetching_data = self.qm.pop()
        
        self.assertEqual(refetching_data.delay,
                         START_DELAY)
        self.assertEqual(refetching_data.source, Source.priority)
        
if __name__ == "__main__":
    unittest.main()
