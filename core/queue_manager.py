"""Implement how different queues and datastores work together"""
import time
import logging

from databases.redis_queues import RedisNormalQueue, RedisPriorityQueue
from schedulers.base_scheduler import BaseRefetchingStrategy
from core.seen_manager import SeenManager
from core.metadata import DocumentMetadata, Source


class QueueManager():
    def __init__(self, queue, start_delay, cfg):
        self.realtime = cfg.get("realtime", False)            
        self.logger = logging.getLogger(queue)
        self.start_delay = start_delay
        refetching_delay = cfg['queues']['refetching-delay']
        self.refetching_strategy = BaseRefetchingStrategy(self.start_delay,
                                                          refetching_delay)

        redis_config = cfg["redis"]
        mongodb_config = cfg["mongodb"]
        self.normal_store = RedisPriorityQueue(queue + "-normal",
                                               redis_config['host'],
                                               redis_config['port'],
                                               redis_config['db'])
        self.priority_store = RedisPriorityQueue(queue + "-priority",
                                                 redis_config['host'],
                                                 redis_config['port'],
                                                 redis_config['db'])
        self.refetch_store = RedisPriorityQueue(queue + "-refetch",
                                                redis_config['host'],
                                                redis_config['port'],
                                                redis_config['db'])
        self.seen = SeenManager(queue + "-hash",
                                mongodb_config['host'],
                                mongodb_config['port'],
                                mongodb_config['db'])
        if self.realtime:
            self.realtime_queue = RedisPriorityQueue('realtime',
                                        redis_config['host'],
                                        redis_config['port'],
                                        redis_config['db'])

    def remove_seen(self, url):
        """Remove previously seen url"""
        self.seen.delete(url)

    def add_seen_and_reschedule(self, doc_meta):
        """
        Add the url to seen and the appropriate list

        Given the metadata passed, the next refetch is computed and
        the url is added to the correct list.
        """
        is_new = self.seen.is_new(doc_meta.url)
        is_changed = self.seen.is_changed(doc_meta.url, doc_meta.dhash)
        
        if self.realtime and (is_new or is_changed):
            self.realtime_queue.push(doc_meta.url, int(time.time()))

        self.seen.add(doc_meta)
        expire, next_delay = self.refetching_strategy.compute(doc_meta, is_new, is_changed)

        if doc_meta.source == Source.priority:
            self.priority_store.push((expire, doc_meta.url, next_delay, doc_meta.depth), expire)
        else:
            self.refetch_store.push((expire, doc_meta.url, next_delay, doc_meta.depth), expire)
        
    def add_normal_urls(self, dm):
        """Add urls to normal list. Usefull, usually, after extracting urls from a page"""
        for i in dm.links:
            if self.seen.is_new(i):
                data = (dm.depth + 1, i)
                self.normal_store.push(data, dm.depth + 1)
            else:
                self.seen.incr_n(i)

    def add_bootstrap_urls(self, json_objs):
        """
        Add urls to nomal lists.

        This function is specific for the case when we have some bootstrap urls.
        """
        #TODO: is it possible to reuse add_normal_url?
        for i in json_objs:
            if self.seen.is_new(i["url"]):
                # INFO: I add two to depth.
                #       I do not want that bootstrap urls affect the
                #       normal workflow and freshnes. Normally depth 0 is only
                #       for seeds and one is only for page in homepage.
                #       (bootstrap urls are considered _old_ urls)
                data = (int(i["depth"])+2, i["url"])
                self.normal_store.push(data, data[0])

    def init_priority_list(self, urls):
        """Add urls to priority list. This happens at any startup"""
        self.priority_store.clear()
        for u in urls:
            expire, depth = 1, 0
            data = (expire, u, self.start_delay, depth)
            self.priority_store.push(data, expire)
            
    def pop(self):
        """Return the next document to fetch"""
        document_metadata = DocumentMetadata()
        item = self.priority_store.pop(int(time.time()))
        if item:
            logging.debug("Get priority:" + str(item[1]))
            document_metadata.url = item[1]
            document_metadata.depth = item[3]
            document_metadata.delay = item[2]
            document_metadata.source = Source.priority
        else:
            while not item:
                item = self.normal_store.pop()
                if not item:
                    break
                # the following check is needed because urls are stored in seen
                # after seeing them
                # so we can have multiple identical url in normal list.
                # and we do not want to have multiple same urls in refetching list
                if not self.seen.is_new(item[1]):
                    item = None
            if item:
                # In case of network error I repush url on normal queue
                # just to not loose them. So it is possible we have
                # something already seen here.
                # It is not a problem to refetch this cases
                logging.debug("Get normal:" + str(item[1]))
                document_metadata.url = item[1]
                document_metadata.depth = item[0]
                document_metadata.delay = 0
                document_metadata.source = Source.normal
            else:
                item = self.refetch_store.pop(int(time.time()))
                if item:
                    logging.debug("Get Refetch:" + str(item[0]))
                    document_metadata.url = item[1]
                    document_metadata.depth = item[3]
                    document_metadata.delay = item[2]
                    document_metadata.source = Source.refetch

        return document_metadata
