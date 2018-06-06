"""Implement a crawler"""
import re
import logging
import sys
import threading
import requests

from bs4 import BeautifulSoup

import databases.document_store as ds
from utils.signals_handlers import GracefulKiller
import utils.requests_wrapper as requests_wrapper
import fetcher.fetcher as fetcher
from core.queue_manager import QueueManager

class Crawler():
    """
    Given some configuration retrieve urls and its links.

    One crawler use the configuration of a spider and the global configuration.
    Based on the conf downlads urls and store the content in the appropriate
    collection.
    """
    def __init__(self, spider, cfg):
        self.logger = logging.getLogger(spider.name)
        self.sleep = threading.Event()
        self.spider = spider
        redis_config = cfg["redis"]
        mongodb_config = cfg["mongodb"]
        self.queue = QueueManager(spider.name, spider.restart_delay, cfg)

        # setup output method
        output = cfg.get('output')
        if output:
            if output.get("type") == "json":
                self.documentStore = ds.JsonStore(output.get("filename"))
            elif output.get("type") == "redis":
                self.documentStore = ds.RedisStore(spider.name,
                                                   redis_config['host'],
                                                   redis_config['port'],
                                                   redis_config['db'])
            elif output.get("type") == "mongodb":
                self.documentStore = ds.MongoDBStore(spider.name,
                                                     mongodb_config['host'],
                                                     mongodb_config['port'],
                                                     mongodb_config['db'])
        else:
            self.documentStore = ds.StandardStore()

    def start(self):
        """
        Start the crawling phase.

        The job continues until a sigterm is cought.
        """
        # starting urls end up in priority
        self.queue.init_priority_list(self.spider.start_urls)
        self.queue.add_bootstrap_urls(self.spider.urllist)

        while not GracefulKiller.kill_now:
            dmeta = self.queue.pop()

            # in case of changing spider filters it is better to recheck
            nurl, toremove = self.spider.check_and_normalize(dmeta.url)
            if toremove:
                self.queue.remove_seen(dmeta.url)
            else:
                dmeta.url = nurl
                dmeta.alternatives = [nurl]
                dmeta = fetcher.fetch(self.spider.headers, dmeta)

                if dmeta.status == fetcher.Status.ConnectionError:
                    # CHECK: there were some other signals before. check if this is
                    #        still correct
                    self.queue.add_seen_and_reschedule(dmeta)

                elif dmeta.response:
                    r_url = self.spider.normalize_url(dmeta.response.url)
                    dmeta.alternatives.append(r_url)

                    document, dmeta = self.spider.parse(dmeta)

                    self.queue.add_normal_urls(dmeta)

                    self.documentStore.store(document)
                    self.queue.add_seen_and_reschedule(dmeta)
                    self.sleep.wait(self.spider.delay)
