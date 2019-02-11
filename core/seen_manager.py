""" Manage the urls seen during drawling"""
from sets import Set

from bs4 import BeautifulSoup
from simhash import hamming_distance

from databases.redis_queues import RedisPageHash
from databases.mongodb_datastore import MongoDBPageHash
from core.metadata import DocumentMetadata
from utils.helpers import canonize

# TODO: this can be parametrized
# hash similarity is used to modify refetch strategy
# if the documents hash change is below this constant then
# is considered ad not changed
SIMILARITY_THRESHOLD = 3L


class SeenManager():
    """
    Store and manage seen pages.

    The idea is to store the pages and all eventual alternatives.
    We want to store also some metadata usefull to compute the next
    fetching date.
    """
    def __init__(self, hashname, rhost, rport, rdb):
        # INFO: I was using redis here before, bu was too big.
        # self.store = RedisPageHash(hashname, rhost, rport, rdb)
        self.store = MongoDBPageHash(hashname, rhost, rport, rdb)

    def add(self, dmeta):
        """ Add a url and its alternatives into seen"""
        assert(isinstance(dmeta, DocumentMetadata))

        # I want to merge previous aternatives with current
        prev_entry = self.get(canonize(dmeta.url))
        prev_alt = []
        if prev_entry:
            prev_alt = prev_entry.get("alternatives")

        canonized = [canonize(a) for a in dmeta.alternatives if a]
        canonized = list(set(canonized + prev_alt))
        for n in canonized:
            self.store.add(n, dmeta.dhash, alternatives=canonized)

    def delete(self, url):
        """Delete a url and its alternatives."""
        if canonize(url) in self.store:
            data = self.store.get(canonize(url))
            for alt in data["alternatives"]:
                self.store.delete(alt)

    def incr_n(self, url):
        """Increment the `count` data associated to an url and its alternatives."""
        if canonize(url) in self.store:
            data = self.store.get(canonize(url))
            for alt in data["alternatives"]:
                self.store.incr_n(alt)

    def is_new(self, url):
        """Return true if the url is seen for the first time."""
        if canonize(url) in self.store:
            return False
        return True

    def is_changed(self, url, page_hash):
        """Return true if the url has changed since the last time."""
        if canonize(url) in self.store:
            data = self.store.get(canonize(url))
            dist = hamming_distance(page_hash, data["page_hash"])
            if dist < SIMILARITY_THRESHOLD:
                return False
        return True
    
    def get(self, url):
        """
        Return the data associated with the url.

        The data we store in seen is a counter since the last time we fetched it and
        the list of alternatives.
        """
        return self.store.get(canonize(url))
