""" Implementation of different clients for MongoDB"""
import logging
import pymongo
from pymongo.errors import DocumentTooLarge


class MongoDB(object):
    """ Client to easily use mongodb. """
    def __init__(self, name, host, port, db):
        self.logger = logging.getLogger(name)
        client = pymongo.MongoClient(host, port)
        db = client[db]
        self.table = db[name]

    def add(self, url, data):
        try:
            self.table.update_one({"_id":url}, {"$set": data}, upsert=True)
        except DocumentTooLarge:
            self.logger.warning("Document too large: Skip Document: %s" %url)

    def getall(self):
        return self.table.find({})

    def delete(self, key):
        self.table.delete_one({"_id":key})

    def get(self, key):
        return self.table.find_one({"_id":key})


class MongoDBPageHash(MongoDB):
    """ Mongodb database with an increasing counter. """
    def __init__(self, name, host, port, db):
        super(MongoDBPageHash, self).__init__(name, host, port, db)

    def __contains__(self, key):
        if self.table.find_one({"_id":key}):
            return True
        return False

    def __len__(self):
        return self.table.count()

    def add(self, key, page_hash, count=1, alternatives=None):
        value = {"page_hash":page_hash, "count":count}
        if alternatives:
            value["alternatives"] = alternatives
        # replace_one with upsert:
        # --> will replace the document if any or insert new
        self.table.replace_one({"_id":key}, value, upsert=True)

    def incr_n(self, key, n=1):
        self.table.update_one({"_id":key}, {"$inc":{"count":n}})
