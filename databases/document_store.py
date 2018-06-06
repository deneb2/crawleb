"""Implement different ways to store the output data"""
import json

from redis_queues import RedisHash
from mongodb_datastore import MongoDB


class StandardStore():
    """Output to stdout."""
    def store(self, data):
        print data.info


class JsonStore(StandardStore):
    """Output to JSON file."""
    def __init__(self, filename):
        self.fd = open(filename, "w")

    def store(self, data):
        self.fd.write(json.dumps(data.info) + "\n")

    def close(self):
        self.fd.close()


class RedisStore(StandardStore):
    """Output to Redis."""
    def __init__(self, name, host, port, db):
        self.db = RedisHash(name + "-output", host, port, db)

    def store(self, data):
        self.db.add(data.url, data.info)


class MongoDBStore(StandardStore):
    """Output to Mongodb."""
    def __init__(self, name, host, port, db):
        self.db = MongoDB(name + "-output", host, port, db)

    def store(self, data):
        self.db.add(data.url, data.info)
