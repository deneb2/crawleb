"""Implement different ways to store the output data"""
import json

from redis_queues import RedisHash
from mongodb_datastore import MongoDB
from utils.helpers import canonize


class StandardStore():
    """Output to stdout."""
    def store(self, data):
        print data.info

    def delete(self, data):
        # is not possible to delete from stdin or a file
        pass


class JsonStore(StandardStore):
    """Output to JSON file."""
    def __init__(self, filename):
        self.fd = open(filename, "w")

    def store(self, data):
        self.fd.write(json.dumps(data.info) + "\n")

    def close(self):
        self.fd.close()


class HashStore(StandardStore):
    """This is a base class to output on databases"""
    def __init__(self):
        ''' This class is a stub'''
        self.db = None
        raise NotImplementedError("This class is a stub. Use a subclass")

    def store(self, data):
        '''
        this function store new data into an hash.
        In case the status is not 200, the data will not be overwritten
        the history maintain anyway the last 10 status and dates.
        '''
        data_old = self.db.get(canonize(data.url))
        if data.status == 200:
            data.history = [(data.fetched_time, data.status)]
            if data_old:
                data.history += data_old["history"][:9]
            to_store = data.info
        else:
            data_old["history"] = [(data.fetched_time, data.status)] + data_old["history"][:9]
            to_store = data_old

        to_store.pop("fetched_time", None)
        to_store.pop("status", None)
        self.db.add(canonize(data.url), to_store)

    def delete(self, data):
        self.db.delete(canonize(data.url))


class RedisStore(HashStore):
    """Output to Redis."""
    def __init__(self, name, host, port, db):
        self.db = RedisHash(name + "-output", host, port, db)


class MongoDBStore(HashStore):
    """Output to Mongodb."""
    def __init__(self, name, host, port, db):
        self.db = MongoDB(name + "-output", host, port, db)
