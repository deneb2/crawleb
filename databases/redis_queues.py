"""Implentation of different clients for Redis"""
import json
import redis
import logging


class RedisPriorityQueue(object):
    """Implement a priority queue with redis."""
    def __init__(self, queue, rhost, rport, rdb):
        self.queue = queue
        self.logger = logging.getLogger(queue)
        self._r = redis.StrictRedis(host=rhost, port=rport, db=rdb)

    def push(self, item, priority):
        return self._r.zadd(self.queue, priority, json.dumps(item))

    def pop(self, current_time=0):
        try:
            _item = self._r.zrange(self.queue, 0, 0)[0]  # INFO: index error if empty
            json_obj = json.loads(_item)
            time_to_fetch = True
            if current_time and json_obj[0] > current_time:
                time_to_fetch = False

            if time_to_fetch:
                if self._r.zrem(self.queue, _item) == 1:
                    return json_obj
                else:
                    # concurrency problem
                    self.logger.debug("concurrent pop on redis priority queue: %s. Retrying..." % (self.queue,))
                    return self.pop(current_time)
        except IndexError:
            # Queue is empty
            self.logger.debug("queue: %s is empty" % (self.queue,))

    def getall(self):
        all_json_values = []
        all_values = self._r.zrange(self.queue, 0, -1)
        for v in all_values:
            all_json_values.append(json.loads(v))
        return all_json_values

    def delete(self, item):
        self._r.zrem(self.queue, json.dumps(item))

    def clear(self):
        self._r.delete(self.queue)


class RedisNormalQueue(object):
    """Implement a FIFO queue with redis."""
    def __init__(self, queue, rhost, rport, rdb):
        self.queue = queue
        self._r = redis.StrictRedis(host=rhost, port=rport, db=rdb)

    def push(self, item):
        json_obj = json.dumps(item)
        return self._r.rpush(self.queue, json_obj)

    def pop(self):
        data = self._r.lpop(self.queue)
        if data:
            return json.loads(data)


class RedisHash(object):
    """Implement an hash map with redis."""
    def __init__(self, hashname, rhost, rport, rdb):
        self.hash = hashname
        self._r = redis.StrictRedis(host=rhost, port=rport, db=rdb)

    def __contains__(self, key):
        return self._r.hexists(self.hash, key)

    def __len__(self):
        return self._r.hlen(self.hash)

    def add(self, key, value):
        self._r.hset(self.hash, key, json.dumps(value))

    def get(self, key):
        value = self._r.hget(self.hash, key)
        if value:
            return json.loads(value)
        
    def getall(self):
        return self._r.hgetall(self.hash)

    def delete(self, key):
        self._r.hdel(self.hash, key)


class RedisPageHash(RedisHash):
    """
    Implement a specific type of hash map with redis.
    
    Main difference is the presence of a counter and the possibility to
    incresse it.
    """
    def __init__(self, hashname, rhost, rport, rdb):
        super(RedisPageHash, self).__init__(hashname, rhost, rport, rdb)

    def add(self, key, page_hash, count=1, alternatives=None):
        value = {"page_hash": page_hash, "count": count}
        if alternatives:
            value["alternatives"] = alternatives
        self._r.hset(self.hash, key, json.dumps(value))

    def incr_n(self, key, n=1):
        # self._r.hincrby(self.hash, key, n)
        value = self.get(key)
        self.add(key, value["page_hash"], value["count"] + n)
