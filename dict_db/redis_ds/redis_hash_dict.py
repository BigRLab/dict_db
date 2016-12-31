"""
Module contains RedisHashDict, which allows users to interact with Redis hashes
as if they were Python dictionaries.
"""
import redis_config as redis_config
from serialization import PassThroughSerializer, PickleSerializer, JSONSerializer
import UserDict
import time


class RedisHashDict(UserDict.DictMixin, PassThroughSerializer):
    """A dictionary interface to Redis hash-maps."""
    def __init__(self, hash_key, redis_client=redis_config.CLIENT):
        """Initialize the redis hash-map dictionary interface."""
        self._client = redis_client
        self.hash_key = hash_key

    @property
    def client(self):
        return self._client

    def keys(self):
        """Return all keys in the Redis hash-map."""
        return self._client.hkeys(self.hash_key)

    def iteritems(self):
        for key, value in self._client.hscan_iter(self.hash_key):
            yield (key, self.deserialize(value))

    def iteritems_cursor(self, cursor="0"):
        """
        Same as iteritems, but iteration is done in segments,
        and continued by calling the function again with the returned cursor
        :param cursor:
        :return:
        """
        while cursor != 0:
            cursor, data = self._client.hscan(self.hash_key, cursor=cursor)
            for item in data.items():
                yield cursor, item

    def __iter__(self):
        for k, v in self.iteritems():
            yield k

    iterkeys = __iter__

    def itervalues(self):
        for k, v in self.iteritems():
            yield v

    def __len__(self):
        """Number of key-value pairs in the Redis hash-map."""
        return self._client.hlen(self.hash_key)

    def __getitem__(self, key):
        """Retrieve a value from the hash-map."""
        value = self._client.hget(self.hash_key, key)
        if value is None:
            raise KeyError(key)
        return self.deserialize(value)

    def get(self, key, default=None):
        """Retrieve a key's value or a default value if the key does not exist."""
        value = self._client.hget(self.hash_key, key)
        if value is None:
            return default
        else:
            return self.deserialize(value)

    def increment_key(self, key,  value=1):
        if isinstance(value, int):
            return self._client.hincrby(self.hash_key, key, value)
        elif isinstance(value, float):
            return self._client.hincrbyfloat(self.hash_key, key, value)
        else:
            raise TypeError("increment values should be numbers. not %s" % type(value))

    def __setitem__(self, key, val):
        """Set a key's value in the hashmap."""
        val = self.serialize(val)
        return self._client.hset(self.hash_key, key, val)

    def upsert(self, key, data):
        """
        Update (or create) an entry in place
        :param key: the key of the updated/created entry
        :param data: The data to update/create
        """
        data = self.get(key, {}) or {}
        data.update(data)
        self[key] = data

    def __delitem__(self, key):
        """Ensure a key does not exist in the hashmap."""
        return self._client.hdel(self.hash_key, key)

    def delete_all(self):
        self._client.delete(self.hash_key)

    def __contains__(self, key):
        """Check if a key exists within the hashmap."""
        return self._client.hexists(self.hash_key, key)


class PickleRedisHashDict(RedisHashDict, PickleSerializer):
    """Serialize hash-map values using pickle."""
    pass


class JSONRedisHashDict(RedisHashDict, JSONSerializer):
    """Serialize hash-map values using JSON."""
    pass


class ExpirableRedisHashDict(RedisHashDict):
    """
    A RedisHashDict
    """

    class KeyExpiredError(KeyError):
        pass

    def __init__(self, hash_key):
        super(ExpirableRedisHashDict, self).__init__(hash_key)
        self._default_expiration = None
        self._expiration = PickleRedisHashDict("meta_%s|expiration" % hash_key)

    def set_default_expiration(self, expiration):
        self._default_expiration = expiration

    def iter_fresh_items(self):
        """
        Iterate through the hash-dict, yielding only items that haven't expired yet
        :return an iterator over the dict:
        """
        for k, v in self.iteritems():
            if not self.should_expire(k):
                yield k, v

    def always_get(self, key):
        """
        :param key:
        :return the value of key, ignoring expiration :
        """
        return super(ExpirableRedisHashDict, self).__getitem__(key)

    def __contains__(self, key):
        """Check if a key exists within the hash-map."""
        if self.should_expire(key):
            return False
        else:
            return super(ExpirableRedisHashDict, self).__contains__(key)

    def __setitem__(self, key, value):
        super(ExpirableRedisHashDict, self).__setitem__(key, value)
        if self._default_expiration is not None:
            self.expire(key, timeout=self._default_expiration)

    def set(self, key, value, timeout=None):
        """
        :param key:
        :param value:
        :param timeout: when should the key expire, None means never.
        :return:
        """
        super(ExpirableRedisHashDict, self).__setitem__(key, value)
        self.expire(key, timeout=timeout)

    def get_expiration(self, key):
        expiration = self._expiration.get(key, None)
        return expiration

    def get_expiration_as_text(self, key):
        ex = self.get_expiration(key)
        if ex is None:
            return "Never"
        else:
            return time.ctime(ex)

    def should_expire(self, key):
        expiration = self._expiration.get(key, None)
        should_expire = expiration is not None and time.time() > expiration
        return should_expire

    def __getitem__(self, key):
        """Retrieve a value from the hash-map."""
        if self.should_expire(key):
            del self[key]
            raise self.KeyExpiredError(key)
        else:
            return super(ExpirableRedisHashDict, self).__getitem__(key)

    @property
    def default_expiration(self):
        return self._default_expiration

    def expire(self, key, timeout):
        """
        Set expiration on a key
        :param key:
        :param timeout: time in seconds, or None to remove expiration
        :return:
        """
        if timeout is None:
            self._expiration[key] = None
        else:
            self._expiration[key] = timeout + time.time()


class ExpirablePickleRedisHashDict(ExpirableRedisHashDict, PickleSerializer):
    """Serialize hashmap values using pickle."""
    pass


class ExpirableJSONRedisHashDict(ExpirableRedisHashDict, JSONSerializer):
    """Serialize hashmap values using JSON."""
    pass
