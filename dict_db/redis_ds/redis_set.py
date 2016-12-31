"""A Pythonic interface to a Redis set."""

import redis_config
import redis_pipe
from serialization import PassThroughSerializer, PickleSerializer, JSONSerializer


class RedisSet(PassThroughSerializer):
    """An object which behaves like a Python set, but which is based by Redis."""
    def __init__(self, set_key, redis_client=redis_config.CLIENT):
        "Initialize the set."
        self._client = redis_client or redis_pipe.RedisPipe()
        self.set_key = set_key
        
    def __len__(self):
        """Number of values in the set."""
        return self._client.scard(self.set_key)

    def __iter__(self):
        for item in self._client.sscan_iter(self.set_key):
            yield self.deserialize(item)

    def delete_all(self):
        self._client.delete(self.set_key)

    def add(self, val):
        """Add a value to the set."""
        val = self.serialize(val)
        self._client.sadd(self.set_key, val)
        
    def update(self, vals):
        """Idempotently add multiple values to the set."""
        vals = [self.serialize(x) for x in vals]
        self._client.sadd(self.set_key, *vals)

    def __contains__(self, val):
        """Check if a value is a member of a set."""
        return self._client.sismember(self.set_key, self.serialize(val))
        
    def pop(self):
        """Remove and return a value from the set."""
        return self.deserialize(self._client.spop(self.set_key))
    
    def remove(self, val):
        """Remove a specific value from the set."""
        self._client.srem(self.set_key, self.serialize(val))
    
    def __unicode__(self):
        """Represent all members in a set."""
        objs = self._client.smembers(self.set_key)
        objs = [self.deserialize(x) for x in objs]
        return u"RedisSet(%s)" % (objs,)

    def __repr__(self):
        "Represent all members in a set."
        return self.__unicode__()


class PickleRedisSet(RedisSet, PickleSerializer):
    "Pickle values stored in set."
    pass


class JSONRedisSet(RedisSet, JSONSerializer):
    "JSON values stored in set."
    pass

