__author__ = 'OrW'


from redis_dict import RedisDict
from redis_set import RedisSet
import redis_config as redis_config


class RedisPathDict(RedisDict):

    def __init__(self, path, redis_client=redis_config.CLIENT):
        super(RedisPathDict, self).__init__(redis_client=redis_client)
        self._path = path
        self._keys = RedisSet(self._build_path("keys", prefix="meta_"), redis_client=redis_client)

    @property
    def path(self):
        return self._path

    def keys(self, pattern="*"):
        """Keys for Redis dictionary."""
        pattern = self._build_path(pattern)
        return self._client.keys(pattern)

    def delete_all(self):
        for key in self._keys:
            with self._client:
                    self._client.delete(key)
                    self._keys.delete_all()

    def __len__(self):
        return len(self._keys)

    def __iter__(self):
        for key in self._keys:
            yield key

    iterkeys = __iter__

    def itervalues(self):
        for key in self:
            yield self[key]

    def iteritems(self):
        for key in self:
            yield key, self[key]

    def _build_path(self, key, prefix=None):
        if prefix is None:
            prefix = ""
        key = "%sPathDict|%s|%s" % (prefix, self.path, key)
        return key

    def __getitem__(self, key):
        """ Retrieve a value by key. """
        key = self._build_path(key)
        return super(RedisPathDict, self).__getitem__(key)

    def __setitem__(self, key, val):
        """Set a value by key."""
        path = self._build_path(key)
        with self._client:
            self._keys.add(key)
            return super(RedisPathDict, self).__setitem__(path, val)

    def __delitem__(self, key):
        """Ensure deletion of a key from dictionary."""
        path = self._build_path(key)
        with self._client:
            self._keys.remove(key)
            return super(RedisPathDict, self).__delitem__(path)

    def __contains__(self, key):
        """Check if database contains a specific key."""
        key = self._build_path(key)
        return super(RedisPathDict, self).__contains__(key)

    def get(self, key, default=None):
        """Retrieve a key's value from the database falling back to a default."""
        # Uses __getitem__ internally - no need to build path twice
        return super(RedisPathDict, self).get(key, default=default)

    def expire(self, key, timeout):
        key = self._build_path(key)
        return super(RedisPathDict, self).expire(key, timeout)
