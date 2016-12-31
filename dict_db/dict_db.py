from redis_ds.redis_hash_dict import JSONRedisHashDict
from redis_ds.redis_list import JSONRedisList
from elastic_ds.doc_dict import ElasticDocDict


# DB Types
class Consts(object):
    DB_REDIS = 'redis'
    DB_ELASTIC = 'elastic'

    DS_DICT = 'dict'
    DS_LIST = 'list'

    SER_JSON = 'json'


class DictDbFactory(object):

    def __init__(self, db_type, default_ds_type=Consts.DS_DICT):
        self._db_type = db_type
        self._default_ds_type = default_ds_type

    def create(self, path, name, ds_type=None):
        if ds_type is None:
            ds_type = self._default_ds_type
        if self._db_type == Consts.DB_REDIS:
            if isinstance(name, basestring) and len(name) > 0:
                key = "%s_%s" % (path, name)
            else:
                key = path
            if ds_type == Consts.DS_DICT:
                return JSONRedisHashDict(key)
            elif ds_type == Consts.DS_LIST:
                return JSONRedisList(key)

        elif self._db_type == Consts.DB_ELASTIC:
            if ds_type == Consts.DS_DICT:
                return ElasticDocDict(path, name)
            elif ds_type == Consts.DS_LIST:
                raise NotImplementedError("ElasticSearch list not available yet...")

