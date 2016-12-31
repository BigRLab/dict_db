import collections
from serialization import PassThroughSerializer
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

class Nil(object):
    pass


NIL = Nil()


class KeyShouldBeStringException(ValueError):
    pass


class ElasticDocDict(collections.MutableMapping):
    """
    A Dict interface for ElasticSearch Documents.
    Every item-value is stored as a document with the item-key as the document-id
    """

    KEYS_ID = "__ElasticDocDict_Keys"
    DOT_ESCAPE_SEQ = '_;_'
    DOT_CHAR = "."
    VALUE_FIELD_NAME = "__value__"
    TYPE_FIELD_NAME = "__type__"

    def __init__(self, index, doc_type, es=None):
        if es is None:
            self._es = Elasticsearch()
        else:
            self._es = es
        self._index = index.lower()
        self._doc_type = doc_type
        self._is_in_bulk_mode = False
        self._bulk_commands = []
        # Set Keys document
        bulk(self._es, [
                        {'_op_type': 'update', "_index": self._index, "_type": self._doc_type,
                         "_id": self.KEYS_ID, "doc": {}, "doc_as_upsert": True}
                        ])

    def __repr__(self):
        return dict(self).__repr__()

    def serialize(self, value):
        if isinstance(value, dict):
            return {self.__escape_field(k): v for k, v in value.iteritems()}
        else:
            return {self.VALUE_FIELD_NAME: value, self.TYPE_FIELD_NAME: type(value).__name__}

    def deserialize(self, data):
        value = data.get(self.VALUE_FIELD_NAME, NIL)
        type_name = data.get(self.TYPE_FIELD_NAME, NIL)
        # If dict
        if isinstance(value, Nil) and isinstance(type_name, Nil):
            return {self.__unescape_field(k): v for k, v in data.iteritems()}
        # Otherwise - plain value
        else:
            return value

    @classmethod
    def __escape_field(cls, field_name):
        """
        ES 2 doesn't support '.' in field names (WTF).
        So we escape '.' in fields
        :param field_name: the string to de-dot
        :return: a de-doted filed_name that can be passed to __unescape_field to be restored
        """
        return field_name.replace(cls.DOT_CHAR, cls.DOT_ESCAPE_SEQ)

    @classmethod
    def __unescape_field(cls, field_name):
        """
        @see __escape_field
        """
        return field_name.replace(cls.DOT_ESCAPE_SEQ, cls.DOT_CHAR)

    def has_key(self, key):
        data = self._es.get(index=self._index, doc_type=self._doc_type, id=key, ignore=[404])
        return "_source" in data

    def get(self, key, default=None):
        data = self._es.get(index=self._index, doc_type=self._doc_type, id=key, ignore=[404])
        if "_source" in data:
            return self.deserialize(data['_source'])
        else:
            return default

    def __getitem__(self, key):
        data = self._es.get(index=self._index, doc_type=self._doc_type, id=key, ignore=[404])
        if "_source" in data:
            return self.deserialize(data['_source'])
        else:
            raise KeyError(data)

    def __enter__(self):
        self._is_in_bulk_mode = True
        return self

    def _bulk_add_commands(self, commands):
        self._bulk_commands += commands
        if len(self._bulk_commands) > 100:
            bulk(self._es, self._bulk_commands)
            self._bulk_commands = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        bulk(self._es, self._bulk_commands)
        self._is_in_bulk_mode = False
        self._bulk_commands = []

    def __setitem__(self, key, value):
        assert isinstance(key, basestring), KeyShouldBeStringException("Data loss- Keys are serialized to strings")
        body = self.serialize(value)
        commands = [{'_op_type': 'index', "_index": self._index,
                         "_type": self._doc_type, "_id": key, "_source": body},

                    {'_op_type': 'update', "_index": self._index, "_type": self._doc_type,
                     "_id": self.KEYS_ID, "doc": {self.__escape_field(key): ""},  "doc_as_upsert": True}
                    ]
        if self._is_in_bulk_mode:
            self._bulk_add_commands(commands)
        else:
            # Store value and update keys
            bulk(self._es, commands)

    def upsert(self, key, data):
        """
        Update (or create) an entry in place
        :param key: the key of the updated/created entry
        :param data: The data to update/create
        """
        data = self.serialize(data)
        bulk(self._es, [
                        {'_op_type': 'update', "_index": self._index, "_type": self._doc_type,
                         "_id": key, "doc": data, "doc_as_upsert": True},

                        {'_op_type': 'update', "_index": self._index, "_type": self._doc_type,
                         "_id": self.KEYS_ID, "doc": {self.__escape_field(key): ""}, "doc_as_upsert": True}
                        ])

    def __len__(self):
        return len(self.keys())

    def __get_keys_document(self):
        return {self.__unescape_field(k): v for k, v in self.get(self.KEYS_ID, {}).iteritems()}

    def keys(self):
        return self.__get_keys_document().keys()

    def iterkeys(self):
        for key in self.keys():
            yield key

    __iter__ = iterkeys

    def __delitem__(self, key):
        new_keys = {k: v for k, v in self.__get_keys_document().iteritems() if k != key}
        bulk(self._es, [
            # remove document
            {'_op_type': 'delete', "_index": self._index, "_type": self._doc_type, "_id": key},
            # update key document, removing deleted key
            {'_op_type': 'index', "_index": self._index, "_type": self._doc_type,
             "_id": self.KEYS_ID, "_source": new_keys},
            # TODO: consider enabling scripting for better performance
            # erase key from key document
            # {'_op_type': 'update', "_index": self._index, "_type": self._doc_type,
            #  "_id": self.KEYS_ID, "script": {'script': "ctx._source.remove(field_name)", 'params': {"field_name": key}}}
        ])

    def delete_all(self):
        """
        Remove all keys (and matching ES documents)
        """
        delete_operations = [{'_op_type': 'delete', "_index": self._index, "_type": self._doc_type,
                             "_id": key} for key in self.keys()]
        empty_keys = [{'_op_type': 'index', "_index": self._index, "_type": self._doc_type,
                       "_id": self.KEYS_ID, "_source": {}}]
        bulk(self._es, delete_operations + empty_keys)

    def delete_elastic_index(self):
        """
        Use with caution! this deletes the index associated with this DictDocument,
        and affects all over doc_types and documents stored under the index
        """
        self.delete_all()
        self._es.indices.delete(index=self._index)
