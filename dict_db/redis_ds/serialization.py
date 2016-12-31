"Mixins for serializing objects."
import json
import cPickle as pickle


class PassThroughSerializer(object):
    "Don't serialize."
    def serialize(self, obj):
        "Support for serializing objects stored in Redis."
        return obj

    def deserialize(self, obj):
        "Support for deserializing objects stored in Redis."
        return obj


class PickleSerializer(PassThroughSerializer):
    """Serialize values using pickle."""
    def serialize(self, obj):
        return pickle.dumps(obj)

    def deserialize(self, obj):
        """Deserialize values using pickle."""
        if obj is None:
            return None
        else:
            return pickle.loads(obj)


class JSONSerializer(PassThroughSerializer):

    class LoadFailure(object):

        def __init__(self, value, err):
            self.value = value
            self.err = err

    @staticmethod
    def default_none(obj):
        return None

    """Serialize values using JSON."""
    def serialize(self, obj):
        return json.dumps(obj, skipkeys=True, default=self.default_none)

    def deserialize(self, obj):
        """Deserialize values using JSON."""
        if obj is None:
            return None
        else:
            try:
                return json.loads(obj)
            except Exception, err:
                return self.LoadFailure(obj, err)
