import logging
from collections import Callable

__author__ = 'OrW'

import redis_pipe

from serialization import PassThroughSerializer, PickleSerializer, JSONSerializer
import time


class MessageQueue(PassThroughSerializer):

    def __init__(self, channels, redis_client=None):
        assert isinstance(channels, list)
        self._redis_client = redis_client or redis_pipe.RedisPipe()
        self._pub_sub = self._redis_client.pubsub()
        self._channels = channels
        self._listener = None
        self._is_in_read_mode = False
        self._received_subscription = False

    def write(self, message, wait_for_readers = True):
        for channel in self._channels:
            received = 0
            while received == 0:
                received = self._redis_client.publish(channel, self.serialize(message))
                if not wait_for_readers:
                    received = 1
                time.sleep(0)

    def read(self, block=True, accept_old_messages=False):
        """
        :param block should poll until an incoming message is received:
        :param accept_old_messages if set to true, old messages sent before we subscribed will be accepted and returned:
        :return the received message:
        """
        assert self._is_in_read_mode
        message_data = None
        while message_data is None and block:
            message = self._listener.next()
            if (accept_old_messages or self._received_subscription) and message["type"] == "message":
                message_data = message["data"]
            # Check for subscription message
            elif message["type"] == "subscribe":
                self._received_subscription = True
            time.sleep(0)

        if message_data is not None:
            return self.deserialize(message_data)
        else:
            return None

    def __enter__(self):
        self._pub_sub.subscribe(*self._channels)
        self._is_in_read_mode = True
        self._listener = self._pub_sub.listen()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pub_sub.unsubscribe()
        self._is_in_read_mode = False
        self._listener = None


class PickleMessageQueue(MessageQueue, PickleSerializer):
    """Serialize hash-map values using pickle."""
    pass


class JSONMessageQueue(MessageQueue, JSONSerializer):
    """Serialize hash-map values using JSON."""
    pass


class QueueApi(JSONMessageQueue):

    METHOD_NAME_KEY = '$name'

    def __init__(self, channels):
        super(QueueApi, self).__init__(channels)

    def run(self):
        with self:
            while True:
                msg = self.read()
                try:
                    self.message_to_call(msg)
                except AssertionError:
                    logging.exception("Incoming message is malformed- %s" % msg)
                except KeyError:
                    logging.exception("Failed to execute incoming message- %s" % msg)

    def message_to_call(self, msg):
        assert isinstance(msg, dict)
        assert self.METHOD_NAME_KEY in msg
        name = msg[self.METHOD_NAME_KEY]
        method = getattr(self, name, None)
        if isinstance(method, Callable):
            msg_copy = msg.copy()
            del msg_copy[self.METHOD_NAME_KEY]
            method(**msg_copy)
        else:
            raise KeyError("Incoming msg name %s doesn't map to a callable method" % name)