__author__ = 'OrW'

from redis import Redis


class RedisPipe(Redis):
    """
    A Wrapper of the Redis client that allows using the with statement to channel all
    calls within the block into a transaction.
    """

    def __init__(self):
        self._current_pipe = None
        super(RedisPipe, self).__init__()
        self._transaction_results = None

    def __getattribute__(self, name):
        current_pipe = super(RedisPipe, self).__getattribute__("_current_pipe")
        if name == "_current_pipe":
            return current_pipe
        elif current_pipe is not None:
            return getattr(current_pipe, name)
        else:
            return super(RedisPipe, self).__getattribute__(name)

    def __enter__(self):
        assert self._current_pipe is None
        self._current_pipe = self.pipeline()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._current_pipe.execute()
        self._current_pipe = None

    @property
    def transaction_results(self):
        """
        :return the results returned by the last transaction __exit__:
        """
        return self._transaction_results

