"Tests for redis datastructures."
import random
import unittest
import sys
import os
import time
import threading
import redis_pipe

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


from redis_dict import RedisDict, PickleRedisDict, JSONRedisDict
from redis_hash_dict import RedisHashDict, PickleRedisHashDict, JSONRedisHashDict,\
                            ExpirableRedisHashDict, ExpirablePickleRedisHashDict, ExpirableJSONRedisHashDict
from redis_path_dict import RedisPathDict
from redis_list import RedisList, PickleRedisList, JSONRedisList
from redis_set import RedisSet, PickleRedisSet, JSONRedisSet
from message_queue import PickleMessageQueue



class TestRedisDatastructures(unittest.TestCase):
    "Test the various data structures."
    prefix = "test_rds"

    def test_redis_dict(self):
        "Test the redis dict implementation."
        key = "%s.dict" % self.prefix
        for class_impl in (RedisDict, PickleRedisDict, JSONRedisDict):
            rd = class_impl()
            del rd[key]
            init_size = len(rd)
            self.assertFalse(key in rd)
            rd[key] = 10
            self.assertTrue(key in rd)
            self.assertEqual(len(rd), init_size + 1)

            # pass through serialize loses type information, whereas
            # the other serializers retain type correctly, hence the
            # ambiguity in this test
            self.assertTrue(rd[key] in  ('10', 10))
            del rd[key]
            self.assertFalse(key in rd)
            self.assertEqual(len(rd), init_size)

    def test_redis_hash_dict(self):
        "Test the redis hash dict implementation."
        hash_key = "%s.hash_dict" % self.prefix
        key = "hello"

        # ensure dictionary isn't here
        rd = RedisDict()
        del rd[hash_key]

        for class_impl in (RedisHashDict, PickleRedisHashDict, JSONRedisHashDict):
            rhd = class_impl(hash_key)
            self.assertEqual(len(rhd), 0)
            self.assertFalse(key in rhd)
            rhd[key] = 10
            self.assertTrue(key in rhd)
            self.assertEqual(len(rhd), 1)

            # pass through serialize loses type information, whereas
            # the other serializers retain type correctly, hence the
            # ambiguity in this test
            self.assertTrue(rhd[key] in ('10', 10))
            del rhd[key]
            self.assertFalse(key in rhd)
            self.assertEqual(len(rhd), 0)

    def test_expirable_redis_hash_dict(self):
        "Test the redis hash dict implementation."
        hash_key = "%s.hash_dict" % self.prefix
        key = "hello"

        # ensure dictionary isn't here
        rd = RedisDict()
        del rd[hash_key]

        for class_impl in (ExpirableRedisHashDict, ExpirablePickleRedisHashDict, ExpirableJSONRedisHashDict):
            rhd = class_impl(hash_key)
            rhd.set_default_expiration(0.1)
            self.assertEqual(len(rhd), 0)
            self.assertFalse(key in rhd)
            rhd[key] = 10
            self.assertTrue(key in rhd)
            self.assertEqual(len(rhd), 1)

            # pass through serialize loses type information, whereas
            # the other serializers retain type correctly, hence the
            # ambiguity in this test
            self.assertTrue(rhd[key] in ('10', 10))
            del rhd[key]
            self.assertFalse(key in rhd)
            self.assertEqual(len(rhd), 0)
            rhd[key] = 100
            self.assertTrue(key in rhd)
            time.sleep(0.101)
            self.assertTrue(key not in rhd)
            del rhd[key]


    # def test_redis_path_dict(self):
    #     "Test the redis hash dict implementation."
    #     hash_key = "%s.hash_dict" % self.prefix
    #     key = "hello"
    #
    #     for class_impl in (RedisPathDict, ):
    #         rhd = class_impl(hash_key)
    #         rhd.delete_all()
    #         rhd = class_impl(hash_key)
    #         self.assertEqual(len(rhd), 0)
    #         self.assertFalse(key in rhd)
    #         rhd[key] = 10
    #         self.assertTrue(key in rhd)
    #         self.assertEqual(len(rhd), 1)
    #
    #         # pass through serialize loses type information, whereas
    #         # the other serializers retain type correctly, hence the
    #         # ambiguity in this test
    #         self.assertTrue(rhd[key] in ('10', 10))
    #         del rhd[key]
    #         self.assertFalse(key in rhd)
    #         self.assertEqual(len(rhd), 0)

    def test_redis_list(self):
        "Test the redis hash dict implementation."
        list_key = "%s.list" % self.prefix

        # ensure list isn't here
        rd = RedisDict()
        del rd[list_key]

        for class_impl in (RedisList, PickleRedisList, JSONRedisList):
            rl = class_impl(list_key)
            self.assertEqual(len(rl), 0)
            rl.append("a")            
            rl.append("b")
            self.assertEqual(len(rl), 2)
            self.assertEquals(rl[0], "a")
            self.assertEquals(rl[-1], "b")
            self.assertEquals(rl[:1], ["a", "b"])
            self.assertEquals(rl[:], ["a", "b"])
            self.assertEquals(rl.pop(), "b")
            self.assertEquals(rl.pop(), "a")

    def test_redis_set(self):
        "Test redis set."
        set_key = "%s.list" % self.prefix

        # ensure set isn't here
        rd = RedisDict()
        del rd[set_key]

        for class_impl in (RedisSet, PickleRedisSet, JSONRedisSet):
            rs = class_impl(set_key)
            self.assertEquals(len(rs), 0)
            rs.add("a")
            rs.add("a")
            rs.update(("a", "b"))
            self.assertEquals(len(rs), 2)
            self.assertTrue(rs.pop() in ("a", "b"))
            self.assertEquals(len(rs), 1)
            self.assertTrue(rs.pop() in ("a", "b"))
            self.assertEquals(len(rs), 0)

    def test_msg_queue(self):
        messages = [random.randrange(0, 300) for i in xrange(0, random.randrange(0, 100))] + ["stop"]
        client = redis_pipe.RedisPipe()
        key = "test_msg_q"

        def reader_func():
            read = PickleMessageQueue([key], client)
            cont = True
            received_messages = []
            with read:
                while cont:
                    read_msg = read.read()
                    received_messages.append(read_msg)
                    cont = read_msg != "stop"
            self.assertEqual(received_messages, messages)

        writer = PickleMessageQueue([key], redis_client=client)
        for i in range(0, 10):
            threading.Thread(target=reader_func).start()

        for msg in messages:
            writer.write(msg)





if __name__ == '__main__':
    unittest.main()
