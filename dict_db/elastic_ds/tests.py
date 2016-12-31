import unittest
from doc_dict import ElasticDocDict


class TestDocDict(unittest.TestCase):

    def test_set_get(self):

        d = ElasticDocDict("test", "TestDocDict")
        d.delete_all()
        d["1"] = "a"
        d["a"] = 1
        d["True"] = True
        d["Dict"] = {"a.b": "123", '5': "ddd", "ddd": 5}
        self.assertEqual(d["1"], "a")
        self.assertEqual(d["a"], 1)
        self.assertEqual(d["True"], True)
        self.assertDictEqual(d["Dict"], {"a.b": "123", '5': "ddd", "ddd": 5})
        self.assertItemsEqual(d.keys(), ["1", "a", 'True', "Dict"])

    def test_iteritems(self):
        d = ElasticDocDict("test", "TestDocDict")
        d.delete_all()
        d["1"] = 1
        d["2"] = 2
        d["3"] = 3
        for k, v in d.iteritems():
            self.assertEqual(d[k], v)

    def test_iterkeys(self):
        d = ElasticDocDict("test", "TestDocDict")
        d.delete_all()
        keys = ["1", "2", "3", "4"]
        d["1"] = 1
        d["2"] = 2
        d["3"] = 3
        for k in d.iterkeys():
            self.assertIn(k, keys)

    def test_set_remove_and_len(self):
        d = ElasticDocDict("test", "TestDocDict")
        d.delete_all()
        keys = ["1", "2", "3", "4", "5"]
        count = 0
        for k in keys:
            count += 1
            d[k] = k
            self.assertEqual(count, len(d))
        for k in keys:
            del d[k]
            count -= 1
            self.assertEqual(count, len(d))


if __name__ == '__main__':
    unittest.main()
