import unittest
from FineCache import PickleCache, HistoryCache


def func(a1: int, a2: int, k1="v1", k2="v2"):
    """normal run function"""
    a3 = a1 + 1
    a4 = a2 + 2
    kr1, kr2 = k1[::-1], k2[::-1]
    # print(a1, a2, k1, k2)
    # print(a1, "+ 1 =", a1 + 1)
    return a3, a4, kr1, kr2

class TestCachelib(unittest.TestCase):
    def setUp(self):
        self.args = (3,)
        self.kwargs = {'a2': 4, 'k1': "v3"}

    def test_wrapped(self):
        pc = PickleCache()
        wrapped = pc.cache(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

        hc = HistoryCache()
        wrapped = hc.cache(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

    def test_pickle_cache(self):
        pc = PickleCache()
        wrapped = pc.cache(func)

        self.assertEqual(func(*self.args, **self.kwargs), wrapped(*self.args, **self.kwargs))


if __name__ == '__main__':
    unittest.main()
