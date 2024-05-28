import os
import pickle
import unittest
from shutil import rmtree

from FineCache import FineCache
from FineCache.cachelib import IncrementDir


def func(a1: int, a2: int, k1="v1", k2="v2"):
    """normal run function"""
    a3 = a1 + 1
    a4 = a2 + 2
    kr1, kr2 = k1[::-1], k2[::-1]
    # print(a1, a2, k1, k2)
    # print(a1, "+ 1 =", a1 + 1)
    return a3, a4, kr1, kr2


class TestFineCache(unittest.TestCase):
    def setUp(self) -> None:
        self.pc = FineCache('.p_cache')

    def tearDown(self):
        super().tearDown()
        # Clear folders...
        if os.path.exists('.p_cache'):
            rmtree('.p_cache')

    def test_wrapped(self):
        wrapped = self.pc.cache()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

    def test_pickle_cache(self):
        args = (3,)
        kwargs = {'a2': 4, 'k1': "v3"}
        wrapped = self.pc.cache()(func)
        self.assertEqual(func(*args, **kwargs), wrapped(*args, **kwargs))
        self.assertEqual(func(*args, **kwargs), wrapped(*args, **kwargs))

    def test_unpicklable_args(self):
        def _test_unpicklable(a1, a2, k1, k2):
            # print(a1, a2, k1, k2)
            return a1, k1

        args = (3, lambda x: x + 2)
        kwargs = {'k1': 4, 'k2': lambda x: x + 3}
        _test_unpicklable(*args, **kwargs)

        wrapped = self.pc.cache()(_test_unpicklable)
        wrapped(*args, **kwargs)

        filepaths = [file for file in os.listdir('.p_cache') if file.startswith(_test_unpicklable.__name__)]
        self.assertEqual(len(filepaths), 1)
        with open(os.path.join('.p_cache', filepaths[0]), 'rb') as fp:
            data = pickle.load(fp)
        self.assertEqual(data['func'], _test_unpicklable.__qualname__)

        self.assertEqual(len(data['args']), 2)
        self.assertEqual(data['args'][0], 3)
        self.assertIsNone(data['args'][1])

        self.assertEqual(data['kwargs']['k1'], 4)
        self.assertIsNone(data['kwargs']['k2'])

    def test_unpicklable_different_action(self):
        def _test_lambda(a1, func1):
            return func1(a1)

        args = (3, lambda x: x)
        res0 = _test_lambda(*args)
        self.assertEqual(res0, 3)
        wrapped = self.pc.cache()(_test_lambda)
        res1 = wrapped(*args)
        self.assertEqual(res1, 3)

        args2 = (3, lambda x: x + 1)
        # 此处不会产生相同结果
        res2 = wrapped(*args2)
        self.assertEqual(res2, 4)

    def test_not_picklable_result(self):
        def _test_unpicklable_result():
            return lambda x: 0

        wrapped = self.pc.cache()(_test_unpicklable_result)
        try:
            wrapped()
        except pickle.PickleError as e:
            pass

    def test_self_defined_hash(self):
        def test_func(a1, a2):
            return a1, a2

        wrapped = self.pc.cache(args_hash=[lambda a1: 'x', lambda a2: 'y'])(test_func)
        wrapped('a1', 'a2')
        self.assertTrue(os.path.exists(os.path.join('.p_cache', "test_func('x','y';).pk")))


class TestHistoryCache(unittest.TestCase):
    def tearDown(self):
        super().tearDown()
        # Clear folders...
        if os.path.exists('.h_cache'):
            rmtree('.h_cache')

    def setUp(self) -> None:
        self.inc = IncrementDir('.h_cache', dir_prefix='test')
        self.hc = FineCache('.h_cache', increment_dir=self.inc)

    def test_wrapped(self):
        wrapped = self.hc.record()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

    def test_output_duplicate(self):
        @self.hc.record()
        def output():
            print('123456789')

        output()
        _, latest_dir = self.inc.latest_number
        filename = os.path.join('.h_cache', latest_dir, 'console.log')
        with open(filename) as fp:
            content = fp.read()
        self.assertTrue('123456789' in content)


if __name__ == '__main__':
    unittest.main()
