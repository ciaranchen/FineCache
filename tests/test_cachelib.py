import json
import os
import pickle
import unittest
from pathlib import Path
from shutil import rmtree

from FineCache import FineCache, IncrementDir


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
        self.inc = IncrementDir('.cache', dir_prefix='test')
        self.fc = FineCache('.cache')

    def tearDown(self):
        super().tearDown()
        # Clear folders...
        if os.path.exists('.cache'):
            rmtree('.cache')
        if os.path.exists('temp.yml'):
            os.remove('temp.yml')

    def test_wrapped(self):
        wrapped = self.fc.cache()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

        wrapped = self.fc.record()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

    # Test for Cache
    def test_pickle_cache(self):
        args = (3,)
        kwargs = {'a2': 4, 'k1': "v3"}
        wrapped = self.fc.cache()(func)
        self.assertEqual(func(*args, **kwargs), wrapped(*args, **kwargs))
        self.assertEqual(func(*args, **kwargs), wrapped(*args, **kwargs))

    def test_unpicklable_args(self):
        def _test_unpicklable(a1, a2, k1, k2):
            # print(a1, a2, k1, k2)
            return a1, k1

        args = (3, lambda x: x + 2)
        kwargs = {'k1': 4, 'k2': lambda x: x + 3}
        _test_unpicklable(*args, **kwargs)

        wrapped = self.fc.cache()(_test_unpicklable)
        wrapped(*args, **kwargs)

        filepaths = [file for file in os.listdir('.cache') if file.startswith(_test_unpicklable.__name__)]
        self.assertEqual(len(filepaths), 1)
        with open(os.path.join('.cache', filepaths[0]), 'rb') as fp:
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
        wrapped = self.fc.cache()(_test_lambda)
        res1 = wrapped(*args)
        self.assertEqual(res1, 3)

        args2 = (3, lambda x: x + 1)
        # 此处不会产生相同结果
        res2 = wrapped(*args2)
        self.assertEqual(res2, 4)

    def test_not_picklable_result(self):
        def _test_unpicklable_result():
            return lambda x: 0

        wrapped = self.fc.cache()(_test_unpicklable_result)
        try:
            wrapped()
        except pickle.PickleError as e:
            pass

    def test_self_defined_hash(self):
        def test_func(a1, a2):
            return a1, a2

        wrapped = self.fc.cache(args_hash=[lambda a1: 'x', lambda a2: 'y'])(test_func)
        wrapped('a1', 'a2')
        self.assertTrue(os.path.exists(os.path.join('.cache', "test_func('x','y';).pk")))

    # Test for Record

    def test_record_context(self):
        Path('./temp.yml').touch()

        def output():
            print('123456789')

        with self.fc.record_context(self.inc, comment="test_record_context", tracking_files=[r'.*\.yml']) as info:
            output()
            info['output'] = 'abcdefg'

        _, latest_dir = self.inc.latest_dir
        self.assertTrue('test_record_context' in latest_dir)
        filename = os.path.join('.cache', latest_dir, 'console.log')
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fp:
            content = fp.read()
        self.assertTrue('123456789' in content)
        filename = os.path.join('.cache', latest_dir, 'information.json')
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fp:
            content = fp.read()
        self.assertTrue('abcdefg' in content)
        self.assertTrue(os.path.exists(os.path.join('.cache', latest_dir, 'tests', 'temp.yml')))

    def test_record(self):
        Path('./temp.yml').touch()

        @self.fc.record(self.inc, comment="test_record", tracking_files=[r'.*\.yml'])
        def output():
            pass

        for _ in range(3):
            output()
        num, latest_dir = self.inc.latest_dir
        self.assertEqual(num, 3)
        self.assertEqual(len(os.listdir('.cache')), 3)
        self.assertTrue('test_record' in latest_dir)

        filename = os.path.join('.cache', latest_dir, 'information.json')
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fp:
            data = json.load(fp)
        self.assertEqual(data['record_function'], output.__qualname__)

        # 测试是否循环复制了tracking_files
        self.assertFalse(os.path.exists(os.path.join('.cache', latest_dir, 'tests', '.cache')))
        self.assertTrue(os.path.exists(os.path.join('.cache', latest_dir, 'tests', 'temp.yml')))


if __name__ == '__main__':
    unittest.main()
