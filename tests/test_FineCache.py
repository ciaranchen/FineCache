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
        self.base_path_name = '.cache'
        self.fc = FineCache(self.base_path_name, "", "test{id}")

    def tearDown(self):
        super().tearDown()
        # Clear folders...
        if os.path.exists(self.base_path_name):
            rmtree(self.base_path_name)

    def test_wrapped(self):
        wrapped = self.fc.cache()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

        wrapped = self.fc.record_main()(func)
        self.assertEqual(wrapped.__qualname__, func.__qualname__)
        self.assertEqual(wrapped.__doc__, func.__doc__)

        wrapped = self.fc.record_output()(func)
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

        filepaths = [file for file in os.listdir(self.base_path_name) if file.startswith(_test_unpicklable.__name__)]
        self.assertEqual(len(filepaths), 1)
        with open(os.path.join(self.base_path_name, filepaths[0]), 'rb') as fp:
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

        wrapped = self.fc.cache(lambda f, *a, **kw: f"{f.__name__}('x','y';).pk")(test_func)
        wrapped('a1', 'a2')
        self.assertTrue(os.path.exists(os.path.join('.cache', "test_func('x','y';).pk")))

    # Test for Record

    def test_record_output(self):
        # Path('./temp.yml').touch()

        @self.fc.record_output('console.log1')
        def output():
            print('123456789')

        with self.fc.record_output('console.log2'):
            output()

        _, latest_dir = self.fc.base_dir.latest

        def check_filename(name):
            filename = os.path.join('.cache', latest_dir, name)
            self.assertTrue(os.path.exists(filename))
            with open(filename) as fp:
                content = fp.read()
            self.assertTrue('123456789' in content)

        check_filename('console.log1')
        check_filename('console.log2')

    def test_main(self):
        self.fc.information['test'] = 'random text'

        @self.fc.record_main()
        def func():
            # print('test main func output')
            pass

        func()
        _, latest_dir = self.fc.base_dir.latest
        filename = os.path.join('.cache', latest_dir, 'information.json')
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fp:
            content = json.load(fp)
        self.assertTrue('test' in content)
        self.assertTrue('random text' == content['test'])

    def test_tracking_files(self):
        base_path = '.test_tracking'
        touch_file = 'temp.yaml'

        Path(touch_file).touch()

        for i in range(3):
            fc = FineCache(base_path, "", "test{id}")
            fc.tracking_files.append(r'.*\.yaml')

            @fc.record_main()
            def func():
                pass

            func()

            num, latest_dir = fc.base_dir.latest
            self.assertEqual(num, i + 1)
            self.assertEqual(len(os.listdir(base_path)), i + 1)
            # 测试是否循环复制了tracking_files
            self.assertFalse(os.path.exists(os.path.join(base_path, latest_dir, 'tests', '.cache')))
            self.assertTrue(os.path.exists(os.path.join(base_path, latest_dir, 'tests', touch_file)))
        # end test
        rmtree(base_path)
        os.remove(touch_file)


if __name__ == '__main__':
    unittest.main()
