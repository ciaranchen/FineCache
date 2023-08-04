import difflib
import json
import os, hashlib, pickle
import inspect
from enum import Enum
from typing import Any, List, Dict, Callable

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class HashFunc:
    @staticmethod
    def hash(x, hash_cls=hashlib.md5):
        # 创建一个 hashlib 对象
        obj = hash_cls()

        # 将字符串 x 转换为字节串，并更新 hashlib 对象
        obj.update(x.encode('utf-8'))

        # 获取 MD5 哈希值并返回
        return obj.hexdigest()


class EnumPickleState(Enum):
    Picklable = 0
    Unpicklable = 1
    Unhashable = 2


class EnumCacheState(Enum):
    after_init = 0
    after_run = 1
    after_load = 2
    after_dump = 3


class CachedCall:
    def __init__(self, func: Callable, args: List, kwargs: Dict, args_hash=None, kwargs_hash=None) -> None:
        self.status = EnumCacheState.after_init
        self.func = func
        self.args = list(args)
        self.kwargs = kwargs
        self.result = None
        self.default_hash = lambda x: HashFunc.hash(str(x))
        self.args_hash = args_hash
        self.kwargs_hash = kwargs_hash
        print(self.args)

    @staticmethod
    def is_picklable(obj):
        try:
            pickle.dumps(obj)
            return EnumPickleState.Picklable
        except pickle.PicklingError:
            return EnumPickleState.Unpicklable

    def dump2file(self, filename):
        args = [a if b == EnumPickleState.Picklable else None for a, b in zip(self.args, self.args_status)]
        kwargs = {k: self.kwargs[k] for k in self.kwargs.keys() if self.kwargs_status[k] == EnumPickleState.Picklable}

        content = {
            'func': self.func.__name__,
            'code': inspect.getsource(self.func),
            'args': args,
            'kwargs': kwargs,
            'result': self.result,
            'args_status': self.args_status,
            'kwargs_status': self.kwargs_status,
        }
        with open(filename, 'wb') as fp:
            pickle.dump(content, fp)
        self.status = EnumCacheState.after_dump
        return content

    def load_from_file(self, filename):
        with open(filename, 'rb') as fp:
            data = pickle.load(fp)
        assert self.func.__name__ == data['func']
        logger.debug(data)
        for i, a, s in zip(range(len(self.args)), data['args'], data['args_status']):
            if s == EnumPickleState.Picklable:
                logger.debug('loading args [{}]: {} ({}) ...'.format(i, a, type(a)))
                self.args[i] = a
            else:
                logger.debug('Skip {} args {}'.format(s, i))

        for k in data['kwargs_status']:
            if data['kwargs_status'][k] == EnumPickleState.Picklable:
                logger.debug('loading kwargs {}: {} ({}) ...'.format(k, data['kwargs'][k], type(data['kwargs'][k])))
                self.kwargs[k] = data['kwargs'][k]
            else:
                logger.debug('Skip kwargs where key is {} ...'.format(k))

        # TODO: 比较func code 是否一致
        old_code = data['code']
        now_code = inspect.getsource(self.func)

        # if now_code != old_code:
        diff = difflib.ndiff(now_code.splitlines(keepends=True), old_code.splitlines(keepends=True))
        msg = "[[NOTICE]]: Your function code is difference from cached code. Diff: \n"
        logger.info(msg + ''.join(diff))

        self.result = data['result']
        self.status = EnumCacheState.after_load
        return data

    def get_args_ids(self):
        args_lst = []
        self.args_status = []
        for i, a in enumerate(self.args):
            hash_func = self.args_hash[i] if self.args_hash and len(self.args_hash) > i else None
            hash = self.arg2hash(a, hash_func)
            if hash == EnumPickleState.Unhashable:
                self.args_status.append(hash)
            else:
                self.args_status.append(self.is_picklable(a))
            args_lst.append(hash)

        kwargs_lst = {}
        self.kwargs_status = {}
        for k, v in self.kwargs.items():
            hash_func = self.kwargs_hash[k] if self.kwargs_hash and k in self.kwargs_hash else None
            hash = self.arg2hash(v, hash_func)
            status = hash if hash == EnumPickleState.Unhashable else self.is_picklable(v)
            self.kwargs_status[k] = status
            kwargs_lst[k] = hash
        return args_lst, kwargs_lst

    def arg2hash(self, arg, hash_func):
        if hash_func:
            return hash_func(arg)
        else:
            try:
                return self.default_hash(arg)
            except:
                return EnumPickleState.Unhashable

    def run(self):
        self.status = EnumCacheState.after_run
        self.result = self.func(*self.args, **self.kwargs)
        if not self.is_picklable(self.result):
            raise Exception("not a picklable result...")


class BaseCache:
    def __init__(self, cfg_path=None, call_cls=CachedCall, args_hash=None, kwargs_hash=None):
        # default setting
        self.join_args = '-'
        self.join_dict = '-'
        self.join_key_value = ':'
        self.template_string = "{function_name}@{args}@{kwargs}"
        # Load setting
        if not cfg_path or not os.path.exists(cfg_path):
            cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.cache_config.json')
        with open(cfg_path) as cfg_fp:
            cfg = json.load(cfg_fp)
        self.load_config(cfg)
        self.call_cls = call_cls

    def load_config(self, cfg):
        if 'join_args' in cfg:
            self.join_args = cfg['join_args']
        if 'join_dict' in cfg:
            self.join_dict = cfg['join_dict']
        if 'join_key_value' in cfg:
            self.join_key_value = cfg['join_key_value']
        if 'template_string' in cfg:
            self.template_string = cfg['template_string']

    def call2hash(self, call: CachedCall):
        arg_list, kwarg_list = call.get_args_ids
        args_string = self.join_args.join(arg_list)
        kwargs_string = self.join_dict.join([k + self.join_key_value + v for k, v in kwarg_list.items()])
        return self.template_string.format(function_name=call.func.__name__, args=args_string, kwargs=kwargs_string)

    def get_result(self, func):
        def _get_result(*args, **kwargs):
            call = self.call_cls(func, list(args), kwargs)
            _id = self.call2hash(call)
            filename = self.get_filename(_id, call)
            if os.path.exists(filename):
                logger.info('Loading {} from {}'.format(_id, filename))
                call.load_from_file(filename)
            else:
                logger.info('Dumping {} to {}'.format(_id, filename))
                call.run()
                call.dump2file(filename)
            return call.result

        return _get_result

    def __call__(self, func) -> Any:
        return self.get_result(func)

    def get_filename(self, _id, call):
        raise NotImplementedError


class CacheFile(BaseCache):

    def __init__(self, filename, cfg_path=None):
        super().__init__(cfg_path)
        self.filename = filename

    def get_filename(self, _id, call):
        return self.filename


@CacheFile('123')
def run(a1, a2, k1="v1", k2="v2"):
    """normal run function"""
    print(a1, a2, k1, k2)
    # Now the code
    print(a1, "+ 1 =", a1 + 1)
    return a1 + 1, a2, k1, k2


res = run(3, a2=4, k1="v3")
print(res)


def find_text_line(file_path, target_text):
    with open(file_path, 'r', encoding='utf-8') as file:
        file_content = file.read()

    # 查找目标文本在文件中的位置
    target_position = file_content.find(target_text)

    if target_position != -1:
        # 计算目标文本在该位置前有多少换行符
        line_number = file_content.count('\n', 0, target_position) + 1
        return line_number

    return None  # 如果未找到目标文本，返回 None

# 测试
# file_path = 'example.txt'
# target_text = '包含换行符的文本'
# line_number = find_text_line(file_path, target_text)
