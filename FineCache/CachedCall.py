import hashlib
import json
import os
from dataclasses import dataclass
from functools import cached_property
from itertools import zip_longest
from typing import Dict, Callable, Any, Tuple, List

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


@dataclass
class CachedCall:
    func: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    @cached_property
    def result(self):
        return self.func(*self.args, **self.kwargs)


class HashFunc:
    @staticmethod
    def hash(x, hash_cls=hashlib.md5):
        """
        普通hash算法
        :param x:
        :param hash_cls:
        :return:
        """
        obj = hash_cls()
        obj.update(x.encode('utf-8'))
        return obj.hexdigest()


@dataclass
class CacheFilenameConfig:
    """
    缓存文件的文件名命名规范
    """
    join_list: str = ','
    join_dict: str = ','
    join_key_value: str = '='
    join_func: str = "{func_name}({args};{kwargs})"
    config_path: str = None
    suffix: str = '.pk'

    def get_filename(self, call: CachedCall, args_hash, kwargs_hash):
        if args_hash is None:
            args_hash = [lambda x: HashFunc.hash(repr(x))] * len(call.args)
        if kwargs_hash is None:
            kwargs_hash = [lambda x, y: (x, HashFunc.hash(repr(y)))] * len(call.kwargs)

        def args_str(call) -> [str]:
            res = []
            for args, hash_func in zip_longest(call.args, args_hash):
                if hash_func is None:
                    res.append(repr(args))
                else:
                    res.append(repr(hash_func(args)))
            return res

        def kwargs_str(call) -> [(str, str)]:
            res = []
            for (key, value), hash_func in zip_longest(call.kwargs.items(), kwargs_hash):
                if hash_func is None:
                    res.append((key, repr(value)))
                else:
                    _key, _value = hash_func(key, value)
                    res.append((_key, repr(_value)))
            return res

        args_string = self.join_list.join(args_str(call))
        kwargs_string = self.join_dict.join([k + self.join_key_value + v for k, v in kwargs_str(call)])
        return self.join_func.format(func_name=call.func.__name__, args=args_string, kwargs=kwargs_string) + self.suffix
