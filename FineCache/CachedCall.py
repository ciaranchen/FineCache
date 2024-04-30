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
class FilenameConfig:
    """
    缓存文件的文件名命名规范
    """
    join_list: str = ','
    join_dict: str = ','
    join_key_value: str = '='
    join_func: str = "{func_name}({args};{kwargs})"
    config_path: str = None


class DefaultOptions:
    """
    CachedCall 类的默认选项
    """
    default_config = FilenameConfig()
    default_args_hash = lambda x: HashFunc.hash(repr(x))
    default_kwargs_hash = lambda x, y: (x, HashFunc.hash(repr(y)))


@dataclass
class CachedCall:
    func: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    config: FilenameConfig = None
    args_hash: List[Callable[[Any], str]] = None
    kwargs_hash: List[Callable[[str, Any], Tuple[str, str]]] = None

    def __post_init__(self):
        if self.config is None:
            self.config = DefaultOptions.default_config
        if self.args_hash is None:
            self.args_hash = [DefaultOptions.default_args_hash] * len(self.args)
        if self.kwargs_hash is None:
            self.kwargs_hash = [DefaultOptions.default_kwargs_hash] * len(self.kwargs)


    @cached_property
    def args_str(self) -> [str]:
        res = []
        for args, hash_func in zip_longest(self.args, self.args_hash):
            if hash_func is None:
                res.append(repr(args))
            else:
                res.append(repr(hash_func(args)))
        return res

    @cached_property
    def kwargs_str(self) -> [(str, str)]:
        res = []
        for (key, value), hash_func in zip_longest(self.kwargs.items(), self.kwargs_hash):
            if hash_func is None:
                res.append((key, repr(value)))
            else:
                _key, _value = hash_func(key, value)
                res.append((_key, repr(_value)))
        return res

    @cached_property
    def filename(self):
        args_string = self.config.join_list.join(self.args_str)
        kwargs_string = self.config.join_dict.join([k + self.config.join_key_value + v for k, v in self.kwargs_str])
        return self.config.join_func.format(func_name=self.func.__name__, args=args_string, kwargs=kwargs_string)

    @cached_property
    def result(self):
        return self.func(*self.args, **self.kwargs)
