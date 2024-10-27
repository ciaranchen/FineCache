import hashlib
import os
import re
from dataclasses import dataclass
from itertools import zip_longest
from pathlib import Path
from typing import Optional

from FineCache.CachedCall import CachedCall


class HashFunc:
    @staticmethod
    def hash(x, hash_cls=hashlib.md5):
        """
        普通hash算法
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


class IncrementDir:
    def __init__(self, base_path: str, dir_prefix: str = ""):
        """
        初始化IncrementDir类，接收基础路径和目录前缀作为参数。

        :param base_path: 基础目录路径。
        :param dir_prefix: 目录名的前缀。
        """
        self.base_path = Path(base_path)
        self.dir_prefix = dir_prefix

    @property
    def latest_dir(self) -> (Optional[int], Optional[str]):
        """
        返回基础路径下按数字递增命名的最新目录的数字部分。

        :return: 最新目录的数字部分及最新目录名，如果找不到则返回None, None。
        """
        dirs = [d for d in os.listdir(self.base_path) if
                os.path.isdir(os.path.join(self.base_path, d)) and d.startswith(self.dir_prefix)]
        # 筛选出符合前缀且剩余部分为数字的目录，并排序
        suffix_dirs = [d[len(self.dir_prefix):] for d in dirs]
        numeric_parts = []
        for d in suffix_dirs:
            m = re.match(r'\d+', d)
            if m:
                numeric_parts.append((int(m.group(0)), self.dir_prefix + d))
        # 返回最大的数字部分，如果列表为空，则返回None
        if len(numeric_parts) == 0:
            return None, None

        return max(numeric_parts, key=lambda x: x[0])

    def create_new_dir(self, dir_suffix: str = "") -> str:
        """
        创建一个新的目录，目录名基于当前最大数字加一，包含自定义的前缀和后缀。

        :param dir_suffix: 目录名的后缀，默认为空。
        :return: 新创建的目录的完整路径。
        """
        latest_num, _ = self.latest_dir
        new_num = latest_num + 1 if latest_num is not None else 1
        if len(dir_suffix) != 0:
            new_dir_name = f"{self.dir_prefix}{new_num}-{dir_suffix}"
        else:
            new_dir_name = f"{self.dir_prefix}{new_num}"
        new_dir_path = self.base_path / new_dir_name
        new_dir_path.mkdir(exist_ok=True)
        return str(new_dir_path)
