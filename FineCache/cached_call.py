import hashlib
from dataclasses import dataclass
from typing import Dict, Callable, Any, Tuple

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


@dataclass
class CachedCall:
    func: Callable
    args: Tuple[Any]
    kwargs: Dict[str, Any]
    result: Any = None
    default_hash: Callable[..., str] = lambda x: HashFunc.hash(str(x))
    args_hash: Tuple[Callable] = None
    kwargs_hash: Dict[str, Callable] = None

    @property
    def arg_lst(self):
        return [self.arg2hash(a, self.args_hash[i] if self.args_hash and len(self.args_hash) > i else None)
                for i, a in enumerate(self.args)]

    @property
    def kwarg_lst(self):
        return {k: self.arg2hash(v, self.kwargs_hash[k] if self.kwargs_hash and k in self.kwargs_hash else None)
                for k, v in self.kwargs.items()}

    def arg2hash(self, arg, hash_func):
        if hash_func:
            return hash_func(arg)
        else:
            try:
                return self.default_hash(arg)
            except:
                return None

    def run(self):
        self.result = self.func(*self.args, **self.kwargs)
        return self.result
