import pickle
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Dict, Callable, Any, Tuple

import logging

logger = logging.getLogger(__name__)


@dataclass
class CachedCall:
    func: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    @cached_property
    def result(self):
        return self.func(*self.args, **self.kwargs)


class PickleAgent:
    @staticmethod
    def is_picklable(obj: Any) -> bool:
        """
        判断是否可以被pickle缓存
        """
        try:
            pickle.dumps(obj)
            return True
        except:
            logger.warning(f'parameters: {obj} could not be pickle')
            return False

    @staticmethod
    def get(call: CachedCall, filename: str) -> Any:
        with open(filename, 'rb') as fp:
            data = pickle.load(fp)
        assert call.func.__qualname__ == data['func']
        logger.debug(data)

        # 可以尝试获取更多的数据内容，但是可以直接返回 'result'
        # n_call = CachedCall(data['func'], data['args'], data['kwargs'], result=data['result'])
        return data['result']

    @staticmethod
    def _construct_content(call, result):
        """
        构造函数调用缓存的内容
        """
        args = [a if PickleAgent.is_picklable(a) else None for a in call.args]
        kwargs = {k: v if PickleAgent.is_picklable(v) else None for k, v in call.kwargs.items()}
        if not PickleAgent.is_picklable(result):
            logger.error(f"{result} isn't picklable...")
            logger.error(f"{call.func.__qualname__}, args: {args}, kwargs: {kwargs}")
            raise pickle.PickleError(f"Object {result} is not picklable...")

        return {
            'func': call.func.__qualname__,
            'args': args,
            'kwargs': kwargs,
            'result': result,
            'module': call.func.__module__,
            'runtime': str(datetime.now())
        }

    def set(self, call: CachedCall, result, filename: str):
        content = self._construct_content(call, result)
        logger.debug(content)
        with open(filename, 'wb') as fp:
            pickle.dump(content, fp)
