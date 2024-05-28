import inspect
import pickle
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Tuple, Callable, Dict, Any, List
from zipfile import ZipFile, ZIP_DEFLATED

from .CachedCall import CachedCall, CacheFilenameConfig
import os
import json

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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
            raise pickle.PickleError("not a picklable result...")

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


class FineCache:
    def __init__(self, base_path=None, agent_class=PickleAgent):
        """
        :param base_path: 保存的文件夹，默认为当前文件夹。
        """
        super().__init__()
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)
        self.agent = PickleAgent()

    def cache(self, args_hash: List[Callable[[Any], str]] = None,
              kwargs_hash: List[Callable[[str, Any], Tuple[str, str]]] = None,
              config: CacheFilenameConfig = CacheFilenameConfig()):
        """
        缓存装饰函数的调用结果。每次调用时，检查是否存在已缓存结果，如果存在则直接给出缓存结果。
        :param args_hash:
        :param kwargs_hash:
        :param config:
        :return:
        """

        def _cache(func: Callable) -> Callable:
            @wraps(func)
            def _get_result(*args, **kwargs):
                call = CachedCall(func, args, kwargs)
                filename = config.get_filename(call, args_hash=args_hash, kwargs_hash=kwargs_hash)
                cache_filename = os.path.join(self.base_path, filename)
                if os.path.exists(cache_filename) and os.path.isfile(cache_filename):
                    # 从缓存文件获取结果
                    return self.agent.get(call, cache_filename)
                else:
                    # 将运行结果缓存到缓存文件中
                    result = call.result
                    self.agent.set(call, result, cache_filename)
                    return result

            return _get_result

        return _cache


class HistoryCache(FineCache):
    """
    这个类只保存函数代码和运行结果，内容可以直接查看。
    """

    def __init__(self, base_path=None, tracking_files: List[str] = None, cache_result: bool = True):
        super().__init__(base_path)
        self.cache_result = cache_result
        self.tracking_files = tracking_files if tracking_files else []
        self.code_filename = 'code.py'  # 当前版本代码文件
        self.result_filename = 'result.pk'  # 当前版本结果与参数等的保存文件 （对于不支持pickle的参数，将会跳过存储。对于不支持pickle 的函数运行结果，将会报错。）
        self.tracking_filename = 'tracking.zip'  # 所有tracking_files的打包

    def exists(self, call: CachedCall):
        path = os.path.join(self.base_path, call.filename)
        if not (os.path.exists(path) and os.path.isdir(path)):
            return False
        # get the latest version code
        version_path = os.path.join(path, '.version.txt')
        if not os.path.exists(version_path):
            return False

        with open(version_path, 'r') as f:
            latest_version = int(f.read())
        latest_version_folder = os.path.join(path, str(latest_version))
        code_filename = os.path.join(latest_version_folder, self.code_filename)
        with open(code_filename, encoding='utf-8') as fp:
            lines = fp.readlines()
            old_code = ''.join(lines[1:])
        return inspect.getsource(call.func) == old_code

    def set(self, call):
        path = os.path.join(self.base_path, call.filename)
        os.makedirs(path, exist_ok=True)
        version_path = os.path.join(path, '.version.txt')
        if os.path.exists(version_path):
            with open(version_path, 'r') as f:
                latest_version = int(f.read())
        else:
            latest_version = 0

        now_version = latest_version + 1
        now_version_folder = os.path.join(path, str(now_version))
        os.makedirs(now_version_folder, exist_ok=True)

        # 保存当前函数代码
        func_code_filename = os.path.join(now_version_folder, self.code_filename)
        src_filename = inspect.getsourcefile(call.func)
        lines, line_num = inspect.getsourcelines(call.func)
        with open(func_code_filename, 'w', encoding='utf-8') as fp:
            fp.write(f'# {src_filename} L{line_num} V{now_version}\n')
            fp.writelines(lines)

        # 保存当前函数运行结果
        pickle_filename = os.path.join(now_version_folder, self.result_filename)
        content = FineCache._construct_content(call)
        content.update({
            'version': now_version
        })
        logger.debug(content)
        with open(pickle_filename, 'wb') as fp:
            pickle.dump(content, fp)

        if len(self.tracking_files) != 0:
            zip_filename = os.path.join(now_version_folder, self.tracking_filename)
            with ZipFile(zip_filename, 'w') as zip_file:
                for f in self.tracking_files:
                    zip_file.write(f, compress_type=ZIP_DEFLATED)

        # 重新写入 Latest version 文件
        latest_version += 1
        with open(version_path, 'w') as f:
            f.write(str(latest_version))

    def get(self, call: CachedCall) -> Any:
        if self.cache_result:
            path = os.path.join(self.base_path, call.filename)
            version_path = os.path.join(path, '.version.txt')
            with open(version_path, 'r') as f:
                latest_version = int(f.read())
            latest_version_folder = os.path.join(path, str(latest_version))
            filename = os.path.join(latest_version_folder, self.result_filename)
            with open(filename, 'rb') as fp:
                data = pickle.load(fp)
            assert call.func.__qualname__ == data['func']
            logger.debug(data)

            # 可以尝试获取更多的数据内容，但是可以直接返回 'result'
            # n_call = CachedCall(data['func'], data['args'], data['kwargs'], result=data['result'])
            return data['result']
        else:
            # 不读取Cache结果，而是直接进行计算
            return call.result
