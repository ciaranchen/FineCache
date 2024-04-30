import inspect
import pickle
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Tuple, Callable, Dict, Any, List
from zipfile import ZipFile, ZIP_DEFLATED

from .CachedCall import CachedCall, FilenameConfig, DefaultOptions
import os
import json

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class BaseCache:
    """
    缓存基础类。
    """
    def __init__(self, base_path=None):
        super().__init__()
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)

    def cache(self, args_hash: List[Callable[[Any], str]] = None,
              kwargs_hash: List[Callable[[str, Any], Tuple[str, str]]] = None,
              config: FilenameConfig = None):
        def _cache(func: Callable) -> Callable:
            @wraps(func)
            def _get_result(*args, **kwargs):
                call = CachedCall(func, args, kwargs, args_hash=args_hash, kwargs_hash=kwargs_hash, config=config)
                if self.exists(call):
                    return self.get(call)
                else:
                    self.set(call)
                    return call.result

            return _get_result

        return _cache

    def exists(self, call: CachedCall) -> bool:
        """
        检查缓存文件是否存在
        :param call:
        :return:
        """
        pass

    def get(self, call: CachedCall):
        """
        从缓存文件获取结果
        :param call:
        :return:
        """
        pass

    def set(self, call) -> None:
        """
        将运行结果缓存到缓存文件中
        :param call:
        :return:
        """
        pass


class PickleCache(BaseCache):
    def __init__(self, base_path=None, cfg_path=None):
        """

        :param base_path: 保存的文件夹，默认为当前文件夹。
        :param cfg_path: 配置文件的路径，主要用于指定保存文件的路径格式。
        """
        super().__init__(base_path)

    @staticmethod
    def is_picklable(obj: Any) -> bool:
        """
        判断是否可以被pickle缓存
        :param obj:
        :return:
        """
        try:
            pickle.dumps(obj)
            return True
        except:
            logger.info(f'parameters: {obj} could not be pickle')
            return False

    def exists(self, call: CachedCall):
        filename = os.path.join(self.base_path, call.filename + '.pk')
        return os.path.exists(filename) and os.path.isfile(filename)

    def get(self, call: CachedCall) -> Any:
        filename = os.path.join(self.base_path, call.filename + '.pk')
        with open(filename, 'rb') as fp:
            data = pickle.load(fp)
        assert call.func.__qualname__ == data['func']
        logger.debug(data)

        # 可以尝试获取更多的数据内容，但是可以直接返回 'result'
        # n_call = CachedCall(data['func'], data['args'], data['kwargs'], result=data['result'])
        return data['result']

    @staticmethod
    def _construct_content(call):
        """
        构造函数调用缓存的内容
        :param call:
        :return:
        """
        args = [a if PickleCache.is_picklable(a) else None for a in call.args]
        kwargs = {k: v if PickleCache.is_picklable(v) else None for k, v in call.kwargs.items()}
        result = call.result
        if not PickleCache.is_picklable(result):
            logger.error(f"{result} isn't picklable...")
            logger.error(f"{call.func.__qualname__}, args: {args}, kwargs: {kwargs}")
            raise pickle.PickleError("not a picklable result...")

        return {
            'func': call.func.__qualname__,
            'args': args,
            'kwargs': kwargs,
            'result': result,
        }

    def set(self, call: CachedCall):
        filename = os.path.join(self.base_path, call.filename + '.pk')
        content = self._construct_content(call)
        logger.debug(content)
        with open(filename, 'wb') as fp:
            pickle.dump(content, fp)


@dataclass
class VersionConfig:
    version: int = 1

    def increment(self):
        self.version += 1

    def save_to_file(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self.version, f)

    @classmethod
    def load_from_file(cls, filename):
        with open(filename, 'rb') as f:
            version = pickle.load(f)
        new_instance = cls(version)
        return new_instance


class HistoryCache(BaseCache):
    """
    这个类只保存函数代码和运行结果，内容可以直接查看。
    """

    def __init__(self, base_path=None, tracking_files=None, cfg_path=None, args_hash=None, kwargs_hash=None):
        super().__init__(args_hash, kwargs_hash)
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)
        self.config = FilenameConfig()
        self.config.set_path(base_path, cfg_path)
        self.tracking_files = tracking_files if tracking_files else []
        self.filename_template = 'v{ver}.{suffix}'

    def cache(self, func: Callable) -> Callable:
        @wraps(func)
        def _get_result(*args, **kwargs):
            call = CachedCall(func, args, kwargs, args_hash=self.args_hash, kwargs_hash=self.kwargs_hash)
            self.set(call)
            return call.result

        return _get_result

    def exists(self, call: CachedCall):
        path = os.path.join(self.base_path, self.config.get_id(call))
        return os.path.exists(path) and os.path.isdir(path)

    def set(self, call):
        path = os.path.join(self.base_path, self.config.get_id(call))
        os.makedirs(path, exist_ok=True)
        version_path = os.path.join(path, '.version.txt')

        ver_conf = VersionConfig() if not os.path.exists(version_path) else VersionConfig.load_from_file(version_path)

        old_filename = self.filename_template.format(ver=ver_conf.version, suffix='py')
        old_path = os.path.join(path, old_filename)
        # 若存在旧文件，则存在新文件，否则直接存在旧文件
        if os.path.exists(old_path):
            with open(old_path, encoding='utf-8') as fp:
                lines = fp.readlines()
                old_code = ''.join(lines[1:])
            # 若现有代码与历史代码不一致
            if inspect.getsource(call.func) != old_code:
                # 保存结果到新的版本
                ver_conf.increment()
            else:
                # 否则无需保存结果，只照常运行即可
                call.run()
                return
        ver_conf.save_to_file(version_path)

        # Save function code
        func_code_filename = os.path.join(path, self.filename_template.format(ver=ver_conf.version, suffix='py'))
        src_filename = inspect.getsourcefile(call.func)
        lines, line_num = inspect.getsourcelines(call.func)
        with open(func_code_filename, 'w', encoding='utf-8') as fp:
            fp.write(f'# {src_filename} L{line_num} V{ver_conf.version}\n')
            fp.writelines(lines)

        pickle_filename = os.path.join(path, self.filename_template.format(ver=ver_conf.version, suffix='pk'))
        content = PickleCache._construct_content(call)
        content.update({
            'module': call.func.__module__,
            'version': ver_conf.version,
            'runtime': str(datetime.now()),
        })
        logger.debug(content)
        with open(pickle_filename, 'wb') as fp:
            pickle.dump(content, fp)

        if len(self.tracking_files) != 0:
            zip_filename = os.path.join(path, self.filename_template.format(ver=ver_conf.version, suffix='zip'))
            with ZipFile(zip_filename, 'w') as zip_file:
                for f in self.tracking_files:
                    zip_file.write(f, compress_type=ZIP_DEFLATED)

    def explore(self, func, args=(), kwargs=None, key=lambda x: x):
        if kwargs is None:
            kwargs = {}
        call = CachedCall(func, args, kwargs)
        path = os.path.join(self.base_path, self.config.get_id(call))
        if not os.path.exists(path):
            raise Exception(f"Could not explore: {func.__qualname__}(args={args}, kwargs={kwargs}), not exists {path}")
        return [key(os.path.join(path, f[:-5])) for f in os.listdir(path) if f.endswith('json')]
