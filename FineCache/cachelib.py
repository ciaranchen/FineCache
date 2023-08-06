import inspect
import pickle
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Tuple, Callable, Dict, Any
from zipfile import ZipFile, ZIP_DEFLATED

from .cached_call import CachedCall
import os
import json

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


@dataclass
class BaseCache:
    """
    缓存基础类。
    """
    args_hash: Tuple[Callable] = None
    kwargs_hash: Dict[str, Callable] = None

    def cache(self, func):
        @wraps(func)
        def _get_result(*args, **kwargs):
            call = CachedCall(func, args, kwargs)
            if self.exists(call):
                self.get(call)
            else:
                self.set(call)
            return call.result

        return _get_result

    def exists(self, call):
        pass

    def get(self, call):
        pass

    def set(self, call):
        pass


@dataclass
class FilenameConfig:
    join_args: str = ';'
    join_dict: str = ';'
    join_key_value: str = '-'
    template_string: str = "{func_name}@[{args}]@{kwargs}"

    def load(self, cfg: Dict[str, str]):
        if 'join_args' in cfg:
            self.join_args = cfg['join_args']
        if 'join_dict' in cfg:
            self.join_dict = cfg['join_dict']
        if 'join_key_value' in cfg:
            self.join_key_value = cfg['join_key_value']
        if 'template_string' in cfg:
            self.template_string = cfg['template_string']

    def get_id(self, call: CachedCall):
        """
        获取缓存的文件名
        :param call:
        :return:
        """
        arg_list, kwarg_list = call.arg_lst, call.kwarg_lst
        args_string = self.join_args.join(arg_list)
        kwargs_string = self.join_dict.join([k + self.join_key_value + v for k, v in kwarg_list.items()])
        return self.template_string.format(func_name=call.func.__qualname__, args=args_string, kwargs=kwargs_string)


class PickleCache(BaseCache):
    def __init__(self, base_path=None, cfg_path=None):
        """

        :param base_path: 保存的文件夹，默认为当前文件夹。
        :param cfg_path: 配置文件的路径，主要用于指定保存文件的路径格式。
        """
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        # Load setting
        self.config = FilenameConfig()

        # scan path exist:

        cfg_path = cfg_path if cfg_path else '.cache_config.json'
        self.config_path = None
        if cfg_path and os.path.exists(cfg_path):
            self.config_path = cfg_path
        elif cfg_path and base_path and os.path.exists(os.path.join(base_path, cfg_path)):
            self.config_path = os.path.join(base_path, cfg_path)

        if self.config_path:
            with open(self.config_path) as config_fp:
                cfg = json.load(config_fp)
            self.config.load(cfg)

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
        except pickle.PicklingError:
            return False

    def exists(self, call: CachedCall):
        filename = os.path.join(self.base_path, self.config.get_id(call) + '.pk')
        return os.path.exists(filename) and os.path.isfile(filename)

    def get(self, call: CachedCall) -> Any:
        filename = os.path.join(self.base_path, self.config.get_id(call) + '.pk')
        with open(filename, 'rb') as fp:
            data = pickle.load(fp)
        assert call.func.__qualname__ == data['func']
        logger.debug(data)

        n_call = CachedCall(data['func'], data['args'], data['kwargs'], result=data['result'])
        return n_call.result

    @staticmethod
    def _construct_content(call):
        """
        构造函数调用缓存的内容
        :param call:
        :return:
        """
        args = [a if PickleCache.is_picklable(a) else None for a in call.args]
        kwargs = {k: v if PickleCache.is_picklable(v) else None for k, v in call.kwargs.items()}
        result = call.run()
        if not PickleCache.is_picklable(result):
            raise Exception("not a picklable result...")

        return {
            'func': call.func.__qualname__,
            'args': args,
            'kwargs': kwargs,
            'result': result,
        }

    def set(self, call: CachedCall):
        filename = os.path.join(self.base_path, self.config.get_id(call) + '.pk')
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


class HistoryCache(PickleCache):
    """
    这个类只保存函数代码和运行结果，内容可以直接查看。
    """

    def __init__(self, base_path=None, tracking_files=None, cfg_path=None):
        self.tracking_files = tracking_files if tracking_files else []
        self.filename_template = 'v{ver}.{suffix}'
        super().__init__(base_path, cfg_path)

    def cache(self, func: Callable) -> Callable:
        @wraps(func)
        def _get_result(*args, **kwargs):
            call = CachedCall(func, args, kwargs)
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

        version_config = VersionConfig() if not os.path.exists(version_path) else VersionConfig.load_from_file(
            version_path)

        old_filename = self.filename_template.format(ver=version_config.version, suffix='py')
        old_path = os.path.join(path, old_filename)
        # 若存在旧文件，则存在新文件，否则直接存在旧文件
        if os.path.exists(old_path):
            with open(old_path, encoding='utf-8') as fp:
                lines = fp.readlines()
                old_code = ''.join(lines[1:])
            # 若现有代码与历史代码不一致
            if inspect.getsource(call.func) != old_code:
                # 保存结果到新的版本
                version_config.increment()
            else:
                # 否则无需保存结果
                return
        version_config.save_to_file(version_path)

        # Save function code
        func_code_filename = os.path.join(path, self.filename_template.format(ver=version_config.version, suffix='py'))
        src_filename = inspect.getsourcefile(call.func)
        lines, line_num = inspect.getsourcelines(call.func)
        with open(func_code_filename, 'w') as fp:
            fp.write(f'# {src_filename} L{line_num} V{version_config.version}\n')
            fp.writelines(lines)

        json_filename = os.path.join(path, self.filename_template.format(ver=version_config.version, suffix='json'))
        content = self._construct_content(call)
        content.update({
            'module': call.func.__module__,
            'version': version_config.version,
            'runtime': str(datetime.now()),
        })
        logger.debug(content)
        with open(json_filename, 'w') as fp:
            json.dump(content, fp)

        if len(self.tracking_files) != 0:
            zip_filename = os.path.join(path, self.filename_template.format(ver=version_config.version, suffix='zip'))
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
        return [key(f[:-5]) for f in os.listdir(path) if f.endswith('json')]
