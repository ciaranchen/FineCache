import inspect
import pickle
import shutil
from dataclasses import dataclass
from functools import wraps
from shutil import copyfile
from typing import Tuple, Callable, Dict, Any

from cached_call import CachedCall
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

    @wraps
    def __call__(self, func):
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


class PickleCache(BaseCache):
    def __init__(self, base_path=None, cfg_path=None):
        """

        :param base_path: 保存的文件夹，默认为当前文件夹。
        :param cfg_path: 配置文件的路径，主要用于指定保存文件的路径格式。
        """
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        # default setting
        self.join_args = ';'
        self.join_dict = ';'
        self.join_key_value = '-'
        self.template_string = "{func_name}@[{args}]@{kwargs}"
        # Load setting
        if not cfg_path or not os.path.exists(cfg_path):
            cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.cache_config.json')
        with open(cfg_path) as cfg_fp:
            cfg = json.load(cfg_fp)
        self._load_config(cfg)

    def _load_config(self, cfg):
        if 'join_args' in cfg:
            self.join_args = cfg['join_args']
        if 'join_dict' in cfg:
            self.join_dict = cfg['join_dict']
        if 'join_key_value' in cfg:
            self.join_key_value = cfg['join_key_value']
        if 'template_string' in cfg:
            self.template_string = cfg['template_string']

    def _get_id(self, call: CachedCall) -> str:
        """
        获取缓存的文件名
        :param call:
        :return:
        """
        arg_list, kwarg_list = call.arg_lst, call.kwarg_lst
        args_string = self.join_args.join(arg_list)
        kwargs_string = self.join_dict.join([k + self.join_key_value + v for k, v in kwarg_list.items()])
        return self.template_string.format(func_name=call.func.__name__, args=args_string, kwargs=kwargs_string)

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
        """
        判断缓存文件是否存在
        :param call:
        :return:
        """
        filename = os.path.join(self.base_path, self._get_id(call) + '.pk')
        return os.path.exists(filename) and os.path.isfile(filename)

    def get(self, call: CachedCall) -> Any:
        """
        从缓存文件读取函数调用的信息
        :param call:
        :return:
        """
        filename = os.path.join(self.base_path, self._get_id(call) + '.pk')
        with open(filename, 'rb') as fp:
            data = pickle.load(fp)
        assert call.func.__name__ == data['func']
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
            'func': call.func.__name__,
            'args': args,
            'kwargs': kwargs,
            'result': result,
        }

    def set(self, call: CachedCall):
        """
        存储内容
        :param call:
        :return:
        """
        filename = os.path.join(self.base_path, self._get_id(call) + '.pk')
        content = self._construct_content(call)
        with open(filename, 'wb') as fp:
            pickle.dump(content, fp)


class HistoryCache(PickleCache):
    """
    这个类只保存函数代码和运行结果，内容可以直接查看。
    """
    def __init__(self, base_path=None, tracking_files=None, cfg_path=None):
        self.tracking_files = tracking_files if tracking_files else []
        super().__init__(base_path, cfg_path)

    def __call__(self, func):
        def _get_result(*args, **kwargs):
            call = CachedCall(func, args, kwargs)
            self.set(call)
            return call.result

        return _get_result

    def exists(self, call: CachedCall):
        path = os.path.join(self.base_path, self._get_id(call))
        return os.path.exists(path) and os.path.isdir(path)

    def set(self, call):
        path = os.path.join(self.base_path, self._get_id(call))
        os.makedirs(path, exist_ok=True)
        json_filename = os.path.join(path, 'function.json')
        content = self._construct_content(call)
        content.update({
            'func': call.func.__qualname__
            # 'version': self._get_lastest_version()
            # 'runtime': '',
        })
        with open(json_filename, 'w') as fp:
            json.dump(content, fp)

        # Save function code
        func_code_filename = os.path.join(path, 'function.py')
        src_filename = inspect.getsourcefile(call.func)
        lines, line_num = inspect.getsourcelines(call.func)
        with open(func_code_filename, 'w') as fp:
            fp.write('# {} L{}\n'.format(src_filename, line_num))
            fp.writelines(lines)

        for f in self.tracking_files:
            shutil.copy(f, path)
