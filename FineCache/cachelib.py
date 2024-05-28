import pickle
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Tuple, Callable, Any, List, Optional
import os
import json

from .CachedCall import CachedCall, CacheFilenameConfig

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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
    def latest_number(self) -> (Optional[int], Optional[str]):
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
        latest_num, _ = self.latest_number
        new_num = latest_num + 1 if latest_num is not None else 1
        if len(dir_suffix) != 0:
            new_dir_name = f"{self.dir_prefix}{new_num}-{dir_suffix}"
        else:
            new_dir_name = f"{self.dir_prefix}{new_num}"
        new_dir_path = self.base_path / new_dir_name
        new_dir_path.mkdir(exist_ok=True)
        return str(new_dir_path)


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


class FineCache:
    def __init__(self, base_path=None, agent_class=PickleAgent, increment_dir: IncrementDir = None):
        """
        :param base_path: 保存的文件夹，默认为当前文件夹。
        """
        super().__init__()
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)
        self.agent = PickleAgent()
        self.increment_dir = increment_dir if increment_dir else IncrementDir(self.base_path)

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

    def record(self, comment: str = "", tracking_files: List[str] = None, save_output: bool = True):
        """
        保存装饰的函数运行时的代码变更.

        :param comment:
        :param tracking_files:
        :param save_output:
        :return:
        """

        class Tee:
            """模仿Linux的tee命令，同时向两个流写入数据"""

            def __init__(self, stdout, file):
                self.stdout = stdout
                self.file = file

            def write(self, data):
                self.stdout.write(data)
                self.file.write(data)

            def flush(self):
                self.stdout.flush()
                self.file.flush()

        @contextmanager
        def duplicate_stdout(file_path):
            with open(file_path, 'a', encoding='utf-8') as f:
                old_stdout = sys.stdout
                sys.stdout = Tee(old_stdout, f)
                try:
                    yield
                finally:
                    sys.stdout = old_stdout

        tracking_files = [] if tracking_files is None else tracking_files

        def record_decorator(func):
            @wraps(func)
            def new_func(*args, **kwargs):
                record_dir = self.increment_dir.create_new_dir(comment)
                # 保存输出至文件
                if save_output:
                    log_filename = os.path.join(record_dir, 'console.log')
                    with duplicate_stdout(log_filename):
                        res = func(*args, **kwargs)
                else:
                    res = func(*args, **kwargs)

                # 获取当前的commit hash
                result = subprocess.run(['git', 'rev-parse', 'HEAD', '--show-toplevel'], stdout=subprocess.PIPE,
                                        encoding='utf-8', text=True)
                commit_hash, project_root = result.stdout.strip().split('\n')

                # 创建一个patch文件，包含当前改动内容
                result = subprocess.run(['git', 'diff', 'HEAD'], stdout=subprocess.PIPE,
                                        encoding='utf-8', text=True)
                patch_content = result.stdout
                patch_location = os.path.join(record_dir, 'current_changes.patch')
                with open(patch_location, 'w', encoding='utf-8') as patch_file:
                    patch_file.write(patch_content)

                # 将追踪的文件复制到相应位置
                tracking_records = defaultdict(list)
                for root, dirs, files in os.walk(project_root):
                    for file in files:
                        # 构建完整的文件路径
                        full_path = os.path.join(root, file)
                        relative_path = os.path.relpath(full_path, project_root)
                        for file_pattern in tracking_files:
                            # 检查是否匹配正则表达式
                            pattern = re.compile(file_pattern)
                            if pattern.search(file):
                                # 记录匹配文件的位置
                                tracking_records[pattern].append(relative_path)
                                # 复制文件
                                shutil.copy(full_path, record_dir)
                                logger.debug(f'Recording {full_path} to {record_dir}')

                # 记录信息：
                information = {
                    'commit': commit_hash,
                    'runtime': str(datetime.now()),
                    'record_function': func.__qualname__,
                    'project_root': project_root
                }
                if len(tracking_records.keys()) > 0:
                    information['tracking_records'] = tracking_records
                information_filename = os.path.join(record_dir, 'information.json')
                with open(information_filename, 'w', encoding='utf-8') as fp:
                    json.dump(information, fp)
                return res

            return new_func

        return record_decorator
