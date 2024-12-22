import os
import sys
import re
import json
import shutil
import subprocess
import types
from collections import defaultdict
from contextlib import ContextDecorator
from datetime import datetime
from functools import wraps
from typing import Callable

from FineCache.CachedCall import CachedCall, PickleAgent
from FineCache.utils import IncrementDir, get_default_filename

import logging

logger = logging.getLogger(__name__)


class FineCache:
    def __init__(self, base_path=None, template: str = "exp{id}", **kwargs):
        """
        :param base_path: 保存的文件夹，默认为当前文件夹。
        :param template: IncrementDir的模板串。
        """
        super().__init__()
        self.base_path: str = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)

        self.base_dir = IncrementDir(self.base_path, template)
        self.dir = self.base_dir.new_path(**kwargs)
        os.makedirs(self.dir, exist_ok=True)

        self.tracking_files = []
        self.information = {}

        # 获取当前的commit hash
        result = subprocess.run(['git', 'rev-parse', 'HEAD', '--show-toplevel'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        commit_hash, project_root = result.stdout.strip().split('\n')
        self.information['commit'] = commit_hash
        self.information['project_root'] = project_root

    def save_changes(self, filename='changes.patch'):
        """
        最好在代码初始化的时刻就记录代码的改动，否则运行时间较长时，将导致记录错误的记录。
        """
        # 创建一个patch文件，包含当前改动内容
        result = subprocess.run(['git', 'diff', 'HEAD'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        patch_content = result.stdout
        # 记录改动及信息
        patch_location = os.path.join(self.dir, filename)
        with open(patch_location, 'w', encoding='utf-8') as patch_file:
            patch_file.write(patch_content)
        self.information['patch_time'] = str(datetime.now())

    def cache(self, filepath_hash: Callable = get_default_filename, in_dir=True):
        """
        缓存装饰函数的调用结果。每次调用时，检查是否存在已缓存结果，如果存在则直接给出缓存结果。
        """

        def _cache(func: Callable) -> Callable:
            class CallableWrapper:
                def __init__(_self, hash_func):
                    super().__init__()
                    _self.filepath_hash = hash_func
                    _self.fine_cache = self
                    _self.agent = PickleAgent()
                    _self.in_dir = in_dir

                @wraps(func)
                def __call__(_self, *args, **kwargs):
                    call = CachedCall(func, args, kwargs)
                    _filename = _self.filepath_hash(func, *args, **kwargs)
                    if _self.in_dir:
                        cache_path = os.path.join(_self.fine_cache.dir, _filename)
                    else:
                        cache_path = _filename
                    if os.path.exists(cache_path) and os.path.isfile(cache_path):
                        # 从缓存文件获取结果
                        logger.warning(f'Acquire cached {func.__qualname__} result from: {cache_path}')
                        result = _self.agent.get(call, cache_path)
                    else:
                        # 将运行结果缓存到缓存文件中
                        result = call.result
                        _self.agent.set(call, result, cache_path)
                    return result

                def __get__(self, instance, owner):
                    if instance is None:
                        return self
                    else:
                        return types.MethodType(self.__call__, instance)

            return CallableWrapper(filepath_hash)

        return _cache

    def record(_self):
        """
        这个函数应该装饰main函数
        """

        class MainContextDecorator(ContextDecorator):
            def __enter__(self):
                self.record_dir = _self.dir
                _self.information['main_start'] = str(datetime.now())

            def __exit__(self, exc_type, exc_val, exc_tb):
                _self.information['main_end'] = str(datetime.now())
                records = _self._track_files()
                _self.information['tracking_records'] = records
                _self.write_information()

        return MainContextDecorator()

    def write_information(self, filename='information.json'):
        information_filename = os.path.join(self.dir, filename)
        with open(information_filename, 'w', encoding='utf-8') as fp:
            json.dump(self.information, fp)

    def _track_files(self):
        # 将追踪的文件复制到相应位置
        tracking_files = self.tracking_files
        patterns = {re.compile(p): p for p in tracking_files}
        tracking_records = defaultdict(list)
        for root, dirs, files in os.walk(self.information['project_root']):
            if os.path.samefile(root, self.base_path):
                dirs[:] = []  # 清空dirs列表以跳过此目录及子目录
                continue
            for file in files:
                # 构建完整的文件路径
                full_path: str = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, self.information['project_root'])
                for pattern in patterns:
                    # 检查是否匹配正则表达式
                    if pattern.search(relative_path):
                        # 构造目标文件路径
                        dest_file_path = os.path.join(self.dir, relative_path)
                        os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
                        # 复制文件
                        shutil.copy(full_path, dest_file_path)
                        logger.debug(f'Recording {full_path} to {dest_file_path}')
                        # 记录匹配文件的位置
                        tracking_records[patterns[pattern]].append(full_path)
        return tracking_records

    def save_console(_self, filename: str = "console.log"):
        """
        将输出保存至文件。（其实使用logging库可能是更好的选择）
        """

        class Tee:
            def __init__(self, stdout, file):
                self.stdout = stdout
                self.file = file

            def write(self, data):
                """"模仿Linux的tee命令，同时向两个流写入数据"""
                self.stdout.write(data)
                self.file.write(data)

            def flush(self):
                self.stdout.flush()
                self.file.flush()

        class RecordDecorator(ContextDecorator):
            def __init__(self):
                super().__init__()
                self.log_filename = None
                self.log_fp = None
                self.old_stdout = None

            def __enter__(self):
                record_dir = _self.dir
                self.log_filename = os.path.join(record_dir, filename)
                self.log_fp = open(self.log_filename, 'w', encoding='utf-8')
                self.old_stdout = sys.stdout
                sys.stdout = Tee(self.old_stdout, self.log_fp)

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.log_fp.close()
                sys.stdout = self.old_stdout

        return RecordDecorator()
