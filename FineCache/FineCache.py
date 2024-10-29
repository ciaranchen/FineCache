import os
import sys
import re
import json
import shutil
import subprocess
from collections import defaultdict
from contextlib import ContextDecorator
from datetime import datetime
from functools import wraps
from typing import Callable, List

from FineCache.CachedCall import CachedCall, PickleAgent
from FineCache.utils import IncrementDir, get_default_filename

import logging

logger = logging.getLogger(__name__)


class FineCache:
    def __init__(self, base_path=None, exp_name: str = "", template: str = "exp{id}-{name}"):
        """

        :param base_path: 保存的文件夹，默认为当前文件夹。
        :param exp_name: 实验名称。
        """
        super().__init__()
        self.base_path: str = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)

        self.base_dir = IncrementDir(self.base_path, template)
        self.dir = self.base_dir.new_path(name=exp_name)
        os.makedirs(self.dir, exist_ok=True)

        self.tracking_files = []
        self.information = {}

        # record current changes immediately
        # 在代码初始化的时刻就记录代码的改动，否则运行时间较长时，将导致记录错误的记录。
        self.record_changes()

    def record_changes(self):
        # 获取当前的commit hash
        result = subprocess.run(['git', 'rev-parse', 'HEAD', '--show-toplevel'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        commit_hash, project_root = result.stdout.strip().split('\n')

        # 创建一个patch文件，包含当前改动内容
        result = subprocess.run(['git', 'diff', 'HEAD'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        patch_content = result.stdout
        # 记录改动及信息
        patch_location = os.path.join(self.dir, 'current_changes.patch')
        with open(patch_location, 'w', encoding='utf-8') as patch_file:
            patch_file.write(patch_content)
        self.information['commit'] = commit_hash
        self.information['patch_time'] = str(datetime.now())
        self.information['project_root'] = project_root

    def cache(self, hash_func: Callable = None, agent=PickleAgent()):
        """
        缓存装饰函数的调用结果。每次调用时，检查是否存在已缓存结果，如果存在则直接给出缓存结果。
        """

        def _cache(func: Callable) -> Callable:
            @wraps(func)
            def _get_result(*args, **kwargs):
                call = CachedCall(func, args, kwargs)
                if hash_func is None:
                    filename = get_default_filename(func, *args, **kwargs)
                else:
                    filename = hash_func(func, *args, **kwargs)
                cache_filename: str = os.path.join(self.base_path, filename)
                # 在后期将中间文件夹复制到文件夹中。
                relative_path = os.path.relpath(cache_filename, self.information['project_root'])
                self.tracking_files.append(relative_path)
                if os.path.exists(cache_filename) and os.path.isfile(cache_filename):
                    # 从缓存文件获取结果
                    logger.warning(f'Acquire cached {func.__qualname__} result from: {cache_filename}')
                    return agent.get(call, cache_filename)
                else:
                    # 将运行结果缓存到缓存文件中
                    result = call.result
                    agent.set(call, result, cache_filename)
                    return result

            return _get_result

        return _cache

    def record_main(_self):
        class MainContextDecorator(ContextDecorator):
            def __enter__(self):
                self.record_dir = _self.dir
                _self.information['main_start'] = str(datetime.now())

            def __exit__(self, exc_type, exc_val, exc_tb):
                _self.information['main_end'] = str(datetime.now())
                _self.write_information()
                _self.copy_files()

        return MainContextDecorator()

    def write_information(self, filename='information.json'):
        information_filename = os.path.join(self.dir, filename)
        with open(information_filename, 'w', encoding='utf-8') as fp:
            json.dump(self.information, fp)

    def copy_files(self):
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

    def record_output(_self, filename: str = "console.log"):
        """
        保存装饰的函数运行时的代码变更.
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
