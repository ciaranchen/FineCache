import os
import sys
import re
import json
import shutil
import subprocess
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from typing import Tuple, Callable, Any, List

from FineCache.CachedCall import CachedCall, PickleAgent
from FineCache.utils import IncrementDir, CacheFilenameConfig

import logging

logger = logging.getLogger(__name__)


class FineCache:
    def __init__(self, base_path=None):
        """
        :param base_path: 保存的文件夹，默认为当前文件夹。
        """
        super().__init__()
        self.base_path = base_path if base_path else os.path.abspath(os.getcwd())
        os.makedirs(self.base_path, exist_ok=True)

        # record current changes immediately
        # 在代码初始化的时刻就记录代码的改动，否则运行时间较长时，将导致记录错误的记录。
        self.commit_hash, self.project_root, self.patch_content = self.record_changes()
        self.patch_time = str(datetime.now())

    @staticmethod
    def record_changes():
        # 获取当前的commit hash
        result = subprocess.run(['git', 'rev-parse', 'HEAD', '--show-toplevel'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        commit_hash, project_root = result.stdout.strip().split('\n')

        # 创建一个patch文件，包含当前改动内容
        result = subprocess.run(['git', 'diff', 'HEAD'], stdout=subprocess.PIPE,
                                encoding='utf-8', text=True)
        patch_content = result.stdout
        return commit_hash, project_root, patch_content

    def cache(self, args_hash: List[Callable[[Any], str]] = None,
              kwargs_hash: List[Callable[[str, Any], Tuple[str, str]]] = None,
              config: CacheFilenameConfig = CacheFilenameConfig(),
              agent=PickleAgent()):
        """
        缓存装饰函数的调用结果。每次调用时，检查是否存在已缓存结果，如果存在则直接给出缓存结果。

        :param args_hash:
        :param kwargs_hash:
        :param config:
        :param agent:
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
                    logger.warning(f'Acquire cached {func.__qualname__} result from: {cache_filename}')
                    return agent.get(call, cache_filename)
                else:
                    # 将运行结果缓存到缓存文件中
                    result = call.result
                    agent.set(call, result, cache_filename)
                    return result

            return _get_result

        return _cache

    @contextmanager
    def record_context(self, inc_dir: IncrementDir, comment: str = "", tracking_files: List[str] = None,
                       save_output: bool = True):
        """
        :param inc_dir:
        :param comment: 注释
        :param tracking_files: 保存的追踪文件
        :param save_output: 是否保存输出到单独文件
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

        increment_dir = inc_dir if inc_dir else IncrementDir(self.base_path)
        record_dir = increment_dir.create_new_dir(comment)
        if save_output:
            log_filename = os.path.join(record_dir, 'console.log')
            log_fp = open(log_filename, 'a', encoding='utf-8')
            old_stdout = sys.stdout
            sys.stdout = Tee(old_stdout, log_fp)

        # 将追踪的文件复制到相应位置
        tracking_files = [] if tracking_files is None else tracking_files
        patterns = {re.compile(p): p for p in tracking_files}
        tracking_records = defaultdict(list)
        for root, dirs, files in os.walk(self.project_root):
            if os.path.samefile(root, increment_dir.base_path):
                dirs[:] = []  # 清空dirs列表以跳过此目录及子目录
                continue
            for file in files:
                # 构建完整的文件路径
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, self.project_root)
                for pattern in patterns:
                    # 检查是否匹配正则表达式
                    if pattern.search(relative_path):
                        # 构造目标文件路径
                        dest_file_path = os.path.join(record_dir, relative_path)
                        os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
                        # 复制文件
                        shutil.copy(full_path, dest_file_path)
                        logger.debug(f'Recording {full_path} to {dest_file_path}')
                        # 记录匹配文件的位置
                        tracking_records[patterns[pattern]].append(full_path)

        # 记录改动及信息
        patch_location = os.path.join(record_dir, 'current_changes.patch')
        with open(patch_location, 'w', encoding='utf-8') as patch_file:
            patch_file.write(self.patch_content)
        information = {
            'commit': self.commit_hash,
            'run_time': str(datetime.now()),
            'patch_time': self.patch_time,
            'project_root': self.project_root
        }
        if len(tracking_records.keys()) > 0:
            information['tracking_records'] = tracking_records
        try:
            yield information  # 允许修改information内容
        finally:
            if save_output:
                # 关闭文件接口；恢复stdout
                log_fp.close()
                sys.stdout = old_stdout
            information_filename = os.path.join(record_dir, 'information.json')
            with open(information_filename, 'w', encoding='utf-8') as fp:
                json.dump(information, fp)

    def record(self, increment_dir: IncrementDir = None, comment: str = "", tracking_files: List[str] = None,
               save_output: bool = True):
        """
        保存装饰的函数运行时的代码变更.
        """

        def record_decorator(func):
            @wraps(func)
            def new_func(*args, **kwargs):
                with self.record_context(increment_dir, comment, tracking_files, save_output) as information:
                    res = func(*args, **kwargs)
                    information['record_function'] = func.__qualname__
                    information['run_end_time'] = str(datetime.now())
                return res

            return new_func

        return record_decorator
