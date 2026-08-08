import hashlib
import os
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class HashFunc:
    @staticmethod
    def hash(x, hash_cls=hashlib.md5):
        """
        普通hash算法
        """
        obj = hash_cls()
        obj.update(x.encode('utf-8'))
        return obj.hexdigest()


def get_default_filename(func, *args, **kwargs):
    # TODO: 确保文件名长度不超限。
    hash_args = [HashFunc.hash(repr(x)) for x in args]
    hash_kwargs = {str(k): HashFunc.hash(repr(v)) for k, v in kwargs.items()}

    str_args = ','.join(hash_args)
    str_kwargs = ','.join([f"{k}={v}" for k, v in hash_kwargs])
    return f"{func.__name__}({str_args};{str_kwargs}).pk"


class IncrementDir:
    _process_locks = {}
    _process_locks_guard = threading.Lock()
    _lock_stale_seconds = 300

    def __init__(self, base_path: str, template: str):
        """
        初始化IncrementDir类。

        :param base_path: 基础目录路径。
        :param template: 匹配和生成文件名的模板字符串。
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        if "{id}" not in template:
            template = "{id}_" + template
        self.template = template
        lock_dir = Path(tempfile.gettempdir()) / 'finecache-incrementdir-locks'
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_id = hashlib.sha256(str(self.base_path.absolute()).encode('utf-8')).hexdigest()
        self._lock_path = lock_dir / f'{lock_id}.lock'
        logger.debug(f"Increment Dir: {self.base_path.absolute()}")

    def _process_lock(self):
        key = str(self._lock_path.absolute())
        with self._process_locks_guard:
            if key not in self._process_locks:
                self._process_locks[key] = threading.Lock()
            return self._process_locks[key]

    @contextmanager
    def _acquire_lock(self):
        process_lock = self._process_lock()
        with process_lock:
            self.base_path.mkdir(parents=True, exist_ok=True)
            while True:
                try:
                    fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                except FileExistsError:
                    if self._is_lock_stale():
                        try:
                            self._lock_path.unlink()
                        except FileNotFoundError:
                            pass
                        continue
                    time.sleep(0.01)
                    continue

                with os.fdopen(fd, 'w', encoding='utf-8') as fp:
                    fp.write(f'pid={os.getpid()}\n')
                    fp.write(f'time={time.time()}\n')
                break

            try:
                yield
            finally:
                try:
                    self._lock_path.unlink()
                except FileNotFoundError:
                    pass

    def _is_lock_stale(self) -> bool:
        try:
            stat = self._lock_path.stat()
        except FileNotFoundError:
            return False
        return (time.time() - stat.st_mtime) > self._lock_stale_seconds

    def _format_name(self, new_id: int, *args, **kwargs):
        template_str = self.template.replace('{id}', str(new_id))
        return template_str.format(*args, **kwargs)

    @property
    def latest(self) -> (Optional[int], Optional[str]):
        """
        返回基础路径下按数字递增命名的最新文件的数字部分。

        :return: 最新目录的数字部分及最新目录名，如果找不到则返回None, None。
        """
        pattern_str = re.sub(r"\{id}", lambda _: r"(?P<id>\d+)", self.template)
        pattern_str = re.sub(r'\{.*}', '(.*)', pattern_str)
        pattern = re.compile(pattern_str)
        dirs = []
        for d in os.listdir(self.base_path):
            res = re.match(pattern, d)
            if res:
                dirs.append((res.group('id'), d))
        # 返回最大的数字部分，如果列表为空，则返回None
        if len(dirs) == 0:
            return None, None
        number_dirs = [(int(_id), d) for _id, d in dirs]
        return max(number_dirs, key=lambda x: x[0])

    def new_name(self, *args, **kwargs):
        """
        args 和 kwargs 为模板字符串使用 str.format 的参数，不包括 {id} 参数。

        :return: 应新建的文件名
        """
        latest_id, _ = self.latest
        if latest_id:
            new_id = latest_id + 1
        else:
            new_id = 1
        return self._format_name(new_id, *args, **kwargs)

    def new_path(self, *args, **kwargs):
        """
        返回并预留应新建的目录路径。

        通过一个基于文件系统的锁包住 ``latest`` 读取和目录创建，保证
        并发调用时每次获取到的 ``id`` 都是唯一的。目录名可以自由包含
        其他模板参数，不会再因为“路径不同就不冲突”而重复使用同一个编号。
        """
        with self._acquire_lock():
            while True:
                latest_id, _ = self.latest
                new_id = (latest_id or 0) + 1
                path = self.base_path / self._format_name(new_id, *args, **kwargs)
                try:
                    path.mkdir()
                except FileExistsError:
                    continue

                return path
