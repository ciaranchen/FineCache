import hashlib
import os
import re
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
    def __init__(self, base_path: str, template: str):
        """
        初始化IncrementDir类。

        :param base_path: 基础目录路径。
        :param template: 匹配和生成文件名的模板字符串。
        """
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            self.base_path.mkdir(exist_ok=True)
        assert "{id}" in template
        self.template = template
        logger.debug(f"Increment Dir: {self.base_path.absolute()}")

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
        template_str = self.template.replace('{id}', str(new_id))
        return template_str.format(*args, **kwargs)

    def new_path(self, *args, **kwargs):
        """
        :return: 应新建的文件路径
        """
        return self.base_path / self.new_name(*args, **kwargs)
