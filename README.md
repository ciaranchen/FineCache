# FineCache

科研项目中实验记录工具。

在科研项目（尤其是在深度学习项目）中代码运行的时间都比较长，有复杂的数据预处理步骤和诸多配置。我发现自己经常出现实验运行完成后，都忘记自己改了哪些东西。

本项目旨在保存一些项目的基本修改信息，以供分析和复现。

按照预期的方式使用本项目将为每次实验生成单独的文件夹，文件夹中将包含：

- `information.json`: 必要的信息。文件至少包含以下字段，也能存储添加的其它信息。
    - `commit`: HEAD的commit ID
    - `project_root`: git项目的根目录
    - `patch_time`: 记录patch的时间
    - `main_start`: main开始的时间
    - `main_end`: main结束的时间
    - `tracking_records`: 额外记录的文件名列表（相对于项目根目录的路径）。
- `console.log`: 记录的被装饰函数的输出。
- `changes.patch`: 与HEAD的差距patch。
- 其它 `FineCache.tracking_files` 中记录的文件。
- 以pickle存储的中间结果文件。

## 安装

```shell
pip install FineCache
```

依赖 git。

## 详细说明

### FineCache(self, base_path=None, template: str = "exp{id}", **kwargs)

- `base_path`。基础目录，默认为当前目录。在初始化时，将在 `base_path` 下创建以 `template` 命名自增的实验文件夹，后续在该文件夹下保存内容。

  可用`self.dir`获取创建的实验文件夹。

- `template`。文件夹命名模板字符串。其中`{id}`为自增的序号，可以以str.format的语法插入其它变量，并通过 `**kwargs` 传入具体的参数。

```python
fc = FineCache('.exp_log', "exp{id}-{name}", name="DeepLearningModel")
# 运行一次将产生 `./.exp_log/exp1-DeepLearningModel/`
```

> 由于需要正则表达式匹配`{id}`以自增，所以应该尽量避免在`{id}`的周围没有间隔符地放入太多其他变量。

### FineCache.information

这个变量是一个Dict，并在 `FineCache.record_main` 结束时保存到文件夹中。

在 `FineCache` 初始化时，就已经存储了以下变量：

- `commit`: HEAD的commit ID。
- `project_root`: git项目的根目录。

在其它函数的使用中，也会向此字典存储相应的变量。

### FineCache.tracking_files

这个变量是一个List，其元素为需要保存的配置文件或任何其它文件。 可以使用正则表达式匹配直接的相对路径（不含`./`开头）。

### FineCache.save_changes(self, filename='changes.patch')

一般认为应该在初始化后立即调用。保存当前代码到HEAD的所有改动到实验文件夹中 filename 对应的文件，并向 `information` 中写入时间。

> 恢复时，首先恢复到 commit ID 对应的提交代码，再使用 `git apply <patch_file>` 命令应用补丁文件。

### FineCache.record(self)

可同时作为装饰器或上下文管理器使用。

```python
# fc = FineCache()
@fc.record()
def main():
    pass


# 或
with fc.record():
    pass
```

一般放在程序的主流程中，记录流程的运行开始时间和结束时间，并在主流程结束后调用 `information` 和 `tracking_files`
对应的内容写入目录。

### FineCache.cache(self, hash_func: Callable = None, agent=PickleAgent(), record=True)

这个装饰器能缓存函数的运行结果和参数。每次调用时，检查是否存在已缓存结果，如果存在则直接给出缓存结果。

缓存结果以文件形式存储在 base_dir 文件夹下。

- `hash_func` 接受一个函数，控制如何产生的缓存文件名。

  默认方法是对参数计算md5值，并以`f"{func_name}({str_args};{str_kwargs}).pk"`的方式组装，应该足以应对大多数的情况。

  需要注意的是，类的方法的首个参数是self，即类的对象。下面是一个使用`args_hash`的示例。

```python
# fc = FineCache()
class DataLoader:
    @fc.cache(hash_func=lambda f, *a, **kw: f"{a[0].__class__.__name__}.{f.__name__}.pk")
    def load(self):
        pass


# 将产生缓存文件 "DataLoader.load.pk"
DataLoader().load()
```

- `agent` 为cache所使用的缓存格式，目前仅支持PickleAgent。

  （对于不支持 pickle 的函数参数，将会跳过存储；对于不支持 pickle 的函数运行结果，将会报错。）

- `record` 标识该缓存文件是否应该复制存储到本次的实验文件夹中。

### 其它函数

#### FineCache.save_console(_self, filename: str = "console.log")

也可以同时作为装饰器或上下文管理器使用。

在不影响代码段中向stdout的输出的同时，将输出的内容保存到实验文件夹中 filename 对应的文件。

## 示例

参见 `tests/example_*`。
