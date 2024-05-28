# FineCache

科研项目缓存和记录方案。主要提供两个装饰器：

- `FineCache.cache` : 缓存函数的运行结果和参数，并且在下次以相同的参数调用时取出返回结果。
- `FineCache.record`: 记录实验进行时的代码改动、配置文件及运行信息。

## 安装

```shell
pip install FineCache
```

## 示例 (FineCache.cache)

```python
from FineCache import FineCache

fc = FineCache()


@fc.cache()
def func(a1: int, a2: int, k1="v1", k2="v2"):
    """normal run function"""
    a3 = a1 + 1
    a4 = a2 + 2
    kr1, kr2 = k1[::-1], k2[::-1]
    # print(a1, a2, k1, k2)
    # print(a1, "+ 1 =", a1 + 1)
    return a3, a4, kr1, kr2


func(3, a2=4, k2='v3')
```

### 详细说明

#### FineCache(base_path: str, agent_class: PickleAgent, increment_dir: IncrementDir)

- base_path。基础缓存目录，默认为当前目录。cache的缓存会生成文件。record将会在目录中生成多个文件夹。
- agent_class。为cache所使用的缓存格式，目前仅支持Pickle。（对于不支持pickle的函数参数，将会跳过存储；对于不支持pickle
  的函数运行结果，将会报错。）
- increment_dir。为record使用的自增目录类。参见 `IncrementDir` 说明。

#### FineCache.cache(self, args_hash, kwargs_hash, config = CacheFilenameConfig())

在科研项目（尤其是涉及机器学习的项目）中，通常都需要对通用的数据集进行预处理；进行预处理的结果不应该永久保存，而且又应该避免重复调用繁琐的预处理流程。

这个装饰器能缓存函数的运行结果和参数，并且在下次以相同的参数调用时取出返回结果。

- `args_hash: List[Callable[[Any], str]]`  与 `kwargs_hash: List[Callable[[str, Any], Tuple[str, str]]]`。
  通过这两个参数对函数的参数进行数字摘要，从而确定文件名。默认方法是对参数计算md5值，应该足以应对大多数的情况。
  如果传入的参数为None，则视为使用参数的__repr__可以部分减少写lambda的麻烦。
  需要注意的是，类的方法的首个参数是self，即类的对象。下面是一个使用`args_hash`的示例。

```python
class DataLoader:
    ...

    @FineCache().cache(args_hash=[lambda x: 'DataLoader'])
    def load(self):
        pass
# 产生缓存文件 "load('DataLoader';).pk"
```

- `config` 定义了缓存文件的文件名生成方式。实际上缓存文件名的生成方式是这样调用的。

```python
config.get_filename(call, args_hash, kwargs_hash)
```

#### FineCache.record(self, comment: str = "", tracking_files: List[str] = None, save_output: bool = True)

在进行研究的过程中，尝尝出现需要调整参数或者方法的情况，这时就需要保存函数的原始代码。每一次运行的过程改动可能都不大，每次都进行git
commit来存储当然不现实。

这个装饰器能记录实验进行时的代码改动、配置文件及运行信息。参数说明如下。

- comment: 本次实验的注释。将会影响在base_path下生成文件夹的文件名。
- tracking_files: 需要保存的配置文件，或任何其它文件。可以使用正则表达式。
- save_output: 是否记录当前装饰函数的stdout。这不会影响原有输出。

装饰器将在被装饰的函数运行时，在base_path下生成一个文件夹。文件夹中将包含：

- `information.json`: 必要的信息。包含 记录的时间、记录时HEAD的commit ID。
- `console.log`: 记录的被装饰函数的输出。
- `current_changes.patch`: 与HEAD的差距patch。
- `其它tracking_fiels中记录的文件`。

### 其它说明

#### IncrementDir(base_path: str, dir_prefix: str = "")

- base_path: 基础缓存目录。
- dir_prefix: 生成文件夹名称的前缀。

其生成的文件夹名称为 `{dir_prefix}{num}` 或 `{dir_prefix}{num}-{comment}`。
