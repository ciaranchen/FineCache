# FineCache

之前就已经有不少项目实现过Python的缓存，但是这些项目的目的都是为了优化函数的运行过程。所以在这些项目中，往往将函数的结果保存在内存中或者数据库中。

在进行研究的过程中，尝尝出现需要调整参数或者方法的情况，这时就需要保存函数的原始代码，而且有时候甚至需要保存函数运行的参数。
每一次运行的过程改动可能都不大，每次都用一个git commit来存储当然不现实。

因此为了帮助调参时暂存结果，编写了这个项目。主要的使用类别为两个装饰器：

- PickleCache: 缓存函数的运行结果和参数，并且在下次以相同的参数调用时取出返回结果。
- HistoryCache: 缓存函数的运行结果、参数和函数及指定文件的代码。用于简化和记录函数原始代码的改动。

## 安装

```shell
pip install FineCache
```

## 使用方法 - PickleCache

```python
from FineCache import PickleCache

pc = PickleCache()


@pc.cache()
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

PickleCache初始化参数为缓存的存储路径。默认是储在当前运行的目录。

其中， `pc.cache`具有可选参数如下：

- args_hash和kwargs_hash 参数接受函数的列表。来定义其文件名，进而确定由哪些变量决定参数是否发生变化，是否需要使用不同的cache。默认的函数是对 原参数的repr计算md5值。
- config 可进一步定义文件名的通用格式。

> 对于不支持pickle的参数，将会跳过存储。
> 
> 对于不支持pickle 的函数运行结果，将会报错。

## 使用方法 - HistoryCache

```python
from FineCache import HistoryCache

hc = HistoryCache('.history')


@hc.cache()
def tracking_func():
    """
    tracking function version 1
    :return:
    """
    # change this line and re-run this function
    return 0


tracking_func()
```

HistoryCache 初始化参数如下：

- base_path: 历史记录缓存的额存储路径。默认为当前文件夹。
- tracking_files: 不同版本需要额外保存的文件。
- ignore_cache: 设置False即使在命中的情况下，也运行函数。

可以通过直接改动 HistoryCache 中的变量值来自定义Cache中文件的命名。下面为这几个变量的意义及默认值，

```python
from FineCache import HistoryCache

hc = HistoryCache()
hc.code_filename = 'code.py'  # 当前版本代码文件
hc.result_filename = 'result.pk'  # 当前版本结果与参数等的保存文件 （对于不支持pickle的参数，将会跳过存储。对于不支持pickle 的函数运行结果，将会报错。）
hc.tracking_filename = 'tracking.zip'  # 所有tracking_files的打包
```

`hc.cache` 的定义与 `pc.cache` 一致。

