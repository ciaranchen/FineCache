# FineCache

之前就已经有不少项目实现过Python的缓存，但是这些项目的目的都是为了优化函数的运行过程。所以在这些项目中，往往将函数的结果保存在内存中或者数据库中。

在进行研究的过程中，尝尝出现需要调整参数或者方法的情况，这时就需要保存函数的原始代码，而且有时候甚至需要保存函数运行的参数。每一次运行的过程改动可能都不大，每次都用一个git
commit来存储当然不现实。

因此为了帮助调参时暂存结果，编写了这个项目。主要的使用类别为两个装饰器：

- PickleCache: 缓存函数的运行结果和参数，并且在下次以相同的参数调用时取出返回结果。
- HistoryCache: 缓存函数的运行结果、参数和函数及指定文件的代码。

## 使用方法

```python
from FineCache.cachelib import PickleCache

pc = PickleCache()


@pc.cache
def func(a1: int, a2: int, k1="v1", k2="v2"):
    """normal run function"""
    a3 = a1 + 1
    a4 = a2 + 2
    kr1, kr2 = k1[::-1], k2[::-1]
    # print(a1, a2, k1, k2)
    # print(a1, "+ 1 =", a1 + 1)
    return a3, a4, kr1, kr2


print(func(*(3,), **{'a2': 4, 'k2': 'v3'}))
```

