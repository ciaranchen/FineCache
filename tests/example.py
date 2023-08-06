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
