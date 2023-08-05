from cachelib import PickleCache, HistoryCache


@HistoryCache()
def run(a1, a2, k1="v1", k2="v2"):
    """normal run function"""
    print(a1, a2, k1, k2)
    # Now the code
    print(a1, "+ 1 =", a1 + 1)
    return a1 + 1, a2, k1, k2


res = run(3, a2=4, k1="v3")
print(res)
