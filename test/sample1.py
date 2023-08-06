from FineCache import HistoryCache

hc = HistoryCache(base_path='.cache')


@hc.cache
def run(a1, a2, k1="v1", k2="v2"):
    """
    example run function
    """
    print(a1, a2, k1, k2)
    print(a1, "+ 1 =", a1 + 1)
    return a1 + 1, a2, k1, k2


res = run(3, a2=4, k1="v3")
print(res)
print(run.__qualname__)
print(run.__doc__)
print(run.__module__)

# hc.explore(run)
print(hc.explore(run, (3,), {'a2': 4, 'k1': "v3"}))
