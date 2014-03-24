# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# A simple LRU cache.
#
# If we ever drop py2 support then this could be replaced by
# functools.lru_cache.

from collections import OrderedDict

class LRU(object):
    def __init__(self, fn, max_size):
        self._fn = fn
        self._max_size = max_size
        self._data = OrderedDict()

    # supporting kwargs is an extra hassle, so we don't
    def __call__(self, *args):
        if args in self._data:
            # remove item so that reinserting it will move it to the end
            value = self._data.pop(args)
        else:
            value = self._fn(*args)
        self._data[args] = value
        while len(self._data) > self._max_size:
            self._data.popitem(last=False)
        return value

def test_LRU():
    calls = []
    def f(x):
        calls.append(x)
        return x ** 2

    cache = LRU(f, 3)
    assert cache(2) == 4
    assert cache(3) == 9
    assert cache(4) == 16
    assert calls == [2, 3, 4]
    # cache hits do not call fn
    assert cache(2) == 4
    assert cache(3) == 9
    assert calls == [2, 3, 4]
    # when cache is full, least-recently-used items get evicted first
    assert cache(5) == 25 # drops 4
    assert calls == [2, 3, 4, 5]
    assert cache(4) == 16 # drops 2
    assert calls == [2, 3, 4, 5, 4]
    assert cache(2) == 4
    assert calls == [2, 3, 4, 5, 4, 2]
