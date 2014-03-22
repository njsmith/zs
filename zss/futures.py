# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# A tiny shim to let zss.reader pretend to use
# concurrent.futures.ProcessPoolExecutor on any version of Python, and whether
# or not we're actually using parallelism.
#
# We prefer to use concurrent.futures to raw multiprocessing, when available,
# because concurrent.futures is more robust against things like the user
# hitting control-C (e.g. from the manual: "Changed in version 3.3: When one
# of the worker processes terminates abruptly, a BrokenProcessPool error is
# now raised. Previously, behaviour was undefined but operations on the
# executor or its futures would often freeze or deadlock." AFAICT no similar
# fixes have ever been applied to multiprocessing.Pool.)
#
# But, concurrent.futures is available on py3 only.
#
# And anyway, we'd like to have the option of just doing things in serial as
# well. So we have a SerialExecutor shim in here as well.
#
# These objects only implement the parts of the concurrent.futures API that
# zss.reader actually uses:
# - Executor.submit()
# - Future.result()
# - Future.cancel()

try:
    from concurrent.futures import ProcessPoolExecutor
    have_process_pool_executor = True
except ImportError:
    have_process_pool_executor = False

class _SerialFuture(object):
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result

    def cancel(self):
        pass

class SerialExecutor(object):
    def submit(self, fn, *args, **kwargs):
        return _SerialFuture(fn(*args, **kwargs))

if not have_process_pool_executor:
    # then fake it!

    import multiprocessing

    class _MultiprocessingFuture(object):
        def __init__(self, async_result):
            self._async_result = async_result

        def result(self):
            return self._async_result.get()

        def cancel(self):
            # Can't be done!
            pass

    class ProcessPoolExecutor(object):
        def __init__(self, num_workers):
            self._pool = multiprocessing.Pool(num_workers)

        def submit(self, fn, *args, **kwargs):
            async_result = self._pool.apply_async(fn, args, kwargs)
            return _MultiprocessingFuture(async_result)
