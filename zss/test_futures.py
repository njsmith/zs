from .futures import ProcessPoolExecutor, SerialExecutor

def square(x):
    return x ** 2

def _do_executor_test(executor):
    f2 = executor.submit(square, 2)
    f3 = executor.submit(square, x=3)
    f4 = executor.submit(square, 4)

    assert f2.result() == 4
    assert f3.result() == 9
    # smoke test:
    f4.cancel()

def test_futures():
    _do_executor_test(ProcessPoolExecutor(3))
    _do_executor_test(ProcessPoolExecutor(1))
    _do_executor_test(SerialExecutor())

