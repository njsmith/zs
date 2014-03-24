# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path
from contextlib import contextmanager
from tempfile import mkstemp

def test_data_path(path=""):
    test_data_dir = os.path.join(os.path.dirname(__file__), "test-data")
    return os.path.join(test_data_dir, path)

test_data_path.__test__ = False

@contextmanager
def tempname(suffix=""):
    try:
        fd, path = mkstemp(suffix=suffix)
        os.close(fd)
        yield path
    finally:
        try:
            os.unlink(path)
        except OSError:
            # if it was already deleted, then that's okay
            pass

def test_tempname():
    with tempname(".asdf") as name:
        assert os.path.exists(name)
        assert name.endswith(".asdf")
    assert not os.path.exists(name)
