# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path
from contextlib import contextmanager
from tempfile import mkstemp

def test_data_path(path=""):
    test_data_dir = os.path.join(os.path.dirname(__file__), "data")
    return os.path.join(test_data_dir, path)

test_data_path.__test__ = False

# NEVER USE unlink_first=True WITHOUT O_EXCL
@contextmanager
def tempname(suffix="", unlink_first=False):
    try:
        fd, path = mkstemp(suffix=suffix)
        os.close(fd)
        if unlink_first:
            os.unlink(path)
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

    with tempname(".asdf", unlink_first=True) as name:
        assert not os.path.exists(name)
        assert name.endswith(".asdf")

    with tempname(".asdf", unlink_first=True) as name:
        assert not os.path.exists(name)
        assert name.endswith(".asdf")
        # securely create the file
        os.close(os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL))
    assert not os.path.exists(name)
