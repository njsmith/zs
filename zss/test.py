# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path

def test_data_path(path=""):
    test_data_dir = os.path.join(os.path.dirname(__file__), "test-data")
    return os.path.join(test_data_dir, path)

test_data_path.__test__ = False
