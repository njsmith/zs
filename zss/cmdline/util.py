# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys

from zss import ZSS

def open_zss(opts):
    kwargs = {}
    zss_path_or_url = opts["<zss_file>"]
    if zss_path_or_url.startswith("http"):
        kwargs["url"] = zss_path_or_url
    else:
        kwargs["path"] = zss_path_or_url
    if "__j__" in opts:
        kwargs["parallelism"] = opts["__j__"]
    return ZSS(**kwargs)

def optfail(msg):
    sys.stderr.write(msg)
    sys.stderr.write("\n")
    sys.exit(2)
