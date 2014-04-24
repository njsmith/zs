# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys

from zs import ZS

def open_zs(opts, **kwargs):
    zs_path_or_url = opts["<zs_file>"]
    if zs_path_or_url.startswith("http"):
        kwargs["url"] = zs_path_or_url
    else:
        kwargs["path"] = zs_path_or_url
    if "__j__" in opts:
        kwargs["parallelism"] = opts["__j__"]
    return ZS(**kwargs)

def optfail(msg):
    sys.stderr.write(msg)
    sys.stderr.write("\n")
    sys.exit(2)
