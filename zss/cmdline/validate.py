# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys

from zss import ZSSCorrupt
from .util import open_zss

def command_validate(opts):
    """Check a .zss file for errors or data corruption.

Usage:
  zss validate [-j PARALLELISM] [--] <zss_file>

Arguments:
  <zss_file>  Path or URL pointing to a .zss file. An argument beginning with
              the four characters "http" will be treated as a URL.

Options:
  -j PARALLELISM             The number of CPUs to use for decompression.
                             [default: all cpus]
"""
    #import pdb; pdb.set_trace()
    with open_zss(opts) as z:
        try:
            z.validate()
        except ZSSCorrupt as e:
            sys.stdout.write(str(e))
            sys.stdout.write("\n")
            return 1
        else:
            sys.stdout.write("looks good!\n")
            return 0
