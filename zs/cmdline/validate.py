# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys

from zs import ZSCorrupt
from .util import open_zs

def command_validate(opts):
    """Check a .zs file for errors or data corruption.

Usage:
  zs validate [-j PARALLELISM] [--] <zs_file>

Arguments:
  <zs_file>  Path or URL pointing to a .zs file. An argument beginning with
             the four characters "http" will be treated as a URL.

Options:
  -j PARALLELISM             The number of CPUs to use for decompression.
                             [default: guess]
"""
    #import pdb; pdb.set_trace()
    with open_zs(opts) as z:
        try:
            z.validate()
        except ZSCorrupt as e:
            sys.stdout.write(str(e))
            sys.stdout.write("\n")
            return 1
        else:
            sys.stdout.write("looks good!\n")
            return 0
