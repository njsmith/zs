# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import argparse

import zss

DESC = """
Check a .zss file for self-consistency.
"""

def main(progname, args):
    parser = argparse.ArgumentParser(progname, description=DESC)
    parser.add_argument("zss_file")
    args = parser.parse_args(args)

    reader = zss.ZSS(args.zss_file)
    reader.fsck()

if __name__ == "__main__":
    main("python -m zss.util.check", sys.argv[1:])
