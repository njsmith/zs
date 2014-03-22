# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import argparse

import zss

DESC = """
Decompress and dump the contents of a ZSS file to stdout.
"""

def main(progname, args):
    parser = argparse.ArgumentParser(progname, description=DESC)
    parser.add_argument("zss_file")
    parser.add_argument("-t", "--terminator", default="\\n")
    args = parser.parse_args(args)
    sep = args.separator.decode("string_escape")

    reader = zss.ZSS(args.zss_file)
    reader.dump(sys.stdout, sep=sep)

if __name__ == "__main__":
    main("python -m zss.util.dump_file", sys.argv[1:])
