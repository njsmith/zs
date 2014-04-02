# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import argparse
import multiprocessing
import json
import binascii

import zss
import zss.common

DESC = """
Convert a file containing sorted, separated records into a structured ZSS
file. (The most common case is where the input is a text file, with each
newline-terminated line as a single ZSS record.)
"""

def main(progname, args):
    parser = argparse.ArgumentParser(
        progname, description=DESC,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("input")
    parser.add_argument("output_zss")
    parser.add_argument("--terminator", default="\\n",
                        help="Record terminator used in input file "
                        "(C-style backslash escapes allowed)")
    parser.add_argument("--branching-factor", default=1024, type=int,
                        help="Number of keys in each *index* block")
    parser.add_argument("--approx-block-size", default=131072, type=int,
                        help="Approximate *uncompressed* size of *data* "
                             "blocks (in bytes)")
    parser.add_argument("-j", "--parallelism", type=int,
                        default=multiprocessing.cpu_count(),
                        help="Number of CPUs to use")
    parser.add_argument("--compression", default="bz2",
                        help="Compression format to use (options: %s)"
                        % (", ".join(zss.common.codecs)))
    parser.add_argument("--compress-level", type=int,
                        help="Compress level (1-9) "
                             "(default: 6 for zlib, 9 for bz2)")
    parser.add_argument("--metadata", metavar="JSON-STRING")
    parser.add_argument("--no-spinner", action="store_true",
                        dest="hide_spinner", default=False,
                        help="Disable the progress meter")

    args = parser.parse_args(args)
    terminator = args.terminator.decode("string_escape")
    metadata = json.loads(args.metadata)
    compress_kwargs = {}
    if args.compress_level is not None:
        compress_kwargs["compress_level"] = args.compress_level
    sys.stderr.write("zss: Setting up writer\n")
    writer = zss.ZSSWriter(args.output_zss, metadata,
                           args.branching_factor, args.approx_block_size,
                           args.parallelism,
                           compression=args.compression,
                           compress_kwargs=compress_kwargs,
                           show_spinner=not args.hide_spinner)
    sys.stderr.write("zss: Reading input\n")
    writer.from_file(open(args.input, "rb"), terminator=terminator)
    sys.stderr.write("zss: Done\n")

if __name__ == "__main__":
    main("python -m zss.util.from_file", sys.argv[1:])
