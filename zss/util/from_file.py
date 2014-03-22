# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import argparse
import multiprocessing
import json

import zss

DESC = """
Convert a file containing sorted, separated records into a structured ZSS
file. (The most common case is where the input is a text file, with each
newline-terminated line as a single ZSS record.)
"""

def main(progname, args):
    parser = argparse.ArgumentParser(progname, description=DESC)
    parser.add_argument("input")
    parser.add_argument("output_zss")
    parser.add_argument("--terminator", default="\\n")
    parser.add_argument("--branching-factor", default=1024, type=int)
    parser.add_argument("--approx-block-size", default=131072, type=int)
    parser.add_argument("-j", "--parallelism",
                        default=multiprocessing.cpu_count(),
                        type=int)
    parser.add_argument("--compression", default="bz2")
    parser.add_argument("--compress-level", type=int)
    parser.add_argument("--uuid")
    parser.add_argument("--metadata", metavar="JSON")
    parser.add_argument("--no-spinner", action="store_false",
                        dest="show_spinner", default=True)

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
                           uuid=args.uuid,
                           show_spinner=args.show_spinner)
    sys.stderr.write("zss: Reading input\n")
    writer.from_file(open(args.input, "rb"), terminator=terminator)
    sys.stderr.write("zss: Done\n")

if __name__ == "__main__":
    main("python -m zss.util.from_file", sys.argv[1:])
