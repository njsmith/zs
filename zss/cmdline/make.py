# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import json

from zss import ZSSWriter
from .util import optfail

def command_make(opts):
    """Create a new .zss file.

Usage:
  zss make <input_file> <new_zss_file>
  zss make [--terminator TERMINATOR | --length-prefixed=TYPE]
           [-j PARALLELISM]
           [--no-spinner]
           [--branching-factor=FACTOR]
           [--approx-block-size=SIZE]
           [--codec=CODEC] [-z COMPRESS-LEVEL]
           [--metadata=JSON]
           [--no-default-metadata]
           [--]
           <input_file> <new_zss_file>
  zss make --help

Arguments:
  <input_file>    A file containing the records to be packed into the
                  new .zss file. Use "-" for stdin. Records must already be
                  sorted in ASCIIbetical order.
  <new_zss_file>  The file to create. Conventionally uses the file extension
                  ".zss".

Input file options:
  --terminator=TERMINATOR    Treat the input file as containing a series of
                             records separated by TERMINATOR. Standard Python
                             string escapes are supported (e.g., "\\x00" for
                             NUL-terminated records). [default: \\n]
  --length-prefixed=TYPE     Treat the input file as containing a series of
                             records containing arbitrary binary data, each
                             prefixed by its length in bytes, with this length
                             encoded according to TYPE. (Valid options:
                             uleb128, u64le.)

Processing options:
  -j PARALLELISM             The number of CPUs to use for compression.
                             [default: all cpus]
  --no-spinner               Disable the progress meter.

Output file options:
  --branching-factor=FACTOR  Number of keys in each *index* block.
                             [default: 1024]
  --approx-block-size=SIZE   Approximate *uncompressed* size of the records in
                             each *data* block, in bytes. [default: 131072]
  --codec=CODEC              Compression algorithm. (Valid options: none,
                             deflate, bz2.) [default: bz2]
  -z COMPRESS-LEVEL, --compress-level=COMPRESS-LEVEL
                             Degree of compression to use. (Default: 6 for
                             deflate, 9 for bz2.)
  --metadata=JSON            A JSON string representing an arbitrary
                             dictionary of properties, to be stored in the
                             file header. [default: {}]
  --no-default-metadata      By default, 'zss make' adds some extra keys to
                             the resulting file: build-host, build-user,
                             build-time. If you don't want these, pass this
                             option.
"""

    try:
        metadata = json.loads(opts.get("--metadata", "{}"))
    except ValueError as e:
        optfail("error parsing JSON string from --metadata: %s" % (e,))

    codec_kwargs = {}
    if "__compress-level__" in opts:
        codec_kwargs["compress_level"] = opts["__compress-level__"]

    sys.stdout.write("zss: Opening new ZSS file: %s\n"
                     % (opts["<new_zss_file>"],))
    with ZSSWriter(opts["<new_zss_file>"],
                   metadata=metadata,
                   branching_factor=opts["__branching-factor__"],
                   parallelism=opts["__j__"],
                   codec=opts["--codec"],
                   codec_kwargs=codec_kwargs,
                   show_spinner=not opts["--no-spinner"],
                   include_default_metadata=not opts["--no-default-metadata"],
               ) as out_z:
        sys.stdout.write("zss: Reading input file: %s\n"
                         % (opts["<input_file>"],))
        sys.stdout.flush()
        if opts["<input_file>"] == "-":
            in_handle = sys.stdin
        else:
            in_handle = open(opts["<input_file>"], "rb")
        if hasattr(in_handle, "detach"):
            in_handle = in_handle.detach()
        out_z.add_file_contents(in_handle,
                                approx_block_size=opts["__approx-block-size__"],
                                terminator=opts["__terminator__"],
                                length_prefixed=opts["--length-prefixed"])
        out_z.finish()
        sys.stdout.write("zss: Done.\n")

    return 0
