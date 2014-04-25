# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys

from .util import open_zs

def command_dump(opts):
    """Unpack some or all of the contents of a .zs file.

Usage:
  zs dump <zs_file>
  zs dump [--start=START] [--stop=STOP] [--prefix=PREFIX]
          [--terminator=TERMINATOR | --length-prefixed=TYPE]
          [-j PARALLELISM]
          [-o FILE]
          [--] <zs_file>
  zs dump --help

Arguments:
  <zs_file>  Path or URL pointing to a .zs file. An argument beginning with
             the four characters "http" will be treated as a URL.

Selection options:
  --start=START            Output only records which are >= START.
  --stop=STOP              Do not output any records which are >= STOP.
  --prefix=PREFIX          Output only records which begin with PREFIX.

  Python string escapes (e.g., "\\n", "\\x00") are allowed. All comparisons
  are performed using ASCIIbetical ordering.

Processing options:
  -j PARALLELISM           The number of CPUs to use for decompression. Note
                           that if you know that you are only reading a small
                           number of records, then -j0 may be the fastest
                           option, since it reduces startup overhead.
                           [default: guess]

Output options:
  -o FILE, --output=FILE   Output to the given file, or "-" for stdout.
                           [default: -]

Record framing options:
  --terminator=TERMINATOR  String used to terminate records in output. Python
                           string escapes are allowed (e.g., "\\n", "\\x00").
                           [default: \\n]
  --length-prefixed=TYPE   Instead of terminating records with a marker,
                           prefix each record with its length, encoded as
                           TYPE. (Options: uleb128, u64le)

  ZS files are organized as a collection of records, which may contain
  arbitrary data. By default, these are output as individual lines. However,
  this may not be a great idea if you have records which themselves contain
  newline characters. As an alternative, you can request that they instead be
  terminated by some arbitrary string, or else request that each record be
  prefixed by its length, encoded in either unsigned little-endian base-128
  (uleb128) format or unsigned little-endian 64-bit (u64le) format.

    """

    if opts["--output"] == "-":
        out_file = sys.stdout
    else:
        out_file = open(opts["--output"], "wb")
    if hasattr(out_file, "buffer"):
        out_file = out_file.buffer

    with open_zs(opts) as z:
        z.dump(out_file,
               start=opts["__start__"],
               stop=opts["__stop__"],
               prefix=opts["__prefix__"],
               terminator=opts["__terminator__"],
               length_prefixed=opts["--length-prefixed"],
               )

    return 0
