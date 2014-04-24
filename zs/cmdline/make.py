# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import json

from zs import ZSWriter
from .util import optfail

def command_make(opts):
    """Create a new .zs file.

Usage:
  zs make <metadata> <input_file> <new_zs_file>
  zs make [--terminator TERMINATOR | --length-prefixed=TYPE]
          [-j PARALLELISM]
          [--no-spinner]
          [--branching-factor=FACTOR]
          [--approx-block-size=SIZE]
          [--codec=CODEC] [-z COMPRESS-LEVEL]
          [--no-default-metadata]
          [--]
          <metadata> <input_file> <new_zs_file>
  zs make --help

Arguments:

  <metadata>      Arbitrary JSON-encoded metadata that will be stored in your
                  new ZS file. This must be a JSON "object", i.e., the
                  outermost characters have to be {}. If you're just messing
                  about, then you can just use "{}" here and be done, but for
                  any file that will live for long then we strongly recommend
                  adding more details about what this file is. See the
                  "Metadata conventions" section of the ZS manual for more
                  information.

  <input_file>    A file containing the records to be packed into the
                  new .zs file. Use "-" for stdin. Records must already be
                  sorted in ASCIIbetical order. You may want to do something
                  like:
                    cat myfile.txt | env LC_ALL=C sort | zs make - myfile.zs

  <new_zs_file>  The file to create. Conventionally uses the file extension
                 ".zs".

Input file options:
  --terminator=TERMINATOR    Treat the input file as containing a series of
                             records separated by TERMINATOR. Standard Python
                             string escapes are supported (e.g., "\\x00" for
                             NUL-terminated records). The default is
                             appropriate for standard Unix/OS X text files. If
                             your have a text file with Windows-style line
                             endings, then you'll want to use "\\r\\n"
                             instead. [default: \\n]
  --length-prefixed=TYPE     Treat the input file as containing a series of
                             records containing arbitrary binary data, each
                             prefixed by its length in bytes, with this length
                             encoded according to TYPE. (Valid options:
                             uleb128, u64le.)

Processing options:
  -j PARALLELISM             The number of CPUs to use for compression.
                             [default: guess]
  --no-spinner               Disable the progress meter.

Output file options:
  --branching-factor=FACTOR  Number of keys in each *index* block.
                             [default: 1024]
  --approx-block-size=SIZE   Approximate *uncompressed* size of the records in
                             each *data* block, in bytes. [default: 131072]
  --codec=CODEC              Compression algorithm. (Valid options: none,
                             deflate, bz2, lzma.) [default: bz2]
  -z COMPRESS-LEVEL, --compress-level=COMPRESS-LEVEL
                             Degree of compression to use. Interpretation
                             depends on the codec in use:
                               deflate: An integer between 1 and 9.
                                 (Default: 6)
                               bz2: An integer between 1 and 9. (Default: 9)
                               lzma: One of the strings 0, 0e, 1, or 1e.
                                 Note that 0 and 1 are several times faster
                                 than 0e and 1e, though at some cost in
                                 compression ratio. Note also that there is no
                                 benefit to using 1 or 1e unless you also
                                 increase --approx-block-size. (Default: 0e)
  --no-default-metadata      By default, 'zs make' adds an extra "build-info"
                             key to the metadata, recording the time, host,
                             user who created the file, and zs library
                             version. This option disables this behaviour.

    """

    try:
        metadata = json.loads(opts["<metadata>"])
    except ValueError as e:
        optfail("error parsing metadata as JSON: %s" % (e,))

    codec_kwargs = {}
    cl = opts["--compress-level"]
    if cl is not None:
        if opts["--codec"] == "lzma":
            if cl.endswith("e"):
                codec_kwargs["extreme"] = True
                cl = cl[:-1]
            else:
                codec_kwargs["extreme"] = False
        try:
            codec_kwargs["compress_level"] = int(cl)
        except ValueError:
            optfail("--compress-level must be an integer, or "
                    "(for lzma only) an integer followed by the letter e")

    sys.stdout.write("zs: Opening new ZS file: %s\n"
                     % (opts["<new_zs_file>"],))
    with ZSWriter(opts["<new_zs_file>"],
                  metadata=metadata,
                  branching_factor=opts["__branching-factor__"],
                  parallelism=opts["__j__"],
                  codec=opts["--codec"],
                  codec_kwargs=codec_kwargs,
                  show_spinner=not opts["--no-spinner"],
                  include_default_metadata=not opts["--no-default-metadata"],
               ) as out_z:
        sys.stdout.write("zs: Reading input file: %s\n"
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
        sys.stdout.write("zs: Done.\n")

    return 0
