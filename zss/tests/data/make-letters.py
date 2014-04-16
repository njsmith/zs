#!/usr/bin/env python

# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os

from six import BytesIO, int2byte, byte2int
import binascii

from zss import ZSSWriter

letters_records = []
for i in xrange(1, 26, 2):
    letter = int2byte(byte2int(b"a") + i)
    letters_records += [letter, 2 * letter]

def letters_file():
    return BytesIO(b"\n".join(letters_records + [b""]))

def do_write(codec, branching_factor, approx_block_size):
    path = "letters-%s.zss" % codec
    if os.path.exists(path):
        os.unlink(path)
    ZSSWriter(path,
              metadata={u"test-data": u"letters",
                        u"build-user": u"test-user",
                        u"build-host": u"test-host",
                        u"build-time": u"2000-01-01T00:00:00.000000Z",
                        },
              branching_factor=branching_factor,
              approx_block_size=approx_block_size,
              parallelism=2,
              compression=codec,
              ).from_file(letters_file())

# Some variation in branching factor and block size increases the diversity of
# code paths we'll end up taking as we do searches in these files.
do_write("none", 2, 4)
do_write("deflate", 3, 3)
do_write("bz2", 4, 5)
