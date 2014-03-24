# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import pickle

import six
from nose.tools import assert_raises

from ..common import *

def test_encoded_crc32c():
    assert encoded_crc32c(b"foo") == b"\x1d\xae\xc4\xcf"
    assert encoded_crc32c(b"\x00" * 32) == b"\xaa\x36\x91\x8a"

def test_codecs():
    for test_vector in [b"",
                        b"foo",
                        b"".join([chr(i) for i in xrange(256)]),
                        b"a" * 32768,
                        ]:
        for name, (comp, decomp) in codecs.items():
            print(name)
            zdata = comp(test_vector)
            assert decomp(zdata) == test_vector
            # We only check every 3rd byte, because checking every byte was a
            # bit slow.
            for i in range(len(zdata), 3):
                # flip a bit in byte i and make sure that either it has no
                # effect or it creates an error
                corrupted = (zdata[:i]
                             + chr(ord(zdata[i]) ^ 1)
                             + zdata[i + 1:])
                try:
                    got = decomp(corrupted)
                except Exception:
                    # error detected, good!
                    pass
                else:
                    # no error detected -- only okay if the reason no error
                    # was detected was because the bit we flipped doesn't
                    # actually affect anything.
                    assert got == test_vector
            # check pickleability
            assert pickle.loads(pickle.dumps(comp)) is comp
            assert pickle.loads(pickle.dumps(decomp)) is decomp

def test_read_n():
    f = six.BytesIO(b"abcde")
    assert read_n(f, 3) == b"abc"
    assert_raises(ZSSCorrupt, read_n, f, 3)

def test_read_format():
    f = six.BytesIO(b"\x01\x02\x03")
    a, b = read_format(f, "<BH")
    assert a == 1
    assert b == 0x0302
