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
                        b"".join([six.int2byte(i) for i in range(256)]),
                        b"a" * 32768,
                        ]:
        for name, (comp, decomp) in codecs.items():
            print(name)
            assert decomp(comp(test_vector)) == test_vector
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
