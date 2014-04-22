# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import pickle

import six
from nose.tools import assert_raises

from ..common import *

def test_encoded_crc64xz():
    # Tested against liblzma
    #   import ctypes
    #   liblzma = ctypes.CDLL("liblzma.so")
    #   liblzma.lzma_crc64.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint64]
    #   liblzma.lzma_crc64.restype = ctypes.c_uint64
    #   liblzma.lzma_crc64(data, len(data), 0)
    assert encoded_crc64xz(b"foo") == b"\x58\x15\xa9\x7b\x2c\xe6\x2c\x98"
    assert encoded_crc64xz(b"\x00" * 32) == b"\x0c\x33\xd5\x7c\x61\xf8\x5a\xc9"

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
    assert_raises(ZSCorrupt, read_n, f, 3)

def test_read_format():
    f = six.BytesIO(b"\x01\x02\x03")
    a, b = read_format(f, "<BH")
    assert a == 1
    assert b == 0x0302
