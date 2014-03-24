# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

from __future__ import absolute_import

import six

import zss
from .._zss import *
from nose.tools import assert_raises

def test_crc32c():
    # Some test vectors stolen from
    #   http://rsfcode.isc.org/git/mtbl/plain/src/test-crc32c.c
    for (data, result) in [
            (b"\x61", 0xc1d04330),
            (b"foo", 0xcfc4ae1d),
            (b"hello world", 0xc99465aa),
            (b"\x00" * 32, 0x8a9136aa),
            (b"\xff" * 32, 0x62a8ab43),
            (b"".join([chr(i) for i in xrange(1, 241)]), 0x24c5d375),
            ]:
        print repr(data)
        print hex(crc32c(data))
        assert result == crc32c(data)

def test_buf_write_uleb128():
    cython_test_buf_write_uleb128()

def test_buf_read_uleb128():
    cython_test_buf_read_uleb128()

def test_read_uleb128():
    def t(data, expected_value, expected_len):
        f = six.BytesIO(data)
        value, extra_bytes = read_uleb128(f)
        assert value == expected_value
        assert data[expected_len:] == (extra_bytes + f.read())

    t(b"\x01", 1, 1)
    t(b"\x01" + b"\x02" * 50, 1, 1)
    t(b"\x80\x01\x05", 0x80, 2)
    t(b"", None, 0)

def test_data_records():
    records = [b"", b"\x00" * 16, b"a", b"b"]
    expected = b"\x00\x10" + b"\x00" * 16 + b"\x01a\x01b"
    for alloc_hint in [1, 5, 100]:
        assert pack_data_records(records, alloc_hint) == expected
    assert unpack_data_records(expected) == records
    # Second record extends past end of block
    assert_raises(zss.ZSSCorrupt,
                  unpack_data_records, b"\x03aaa\x04aaa")
    # Second uleb128 extends past end of block
    assert_raises(zss.ZSSCorrupt,
                  unpack_data_records, b"\x03aaa\x80")
    # incorrectly sorted records
    assert_raises(zss.ZSSError,
                  pack_data_records, [b"z", b"a"], 100)
    assert_raises(zss.ZSSError,
                  pack_data_records, [b"a\x00", b"a"], 100)

def test_index_records():
    records = [b"", b"\x00" * 16, b"a", b"b"]
    offsets = [0, 10, 12345, 10 ** 12]
    block_lengths = [2, 3, 4, 2 ** 13]
    expected = (b"\x00\x00\x02"
                + b"\x10" + b"\x00" * 16 + b"\x0a" + b"\x03"
                + b"\x01a\xb9\x60\x04"
                + "\x01b\x80\xa0\x94\xa5\x8d\x1d\x80\x40")
    for alloc_hint in [1, 5, 100]:
        print("asdf")
        assert pack_index_records(records, offsets, block_lengths, alloc_hint
            ) == expected
    assert unpack_index_records(expected) == (records, offsets, block_lengths)
    # Second record extends past end of block
    print("a")
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x01\x04aaa")
    # Second uleb128 record length extends past end of block
    print("b")
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x80")
    # Second uleb128 offset extends past end of block
    print("c")
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x01a\x80")
    # Second uleb128 block_length extends past end of block
    print("d")
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x01a\x01\x80")
    # incorrectly sorted records
    assert_raises(zss.ZSSError,
                  pack_index_records, [b"z", b"a"], [1, 2], [10, 10], 100)
    assert_raises(zss.ZSSError,
                  pack_index_records, [b"a\x00", b"a"], [1, 2], [10, 10], 100)
    # incorrectly sorted offsets
    assert_raises(zss.ZSSError,
                  pack_index_records, [b"a", b"z"], [2, 1], [10, 10], 100)
