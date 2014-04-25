# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

from __future__ import absolute_import

import six

import zs
from .._zs import *
from nose.tools import assert_raises

def test_crc64xz():
    for (data, result) in [
            (b"123456789", 0x995dc9bbdf1939fa),
            ]:
        print(repr(data))
        print(hex(crc64xz(data)))
        assert result == crc64xz(data)

def test_buf_write_uleb128():
    cython_test_buf_write_uleb128()

def test_write_uleb128():
    def t(value, expected_data):
        f = six.BytesIO()
        write_uleb128(value, f)
        assert f.getvalue() == expected_data
    t(1, b"\x01")
    t(0x81, b"\x81\x01")
    t(1 << 43, b"\x80\x80\x80\x80\x80\x80\x02")

def test_buf_read_uleb128():
    cython_test_buf_read_uleb128()

def test_read_uleb128():
    def t(data, expected_value, expected_len):
        f = six.BytesIO(data)
        value = read_uleb128(f)
        assert value == expected_value
        assert data[expected_len:] == f.read()

    t(b"\x01", 1, 1)
    t(b"\x01" + b"\x02" * 50, 1, 1)
    t(b"\x80\x01\x05", 0x80, 2)
    t(b"", None, 0)

def test_data_records():
    records = [b"", b"\x00" * 16, b"a", b"b"]
    expected = b"\x00\x10" + b"\x00" * 16 + b"\x01a\x01b"
    for alloc_hint in [0, 1, 5, 100]:
        assert pack_data_records(records, alloc_hint) == expected
    assert pack_data_records(records) == expected
    assert unpack_data_records(expected) == records
    # Second record extends past end of block
    assert_raises(zs.ZSCorrupt,
                  unpack_data_records, b"\x03aaa\x04aaa")
    # Second uleb128 extends past end of block
    assert_raises(zs.ZSCorrupt,
                  unpack_data_records, b"\x03aaa\x80")
    # incorrectly sorted records
    assert_raises(zs.ZSError,
                  pack_data_records, [b"z", b"a"], 100)
    assert_raises(zs.ZSError,
                  pack_data_records, [b"a\x00", b"a"], 100)

def test_index_records():
    records = [b"", b"\x00" * 16, b"a", b"b"]
    offsets = [0, 10, 12345, 10 ** 12]
    block_lengths = [2, 3, 4, 2 ** 13]
    expected = (b"\x00\x00\x02"
                + b"\x10" + b"\x00" * 16 + b"\x0a" + b"\x03"
                + b"\x01a\xb9\x60\x04"
                + b"\x01b\x80\xa0\x94\xa5\x8d\x1d\x80\x40")
    for alloc_hint in [0, 1, 5, 100]:
        print("asdf")
        assert pack_index_records(records, offsets, block_lengths, alloc_hint
            ) == expected
    assert pack_index_records(records, offsets, block_lengths) == expected
    assert unpack_index_records(expected) == (records, offsets, block_lengths)
    # Second record extends past end of block
    print("a")
    assert_raises(zs.ZSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x01\x04aaa")
    # Second uleb128 record length extends past end of block
    print("b")
    assert_raises(zs.ZSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x80")
    # Second uleb128 offset extends past end of block
    print("c")
    assert_raises(zs.ZSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x01a\x80")
    # Second uleb128 block_length extends past end of block
    print("d")
    assert_raises(zs.ZSCorrupt,
                  unpack_index_records, b"\x03aaa\x00\x00\x01a\x01\x80")
    # incorrectly sorted records
    assert_raises(zs.ZSError,
                  pack_index_records, [b"z", b"a"], [1, 2], [10, 10], 100)
    assert_raises(zs.ZSError,
                  pack_index_records, [b"a\x00", b"a"], [1, 2], [10, 10], 100)
    # incorrectly sorted offsets
    assert_raises(zs.ZSError,
                  pack_index_records, [b"a", b"z"], [2, 1], [10, 10], 100)
