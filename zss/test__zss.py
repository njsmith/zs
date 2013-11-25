from __future__ import absolute_import

import zss
from ._zss import *
from nose.tools import assert_raises

def test_crc32c():
    # Some test vectors stolen from
    #   http://rsfcode.isc.org/git/mtbl/plain/src/test-crc32c.c
    for (data, result) in [
            ("\x61", 0xc1d04330),
            ("foo", 0xcfc4ae1d),
            ("hello world", 0xc99465aa),
            ("\x00" * 32, 0x8a9136aa),
            ("\xff" * 32, 0x62a8ab43),
            ("".join([chr(i) for i in xrange(1, 241)]), 0x24c5d375),
            ]:
        print repr(data)
        print hex(crc32c(data))
        assert result == crc32c(data)

def test_to_uleb128():
    assert to_uleb128(0) == b"\x00"
    assert to_uleb128(0x10) == b"\x10"
    assert to_uleb128(0x81) == b"\x81\x01"
    assert to_uleb128(0x7f) == b"\x7f"
    assert to_uleb128(0x107f) == b"\xff\x20"
    assert to_uleb128(1 << 43) == b"\x80\x80\x80\x80\x80\x80\x02"

def test_read_uleb128():
    cython_test_read_uleb128()

def test_from_uleb128():
    assert from_uleb128("\x10xxx") == (0x10, 1)
    assert from_uleb128("\x80\x10xxx") == (1 << 11, 2)
    assert_raises(zss.ZSSCorrupt, from_uleb128, "\x80")

def test_data_records():
    records = ["", "\x00" * 16, "a", "b"]
    expected = "\x00\x10" + "\x00" * 16 + "\x01a\x01b"
    for alloc_hint in [1, 5, 100]:
        assert pack_data_records(records, alloc_hint) == expected
    assert unpack_data_records(expected) == records
    # Second record extends past end of block
    assert_raises(zss.ZSSCorrupt,
                  unpack_data_records, "\x03aaa\x04aaa")
    # Second uleb128 extends past end of block
    assert_raises(zss.ZSSCorrupt,
                  unpack_data_records, "\x03aaa\x80")
    # incorrectly sorted records
    assert_raises(zss.ZSSError,
                  pack_data_records, ["z", "a"], 100)
    assert_raises(zss.ZSSError,
                  pack_data_records, ["a\x00", "a"], 100)

def test_index_records():
    records = ["", "\x00" * 16, "a", "b"]
    voffsets = [0, 10, 12345, 10 ** 12]
    expected = ("\x00\x00"
                + "\x10" + "\x00" * 16 + "\x0a"
                + "\x01a\xb9\x60"
                + "\x01b\x80\xa0\x94\xa5\x8d\x1d")
    for alloc_hint in [1, 5, 100]:
        print "asdf"
        assert pack_index_records(records, voffsets, alloc_hint) == expected
    assert unpack_index_records(expected) == (records, voffsets)
    # Second record extends past end of block
    print "a"
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, "\x03aaa\x00\x04aaa")
    # Second uleb128 record length extends past end of block
    print "b"
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, "\x03aaa\x00\x80")
    # Second uleb128 voffset extends past end of block
    print "c"
    assert_raises(zss.ZSSCorrupt,
                  unpack_index_records, "\x03aaa\x00\x01a\x80")
    # incorrectly sorted records
    assert_raises(zss.ZSSError,
                  pack_index_records, ["z", "a"], [1, 2], 100)
    assert_raises(zss.ZSSError,
                  pack_index_records, ["a\x00", "a"], [1, 2], 100)
    # incorrectly sorted voffsets
    assert_raises(zss.ZSSError,
                  pack_index_records, ["a", "z"], [2, 1], 100)
