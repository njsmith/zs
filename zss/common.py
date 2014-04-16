# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# Compressed Sorted Sets
# A static database for range queries on compressible data.

# Data model: we have a set of variable-length opaque bitstrings. We want to
# store them compressed, but be able to efficiently perform range queries,
# i.e., read out all bitstrings which are in the range [low, high).

# Storage model:
#
# Data is stored in a large, flat, file, indexed by byte offsets.
#
# The file has the format:
#
# FILE := magic HEADER BLOCK+
# HEADER := header_data_length header_data header_data_crc64xz
# header_data_length, header_data_crc64xz := little-endian uint64
# header_data := (see below)
# BLOCK := contents_length BLOCK_CONTENTS checksum
# contents_length := uleb128
# BLOCK_CONTENTS := block_level compress(BLOCK_DATA)
# level := uint8
# if level = 0x00:
#   BLOCK_DATA := (record_length data_record)*
# if level > 0x00:
#   BLOCK_DATA := (record_length index_record block_offset block_length)*
# record_length := uleb128
# block_offset := uleb128
# block_length := uleb128
# data_record, index_record := arbitrary 8-bit data
#
# Invariants:
# a) Given two data_records, record1 and record2, at offset n1 and n2,
#    n1 <= n2 numerically implies that record1 <= record2 in memcmp() order
#    (with end-of-string sorting before all other values).
# b) For every DATA_BLOCK Bi there is a corresponding index_record Ri.
#      Ri <= the first data_record in Bi
#    and, for all data_records Dj that occur earlier in the file than Bi,
#      Dj <= Ri.

import zlib
import bz2
import struct
import ctypes

import zss._zss

CRC_LENGTH = 8

# Reserve high levels for future extensions
MAX_LEVEL = 63

# "ZSS", three bytes from urandom, and 2 bytes to serve as a version
# identifier in case that turns out to be useful.
MAGIC = "ZSS\x1c\x8e\x6c\x00\x01"
# This is what we stick at the beginning of a file while we constructing it in
# the first place, before it is complete and coherent.
INCOMPLETE_MAGIC = "SSZ\x1c\x8e\x6c\x00\x01"
header_data_length_format = "<Q"
header_data_format = [
    # The offset of the top-level index block.
    ("root_index_offset", "<Q"),
    # The length of the top-level index block.
    ("root_index_length", "<Q"),
    # The total length of this file (necessary for detecting truncation)
    ("file_total_length", "<Q"),
    # sha256(concat(all data blocks)), to let us uniquely identify archives
    ("sha256", "32s"),
    # A null-padded code for the storage algorithm used. So far:
    #   "none"
    #   "deflate"
    #   "bzip2"
    ("compression", "16s"),
    # "<Q" giving length, then arbitrary utf8-encoded json
    ("metadata", "length-prefixed-utf8-json"),
    ]

class ZSSError(Exception):
    pass

class ZSSCorrupt(ZSSError):
    pass

def encoded_crc64xz(data):
    return struct.pack("<Q", zss._zss.crc64xz(data))

# Standardize the name of the compress_level argument:
def deflate_compress(data, compress_level=6):
    # Weird poorly-documented zlib feature: you can get raw 'deflate' by
    # passing a negative 'wbits' argument. So wbits=15 (the default) produces
    # data with a zlib header and checksum (RFC 1950), while wbits=-15 gives
    # the same data but with no framing ("raw deflate", RFC 1951). We don't
    # need the framing and we have our own checksum, so raw deflate is good
    # for us.
    compressor = zlib.compressobj(compress_level, zlib.DEFLATED, -15)
    zdata = compressor.compress(data)
    zdata += compressor.flush()
    return zdata

def deflate_decompress(zdata):
    return zlib.decompress(zdata, -15)

# Standardize the name of the compress_level argument:
def bz2_compress(data, compress_level=9):
    # This uses bz2 framing, which is wasteful and means we end up with a
    # double-checksum, but checksumming is more than an order of magnitude
    # faster than bz2 itself, and there's no practical way to get a 'raw' bz2
    # stream, so oh well.
    return bz2.compress(data, compress_level)

def none_compress(data):
    return data

def none_decompress(zdata):
    return zdata

# These callables must be pickleable for multiprocessing.
codecs = {
    "deflate": (deflate_compress, deflate_decompress),
    "bz2": (bz2_compress, bz2.decompress),
    "none": (none_compress, none_decompress),
    }

def read_n(f, n):
    data = f.read(n)
    if len(data) < n:
        raise ZSSCorrupt("unexpectedly encountered end of file")
    return data

def read_format(f, struct_format):
    length = struct.calcsize(struct_format)
    data = read_n(f, length)
    return struct.unpack(struct_format, data)

def read_u64le(f):
    """Read a u64le from file-like object 'f'.

    Returns value, or else None if 'f' is at EOF."""
    encoded_value = f.read(8)
    if not encoded_value:
        return None
    elif len(encoded_value) != 8:
        raise ValueError("file ended in middle of a u64le")
    else:
        return struct.unpack("<Q", encoded_value)[0]

def read_length_prefixed(f, mode):
    if mode == "u64le":
        get_length = read_u64le
    elif mode == "uleb128":
        get_length = zss._zss.read_uleb128
    else:
        raise ValueError("length-prefix mode must be u64le or uleb128")
    while True:
        length = get_length(f)
        print length
        if length is None:
            return
        record = f.read(length)
        print record
        if len(record) != length:
            raise ValueError("%s length-prefixed file ended mid-record"
                             % (mode,))
        yield record

def test_read_length_prefixed():
    from six import BytesIO
    f = BytesIO(b"\x00\x00\x00\x00\x00\x00\x00\x00"
                b"\x01\x00\x00\x00\x00\x00\x00\x00a"
                b"\x02\x00\x00\x00\x00\x00\x00\x00bb")
    got = list(read_length_prefixed(f, "u64le"))
    assert got == [b"", b"a", b"bb"]
    from nose.tools import assert_raises
    assert_raises(ValueError, list,
                  read_length_prefixed(BytesIO("b\x00"), "u64le"))
    assert_raises(ValueError, list,
                  read_length_prefixed(
                      BytesIO("b\x02\x00\x00\x00\x00\x00\x00\x00a"), "u64le"))

    assert (list(read_length_prefixed(
        BytesIO(b"\x00"
                b"\x01a"
                b"\x02bb"
                b"\x80\x01" + (b"c" * 0x80)), "uleb128"))
            == [b"", b"a", b"bb", b"c" * 0x80])
    assert_raises(ValueError, list,
                  read_length_prefixed(BytesIO("b\x02a"), "uleb128"))
