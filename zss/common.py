# Compressed Sorted Sets
# A static database for range queries on compressible data.

# TODO:
# - use relative offsets to allow for cheap subsetting
# - maybe add a new block type (new rule: blocks with high bit set, >= 128,
#   are special) to indicate a span that is missing?
# - replace each voffset with a block reference that has both relative offset
#   *and* length of the block that is referred to, to halve round-trips when
#   accessing a zss file over a dumb transport like HTTP or NFS
# - then write a tool that given a URL fetches a subset
#
# ...But does anyone actually care about getting all and only the "a" ngrams?
# In actual use, being able to fetch a span over a dumb transport is very
# useful, but then what?  I guess the use is that if we can easily represent
# the result of such a query as a ZSS file then it saves some hassle -- e.g.,
# it means you aren't forced to immediately decompress what you fetch? ...but
# again, who cares?

# Data model: we have a set of variable-length opaque bitstrings. We want to
# store them compressed, but be able to efficiently perform range queries,
# i.e., read out all bitstrings which are in the range [low, high).

# Storage model:
#
# Data is stored in a large, flat, file, indexed by virtual offsets
# (voffsets). voffsets are not file offsets. They start at 0 at the beginning
# of the data, and count uniformly from there, ignoring the header and its
# checksum. This is a carryover from a previous version of the file format
# that supported multi-file archives; it may be useful again in the future if
# we re-add that support.
#
# The file has the format:
#
# FILE := magic HEADER BLOCK+
# HEADER := header_data_length header_data header_data_crc32c
# header_data_length, header_data_crc32c := little-endian uint32
# header_data := (see below)
# BLOCK := level block_length compress(BLOCK_DATA)
# level := uint8
# block_length := uleb128
# if level = 0x00:
#   BLOCK_DATA := (record_length data_record)*
# if level > 0x00:
#   BLOCK_DATA := (record_length index_record block_voffset)*
# record_length := uleb128
# block_voffset := uleb128
# data_record, index_record := arbitrary 8-bit data
#
# Invariants:
# a) Given two data_records, record1 and record2, at voffset n1 and n2,
#    n1 <= n2 numerically implies that record1 <= record2 in memcmp() order
#    (with end-of-string sorting before all other values).
# b) For every DATA_BLOCK Bi there is a corresponding index_record Ri.
#      Ri <= the first data_record in Bi
#    and, for all data_records Dj that occur earlier in the file than Bi,
#      Dj <= Ri.

import zlib
import bz2
import struct
import zss._zss

CRC_LENGTH = 4

MAX_LEVEL = 255

# "ZSS", three bytes from urandom, and 2 bytes to serve as a version
# identifier in case that turns out to be useful.
MAGIC = "ZSS\x1c\x8e\x6c\x00\x01"
# This is what we stick at the beginning of a file while we constructing it in
# the first place, before it is complete and coherent.
INCOMPLETE_MAGIC = "SSZ\x1c\x8e\x6c\x00\x01"
header_data_length_format = "<I"
header_data_format = [
    # The voffset of the top-level index block.
    ("root_index_voffset", "<Q"),
    # A unique identifier for this archive.
    ("uuid", "16s"),
    # A null-padded code for the storage algorithm used. So far:
    #   "zlib"
    #   "bzip2"
    #   "none+crc32c"
    ("compression", "16s"),
    # "<I" giving length, then arbitrary utf8-encoded json
    ("metadata", "length-prefixed-utf8-json"),
    ]
header_offset = (len(MAGIC)
                 + struct.calcsize(header_data_length_format))

class ZSSError(Exception):
    pass

class ZSSCorrupt(ZSSError):
    pass

def encoded_crc32c(data):
    return struct.pack("<I", zss._zss.crc32c(data))

# Standardize the name of the compress_level argument:
def zlib_compress(data, compress_level=6):
    return zlib.compress(data, compress_level)

# Standardize the name of the compress_level argument:
def bz2_compress(data, compress_level=9):
    return bz2.compress(data, compress_level)

def none_crc32c_compress(data):
    return data + encoded_crc32c(data)

def none_crc32c_decompress(compressed_data):
    data = compressed_data[:-4]
    checksum = compressed_data[-4:]
    if checksum != encoded_crc32c(data):
        raise ZSSCorrupt("checksum mismatch")
    return data

codecs = {
    "zlib": (zlib_compress, zlib.decompress),
    "bz2": (bz2_compress, bz2.decompress),
    "none+crc32c": (none_crc32c_compress, none_crc32c_decompress),
    }

def read_n(f, n):
    data = f.read(n)
    if len(data) < n:
        raise ZSSCorrupt("unexpectedly encountered end of file")
    return data

def read_format(f, struct_format):
    length = struct.calcsize(struct_format)
    data = read_n(f, length)
    return struct.unpack(struct_format, data)[0]
