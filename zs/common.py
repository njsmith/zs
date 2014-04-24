# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import zlib
import bz2
import struct
import ctypes

import zs._zs

CRC_LENGTH = 8

# Reserve high levels for future extensions.
FIRST_EXTENSION_LEVEL = 64

MAGIC = b"\xab" b"ZSfiLe" b"\x01"
# This is what we stick at the beginning of a file while we constructing it in
# the first place, before it is complete and coherent.
INCOMPLETE_MAGIC = b"\xab" b"ZStoBe" b"\x01"
header_data_length_format = "<Q"
header_data_format = [
    # The offset of the top-level index block.
    ("root_index_offset", "<Q"),
    # The length of the top-level index block.
    ("root_index_length", "<Q"),
    # The total length of this file (necessary for detecting truncation)
    ("total_file_length", "<Q"),
    # sha256(concat(all data blocks)), to let us uniquely identify archives
    ("sha256", "32s"),
    # A null-padded code for the storage algorithm used. So far:
    #   "none"
    #   "deflate"
    #   "bz2"
    #   "lzma"
    ("codec", "NUL-padded-ascii-16"),
    # "<Q" giving length, then arbitrary utf8-encoded json
    ("metadata", "length-prefixed-utf8-json"),
    ]

class ZSError(Exception):
    """Exception class used for most errors encountered in the ZS
    package. (Though we do sometimes raise exceptions of the standard Python
    types like :class:`IOError`, :class:`ValueError`, etc.)

    """
    pass

class ZSCorrupt(ZSError):
    """A subclass of :class:`ZSError`, used specifically for errors that
    indicate a malformed or corrupted ZS file.

    """
    pass

def encoded_crc64xz(data):
    return struct.pack("<Q", zs._zs.crc64xz(data))

# Standardize the name of the compress_level argument:
def deflate_compress(payload, compress_level=6):
    # Weird poorly-documented zlib feature: you can get raw 'deflate' by
    # passing a negative 'wbits' argument. So wbits=15 (the default) produces
    # data with a zlib header and checksum (RFC 1950), while wbits=-15 gives
    # the same data but with no framing ("raw deflate", RFC 1951). We don't
    # need the framing and we have our own checksum, so raw deflate is good
    # for us.
    compressor = zlib.compressobj(compress_level, zlib.DEFLATED, -15)
    zpayload = compressor.compress(payload)
    zpayload += compressor.flush()
    return zpayload

def deflate_decompress(zpayload):
    return zlib.decompress(zpayload, -15)

# Standardize the name of the compress_level argument:
def bz2_compress(payload, compress_level=9):
    # This uses bz2 framing, which is wasteful and means we end up with a
    # double-checksum, but checksumming is more than an order of magnitude
    # faster than bz2 itself, and there's no practical way to get a 'raw' bz2
    # stream, so oh well.
    return bz2.compress(payload, compress_level)

def none_compress(payload):
    return payload

def none_decompress(zpayload):
    return zpayload

have_lzma = True
try:
    import lzma
except ImportError:
    try:
        from backports import lzma
    except ImportError:
        have_lzma = False

if have_lzma:
    def lzma_compress_dsize20(payload, compress_level=0, extreme=True):
        if compress_level > 1:
            raise ValueError("lzma compress level must be 0 or 1")
        if extreme:
            compress_level |= lzma.PRESET_EXTREME
        return lzma.compress(payload,
                             format=lzma.FORMAT_RAW,
                             filters=[{
                                 "id": lzma.FILTER_LZMA2,
                                 "preset": compress_level,
                             }])

    _lzma_decompress_filter_chain = [{
        "id": lzma.FILTER_LZMA2,
        "dict_size": 2 ** 20,
    }]
    def lzma_decompress_dsize20(zpayload):
        # LZMADecompressor is different from the lzma.decompress convenience
        # wrapper in that it won't automatically handle concatenated
        # streams. And that's what we want.
        decobj = lzma.LZMADecompressor(format=lzma.FORMAT_RAW,
                                       filters=_lzma_decompress_filter_chain)
        payload = decobj.decompress(zpayload)
        if not decobj.eof:
            raise ZSCorrupt("LZMA2 stream cut-off in the middle")
        if decobj.unused_data:
            raise ZSCorrupt("trailing garbage after LZMA2 stream")
        return payload
else:
    def no_lzma(*args, **kwargs):
        raise ImportError("please install the backports.lzma package")
    lzma_compress_dsize20 = lzma_decompress_dsize20 = no_lzma

# These callables must be pickleable for multiprocessing.
codecs = {
    "deflate": (deflate_compress, deflate_decompress),
    "bz2": (bz2_compress, bz2.decompress),
    "none": (none_compress, none_decompress),
    "lzma2;dsize=2^20": (lzma_compress_dsize20, lzma_decompress_dsize20),
}

# These are the strings passed to ZSWriter.__init__'s codec= argument, or to
# zs make --codec. In the future if we ever support more lzma dict sizes, then
# we'll keep the 'lzma' shorthand the same, but add some more cleverness to
# the code here to automatically pick the right underlying codec string.
codec_shorthands = {
    "deflate": "deflate",
    "bz2": "bz2",
    "none": "none",
    "lzma": "lzma2;dsize=2^20",
}

def read_n(f, n):
    data = f.read(n)
    if len(data) < n:
        raise ZSCorrupt("unexpectedly encountered end of file")
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
        get_length = zs._zs.read_uleb128
    else:
        raise ValueError("length-prefix mode must be u64le or uleb128")
    while True:
        length = get_length(f)
        if length is None:
            return
        record = f.read(length)
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
                  read_length_prefixed(BytesIO(b"\x00"), "u64le"))
    assert_raises(ValueError, list,
                  read_length_prefixed(
                      BytesIO(b"\x02\x00\x00\x00\x00\x00\x00\x00a"), "u64le"))

    assert (list(read_length_prefixed(
        BytesIO(b"\x00"
                b"\x01a"
                b"\x02bb"
                b"\x80\x01" + (b"c" * 0x80)), "uleb128"))
            == [b"", b"a", b"bb", b"c" * 0x80])
    assert_raises(ValueError, list,
                  read_length_prefixed(BytesIO(b"\x02a"), "uleb128"))

    assert_raises(ValueError, list,
                  read_length_prefixed(BytesIO(), "asdfasdf"))

def write_length_prefixed(f, records, mode):
    if mode == "u64le":
        for record in records:
            f.write(struct.pack("<Q", len(record)))
            f.write(record)
    elif mode == "uleb128":
        f.write(zs._zs.pack_data_records(records))
    else:
        raise ValueError("length prefixed mode must be 'uleb128' or 'u64le'")

def test_write_length_prefixed():
    from six import BytesIO
    records = [b"", b"a", b"aaaaa", b"z" * 500]
    for mode in ["uleb128", "u64le"]:
        f_out = BytesIO()
        write_length_prefixed(f_out, records, mode)
        f_in = BytesIO(f_out.getvalue())
        assert list(read_length_prefixed(f_in, mode)) == records

    from nose.tools import assert_raises
    assert_raises(ValueError, write_length_prefixed,
                  BytesIO(), [], "asdfsadf")
