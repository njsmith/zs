# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# A minimalist ZS decompressor. Do not use this, it has no fancy features and
# no integrity or error checking. But it does serve as executable
# documentation of how to get your data out in case of an emergency.

# Requires Python 3.3+.

# Usage: python3 unzs.py input-file.zs output-file.txt
import sys, struct, zlib, lzma, io

def read_uleb128(f):
    value = shift = 0
    while True:
        byte_str = f.read(1)
        if not byte_str: # Check for end-of-file
            return None
        value |= (byte_str[0] & 0x7f) << shift
        shift += 7
        if not (byte_str[0] & 0x80):
            return value

def none_decompress(zpayload):
    return zpayload

def deflate_decompress(zpayload):
    return zlib.decompress(zpayload, -15) # -15 indicates raw deflate format

def lzma_decompress(zpayload):
    filters = [{"id": lzma.FILTER_LZMA2, "dict_size": 2 ** 20}]
    return lzma.decompress(zpayload, format=lzma.FORMAT_RAW, filters=filters)

decompressors = {b"none": none_decompress,
                 b"deflate": deflate_decompress,
                 b"lzma2;dsize=2^20": lzma_decompress}

in_f = open(sys.argv[1], "rb")
out_f = open(sys.argv[2], "wb")
in_f.seek(8)
header_length = struct.unpack("<Q", in_f.read(8))[0]
in_f.seek(72)
codec = in_f.read(16).rstrip(b"\x00")
decompress = decompressors[codec]
in_f.seek(8 + 8 + header_length + 8)
while True:
    block_len = read_uleb128(in_f)
    if block_len is None:
        break
    block = in_f.read(block_len)
    in_f.read(8) # skip CRC
    if block[0] == 0: # data block
        payload = io.BytesIO(decompress(block[1:]))
        while True:
            record_len = read_uleb128(payload)
            if record_len is None:
                break
            out_f.write(payload.read(record_len) + b"\n")
