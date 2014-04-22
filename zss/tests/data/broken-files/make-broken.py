#!/usr/bin/env python

# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import shutil
import hashlib
from six import BytesIO, int2byte, byte2int
import zs
from zs.common import *
from zs.writer import _encode_header
from zs._zs import write_uleb128

def _pack_index_records_unchecked(contents):
    f = BytesIO()
    for key, offset, length in zip(*contents):
        key = key.encode("ascii")
        write_uleb128(len(key), f)
        f.write(key)
        write_uleb128(offset, f)
        write_uleb128(length, f)
    return f.getvalue()

def _pack_data_records_unchecked(contents):
    f = BytesIO()
    for record in contents:
        record = record.encode("ascii")
        write_uleb128(len(record), f)
        f.write(record)
    return f.getvalue()

class SimpleWriter(object):
    def __init__(self, p, metadata={}, codec_name="none", magic=MAGIC,
                 bad_header_checksum=False, header_extra=""):
        self.f = open(p, "w+b")

        self.hasher = hashlib.sha256()
        assert len(magic) == len(MAGIC)
        self._magic = magic
        self._bad_header_checksum = bad_header_checksum
        self.f.write(INCOMPLETE_MAGIC)
        self._header = {
            "root_index_offset": 2 ** 63 - 1,
            "root_index_length": 0,
            "total_file_length": 0,
            "sha256": b"\x00" * 32,
            "codec": codec_name,
            "metadata": metadata,
            }
        self._header_extra = header_extra
        encoded_header = _encode_header(self._header) + self._header_extra
        self._header_length = len(encoded_header)
        self.f.write(struct.pack(header_data_length_format,
                                 len(encoded_header)))
        self._header_offset = self.f.tell()
        self.f.write(encoded_header)
        self.f.write(encoded_crc64xz(encoded_header))
        self._have_root = False

    def raw_block(self, block_level, zpayload,
                  bad_checksum=False, truncate_checksum=0):
        self.f.seek(0, 2)
        offset = self.f.tell()
        contents = int2byte(block_level) + zpayload
        write_uleb128(len(contents), self.f)
        self.f.write(contents)
        checksum = encoded_crc64xz(contents)
        if bad_checksum:
            checksum = b"\x00" * len(checksum)
        if truncate_checksum > 0:
            checksum = checksum[:-truncate_checksum]
        self.f.write(checksum)
        block_length = self.f.tell() - offset
        return offset, block_length

    def append(self, garbage):
        self.f.seek(0, 2)
        self.f.write(garbage)

    def data_block(self, records, **kwargs):
        zpayload = _pack_data_records_unchecked(records)
        self.hasher.update(zpayload)
        return self.raw_block(0, zpayload, **kwargs)

    def index_block(self, block_level, records, offsets, block_lengths,
                    **kwargs):
        zpayload = _pack_index_records_unchecked([records, offsets, block_lengths])
        return self.raw_block(block_level, zpayload, **kwargs)

    def root_block(self, *args, **kwargs):
        root_offset, root_length = self.index_block(*args, **kwargs)
        self.set_root(root_offset, root_length)
        return root_offset, root_length

    def set_root(self, root_offset, root_length):
        self._header["root_index_offset"] = root_offset
        self._header["root_index_length"] = root_length
        self._have_root = True

    def close(self, header_overrides={}):
        assert self._have_root
        self.f.seek(0, 2)
        self._header["total_file_length"] = self.f.tell()
        self._header["sha256"] = self.hasher.digest()
        self._header.update(header_overrides)
        encoded_header = _encode_header(self._header) + self._header_extra
        assert len(encoded_header) == self._header_length
        self.f.seek(self._header_offset)
        self.f.write(encoded_header)
        checksum = encoded_crc64xz(encoded_header)
        if self._bad_header_checksum:
            checksum = b"\x00" * len(checksum)
        self.f.write(checksum)
        self.f.seek(0)
        self.f.write(self._magic)
        self.f.flush()
        self.f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None and not self.f.closed:
            self.close()

    def minimal(self):
        o1, l1 = w.data_block(["a", "b"])
        w.root_block(1, ["a"], [o1], [l1])

with SimpleWriter("bad-data-order.zs") as w:
    offset, length = w.data_block(["z", "a"])
    w.root_block(1, ["z"], [offset], [length])

with SimpleWriter("wrong-root-level-1.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(2, ["a", "c"], [o1, o2], [l1, l2])

with SimpleWriter("wrong-root-level-2.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    io, il = w.index_block(1, ["a", "c"], [o1, o2], [l1, l2])
    w.root_block(3, ["a"], [io], [il])

with SimpleWriter("bad-ref-length.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(1, ["a", "c"], [o1, o2], [l1 + 1, l2])

# index key must be <= first entry in referenced block
with SimpleWriter("bad-index-key-1.zs") as w:
    o1, l1 = w.data_block(["a", "c"])
    w.root_block(1, ["b"], [o1], [l1])

# but this doesn't have to be exact
with SimpleWriter("good-index-key-1.zs") as w:
    o1, l1 = w.data_block(["b", "c"])
    w.root_block(1, ["a"], [o1], [l1])

# index key must be >= last entry in block-before-referenced-block
with SimpleWriter("bad-index-key-2.zs") as w:
    o1, l1 = w.data_block(["a", "c"])
    o2, l2 = w.data_block(["e", "g"])
    w.root_block(1, ["a", "b"], [o1, o2], [l1, l2])

with SimpleWriter("good-index-key-2.zs") as w:
    o1, l1 = w.data_block(["a", "c"])
    o2, l2 = w.data_block(["e", "g"])
    w.root_block(1, ["a", "c"], [o1, o2], [l1, l2])

# for references to index blocks, these invariants must be maintained for the
# underlying *data* blocks, not just the keys in the index blocks themselves
with SimpleWriter("bad-index-key-3.zs") as w:
    o1, l1 = w.data_block(["a", "c"])
    o2, l2 = w.data_block(["e", "g"])
    io1, il1 = w.index_block(1, ["a", "e"], [o1, o2], [l1, l2])
    o3, l3 = w.data_block(["i", "k"])
    o4, l4 = w.data_block(["m", "o"])
    io2, il2 = w.index_block(1, ["i", "m"], [o3, o4], [l3, l4])
    # the index blocks this refers to have keys [a, e], [i, m]
    # so the "f" falls in between them.
    # But it *doesn't* fall before the "g" that's in the 2nd data block that
    # the first index block points to.
    w.root_block(2, ["a", "f"], [io1, io2], [il1, il2])

with SimpleWriter("good-index-key-3.zs") as w:
    o1, l1 = w.data_block(["a", "c"])
    o2, l2 = w.data_block(["e", "g"])
    io1, il1 = w.index_block(1, ["a", "e"], [o1, o2], [l1, l2])
    o3, l3 = w.data_block(["i", "k"])
    o4, l4 = w.data_block(["m", "o"])
    io2, il2 = w.index_block(1, ["i", "m"], [o3, o4], [l3, l4])
    w.root_block(2, ["a", "g"], [io1, io2], [il1, il2])

with SimpleWriter("bad-index-order.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(1, ["c", "a"], [o2, o1], [l2, l1])

with SimpleWriter("wrong-root-length.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    ro, rl = w.index_block(1, ["a", "c"], [o1, o2], [l1, l2])
    w.set_root(ro, rl + 1)
    # And we also add another block at the end so that the wrong length
    # doesn't just immediately result in a short-read
    w.data_block(["w", "x"])

with SimpleWriter("wrong-root-offset.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    ro, rl = w.index_block(1, ["a", "c"], [o1, o2], [l1, l2])
    w.set_root(ro + 1, rl)
    w.data_block(["w", "x"])

# unreferenced trailing index block -- it just references the former root, so
# really it is the root. but the header still points to the old root, so this
# is just unreferenced.
with SimpleWriter("unref-index.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    ro, rl = w.root_block(1, ["a", "c"], [o1, o2], [l1, l2])
    w.index_block(2, ["a"], [ro], [rl])

# Two copies of the index block at the end. In addition to being an
# unreferenced block, this creates a bunch of double-references to previous
# blocks.
with SimpleWriter("repeated-index.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(1, ["a", "c"], [o1, o2], [l1, l2])
    w.index_block(1, ["a", "c"], [o1, o2], [l1, l2])

with SimpleWriter("unref-data.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(1, ["a"], [o1], [l1])

with SimpleWriter("non-dict-metadata.zs", metadata="hi!") as w:
    w.minimal()

with SimpleWriter("root-is-data.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    w.set_root(o1, l1)

with SimpleWriter("bad-codec.zs", codec_name="XXX-bad-codec-XXX") as w:
    w.minimal()

# cut off in the middle of a record
with SimpleWriter("partial-data-1.zs") as w:
    o1, l1 = w.raw_block(0, b"\x01a\x02b")
    w.root_block(1, ["a"], [o1], [l1])

# cut off in the middle of a uleb128
with SimpleWriter("partial-data-2.zs") as w:
    o1, l1 = w.raw_block(0, b"\x01a\x80")
    w.root_block(1, ["a"], [o1], [l1])

# simply empty
with SimpleWriter("empty-data.zs") as w:
    o1, l1 = w.raw_block(0, b"")
    w.root_block(1, [""], [o1], [l1])

with SimpleWriter("partial-index-1.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    assert o1 < 128
    assert l1 < 128
    zpayload = b"\x01a" + int2byte(o1) + int2byte(l1)
    w.set_root(*w.raw_block(1, zpayload[:-1]))
with SimpleWriter("partial-index-2.zs") as w:
    assert w.data_block(["a", "b"]) == (o1, l1)
    w.set_root(*w.raw_block(1, zpayload[:-2]))
with SimpleWriter("partial-index-3.zs") as w:
    assert w.data_block(["a", "b"]) == (o1, l1)
    w.set_root(*w.raw_block(1, zpayload[:-3]))
with SimpleWriter("partial-index-4.zs") as w:
    assert w.data_block(["a", "b"]) == (o1, l1)
    w.set_root(*w.raw_block(1, b"0x80"))
with SimpleWriter("empty-index.zs") as w:
    w.data_block(["a", "b"])
    w.set_root(*w.raw_block(1, b""))

with SimpleWriter("bad-total-length.zs") as w:
    w.minimal()
    w.close({"total_file_length": 10 ** 10})

with SimpleWriter("bad-sha256.zs") as w:
    w.minimal()
    w.close({"sha256": b"\x00" * 32})

with SimpleWriter("bad-magic.zs", magic=b"Q" * 8) as w:
    w.minimal()

with SimpleWriter("incomplete-magic.zs", magic=INCOMPLETE_MAGIC) as w:
    w.minimal()

with SimpleWriter("root-checksum.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    w.root_block(1, ["a"], [o1], [l1], bad_checksum=True)

with SimpleWriter("header-checksum.zs", bad_header_checksum=True) as w:
    w.minimal()

# total_file_length header field is correct, but root_index_length points past
# end of file.
with SimpleWriter("short-root.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    ro, rl = w.index_block(1, ["a"], [o1], [l1])
    w.set_root(ro, rl + 1)

# total_file_length and root_index_length correctly point to a block that has
# been truncated before being written.
with SimpleWriter("truncated-root.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    w.root_block(1, ["a"], [o1], [l1], truncate_checksum=1)

with SimpleWriter("truncated-data-1.zs") as w:
    w.minimal()
    # partial length marker for another block
    w.append(b"\x80")

with SimpleWriter("truncated-data-2.zs") as w:
    w.minimal()
    # partial block contents for another block
    w.append(b"\x08" + b"\x00" * 7)

with SimpleWriter("truncated-data-3.zs") as w:
    w.minimal()
    # partial trailing checksum
    w.append(b"\x08" + b"\x00" * 8 + b"\x01" * 2)

# Index blocks can't have level >= 64
with SimpleWriter("bad-level-root.zs") as w:
    o, l = w.data_block(["a", "b"])
    for i in xrange(1, 65):
        o, l = w.index_block(i, ["a"], [o], [l])
    w.set_root(o, l)

with SimpleWriter("bad-level-index-1.zs") as w:
    o, l = w.index_block(64, ["a"], [0], [0])
    w.root_block(1, ["a"], [o], [l])

with SimpleWriter("bad-level-index-2.zs") as w:
    o, l = w.index_block(64, ["a"], [0], [0])
    w.root_block(2, ["a"], [o], [l])

# Random extension blocks in the middle of the file are ignored though
with SimpleWriter("good-extension-blocks.zs") as w:
    o1, l1 = w.data_block(["a", "b"])
    for i in xrange(64, 256):
        # Not really an index block, but this is the quickest way to write
        # a block with a high level and some arbitrary payload to the file.
        w.index_block(i, ["asdf"], [0], [0])
    o2, l2 = w.data_block(["c", "d"])
    w.root_block(1, ["a", "c"], [o1, o2], [l1, l2])

with SimpleWriter("good-extension-header-fields.zs",
                  header_extra=b"0123456789") as w:
    w.minimal()

# the first block has to start immediately after the header
# (early versions of the reader code didn't catch this because it always went
# through the index)
with SimpleWriter("post-header-junk.zs") as w:
    w.append(b"\x00")
    w.minimal()
