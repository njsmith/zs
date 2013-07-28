import sys
import struct
import json
from cStringIO import StringIO
from bisect import bisect_left
from zss.common import (ZSSCorrupt,
                        MAGIC,
                        INCOMPLETE_MAGIC,
                        encoded_crc32c,
                        CRC_LENGTH,
                        header_data_length_format,
                        header_data_format,
                        codecs,
                        read_n,
                        read_format)
from zss._zss import (unpack_data_records, unpack_index_records,
                      from_uleb128, MAX_ULEB128_LENGTH)

def _decode_header_data(encoded):
    fields = {}
    f = StringIO(encoded)
    for (field, field_format) in header_data_format:
        if field_format == "length-prefixed-utf8-json":
            length = read_format(f, "<I")
            data = read_n(f, length)
            fields[field] = json.loads(data, encoding="utf-8")
        else:
            fields[field] = read_format(f, field_format)
    return fields

class ZSS(object):
    def __init__(self, path):
        self._path = path
        self._file = open(path, "rb")
        magic = read_n(self._file, len(MAGIC))
        if magic == INCOMPLETE_MAGIC:
            raise ZSSCorrupt("%s: looks like this ZSS file was only "
                             "partially written" % (self._path,))
        if magic != MAGIC:
            raise ZSSCorrupt("%s: bad magic number (are you sure this is "
                             "a ZSS file?)" % (self._path))
        header_data_length = read_format(self._file,
                                          header_data_length_format)
        header_encoded = read_n(self._file, header_data_length)
        header_crc = read_n(self._file, CRC_LENGTH)
        self._data_offset = self._file.tell()
        if encoded_crc32c(header_encoded) != header_crc:
            raise ZSSCorrupt("%s: header checksum mismatch" % (self._path,))
        header = _decode_header_data(header_encoded)

        self._root_index_voffset = header["root_index_voffset"]
        compression = header["compression"].rstrip(b"\x00")
        if compression not in codecs:
            raise ZSSCorrupt("unrecognized compression format %r"
                             % (compression,))
        self._decompress = codecs[compression][-1]
        # For the user to peek at if they want
        self.compression = compression
        self.uuid = header["uuid"]
        self.metadata = header["metadata"]
        if not isinstance(self.metadata, dict):
            raise ZSSCorrupt("bad metadata")

    def _get_block_at(self, voffset):
        # XX in a parallelized reader, this should issue some prefetch
        # Returns (None, None, None) for EOF
        self._file.seek(self._data_offset + voffset)
        encoded_block_level = self._file.read(1)
        if not encoded_block_level:
            return (None, None, None)
        block_level = ord(encoded_block_level)
        block_len_buf = self._file.read(MAX_ULEB128_LENGTH)
        block_len, block_len_len = from_uleb128(block_len_buf)
        first_bytes = block_len_buf[block_len_len:]
        to_read = block_len - len(first_bytes)
        new_voffset = self._file.tell() - self._data_offset + to_read
        if to_read < 0:
            cdata = first_bytes[:block_len]
        else:
            cdata = first_bytes + self._file.read(to_read)
        data = self._decompress(cdata)
        if block_level == 0:
            return (block_level, unpack_data_records(data), new_voffset)
        else:
            return (block_level, unpack_index_records(data), new_voffset)

    def __iter__(self):
        return ZSSIter(self, iter([]), 0, None)

    def iter_from(self, key, stop_record=None):
        voffset = self._root_index_voffset
        while True:
            block_level, values, next_voffset = self._get_block_at(voffset)
            if block_level == 0:
                keys = values
            else:
                keys, voffsets = values
            first_ge_entry = bisect_left(keys, key)
            if block_level == 0:
                # Exit loop
                return ZSSIter(self, iter(keys[first_ge_entry:]),
                               next_voffset, stop_record)
            else:
                if first_ge_entry == 0:
                    # Exit loop
                    return ZSSIter(self, iter([]), 0, stop_record)
                else:
                    voffset = voffsets[first_ge_entry - 1]
                    # Continue looping

    def fsck(self):
        # Read through to find locations of all blocks
        # make sure they're sorted overall
        # then do a depth-first pass through indexes to make sure
        # and that all blocks are reachable, and each index is sorted, and
        # always points to lower level, etc.
        sys.stderr.write("%s: Sequential read & sort check..." % (self._path,))
        all_blocks = set()
        voffset = 0
        last_record_by_level = {}
        def fail(voffset, msg):
            raise ZSSCorrupt("%s at %s: %s" % (self._path, voffset, msg))
        while True:
            block_level, contents, next_voffset = self._get_block_at(voffset)
            if block_level is None:
                break
            elif block_level == 0:
                records = contents
            else:
                records, voffsets = contents
                if not sorted(voffsets) == voffsets:
                    fail(voffset, "unsorted voffsets in index block")
            if not sorted(records) == records:
                fail(voffset, "unsorted records within block")
            if block_level in last_record_by_level:
                if records[0] < last_record_by_level[block_level]:
                    fail(voffset, "unsorted records across blocks")
            last_record_by_level[block_level] = records[-1]
            all_blocks.add(voffset)
            voffset = next_voffset
        sys.stderr.write("done.\n")
        sys.stderr.write("%s: Depth-first scan and reachability check..."
                         % (self._path,))
        referred_to_by = {}
        def _dfs_check(key, expected_level, voffset, source_voffset):
            if voffset in referred_to_by:
                fail(voffset,
                     "referenced twice: %s and %s" % (referred_to_by[voffset],
                                                      source_voffset))
            referred_to_by[voffset] = source_voffset
            if voffset not in all_blocks:
                fail(voffset,
                     "unknown block (referenced by %s)" % (source_voffset,))
            block_level, contents, _ = self._get_block_at(voffset)
            if block_level == 0:
                records = contents
            else:
                records, voffsets = contents
            if expected_level is not None and block_level != expected_level:
                fail(voffset, "expected level %s, got %s" % (expected_level,
                                                             block_level))
            if not (key <= records[0]):
                fail(voffset, "key for this block in %s is > first record"
                     % (source_voffset,))
            if block_level > 0:
                for record, child_voffset in zip(*contents):
                    _dfs_check(record, block_level - 1, child_voffset, voffset)
        _dfs_check("", None, self._root_index_voffset, "root")
        sys.stderr.write("done.\n")
        sys.stderr.write("%s: checking for unreachable blocks..."
                         % (self._path,))
        for voffset in all_blocks.difference(referred_to_by):
            fail(voffset, "unreachable block")
        sys.stderr.write("done\n")
        return "PASS"

class ZSSIter(object):
    def __init__(self, zss, record_iter, next_voffset, stop_record):
        self._zss = zss
        # The current block
        self._record_iter = record_iter
        self._next_voffset = next_voffset
        self._stop_record = stop_record

    def __iter__(self):
        return self

    def next(self):
        for record in self._record_iter:
            if self._stop_record is not None and record >= self._stop_record:
                raise StopIteration
            return record
        # We only reach here if the iterator was empty
        block_level = 0xff
        while block_level not in (0, None):
            block_level, records, self._next_voffset = self._zss._get_block_at(
                self._next_voffset)
        if block_level is None:
            # EOF
            raise StopIteration
        assert block_level == 0
        self._record_iter = iter(records)
        return self.next()
