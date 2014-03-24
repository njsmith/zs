# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import struct
import json
from bisect import bisect_left
from contextlib import closing, contextmanager
from collections import namedtuple, deque
import multiprocessing

from six import BytesIO, byte2int, int2byte

from .futures import SerialExecutor, ProcessPoolExecutor
from .common import (ZSSError,
                     ZSSCorrupt,
                     MAGIC,
                     INCOMPLETE_MAGIC,
                     encoded_crc32c,
                     CRC_LENGTH,
                     header_data_length_format,
                     header_data_format,
                     codecs,
                     read_n,
                     read_format)
from ._zss import unpack_data_records, unpack_index_records, read_uleb128
from .lru import LRU
from .transport import FileTransport, HTTPTransport

# How much data to read from the header on our first request on slow
# transports. If the header is shorter than this, then we waste a bit of
# bandwidth reading too much. OTOH if the header is longer than this, then we
# have to make a second request, incurring the relevant latency. So we want a
# number that's bigger than most headers, but not *too* big. (Also, it *must*
# be larger than the magic + header length field, which is currently 16.)
HEADER_SIZE_GUESS = 8192

# for testing
@contextmanager
def _lower_header_size_guess():
    global HEADER_SIZE_GUESS
    was = HEADER_SIZE_GUESS
    HEADER_SIZE_GUESS = 16
    yield
    HEADER_SIZE_GUESS = was

def _decode_header_data(encoded):
    fields = {}
    f = BytesIO(encoded)
    for (field, field_format) in header_data_format:
        if field_format == "length-prefixed-utf8-json":
            length, = read_format(f, "<I")
            data = read_n(f, length)
            fields[field] = json.loads(data, encoding="utf-8")
        else:
            fields[field], = read_format(f, field_format)
    return fields

def _get_raw_block_unchecked(stream):
    # Returns (None, None) for EOF
    length = read_uleb128(stream)
    if length is None:
        # EOF
        return (None, None)
    block_contents = stream.read(length)
    checksum = stream.read(CRC_LENGTH)
    if len(block_contents) != length or len(checksum) != CRC_LENGTH:
        raise ZSSCorrupt("unexpected EOF")
    return block_contents, checksum

def test__get_raw_block_unchecked():
    f = BytesIO(b"\x05" + b"\x01" * 5 + b"\x02" * 4)
    assert _get_raw_block_unchecked(f) == (b"\x01" * 5, b"\x02" * 4)
    from nose.tools import assert_raises
    # It's almost impossible to create a ZSS file that actually shows this
    # error without hitting another error first. (It has to be
    # in a block that is *not* read during the initial index lookup, but *is*
    # at the end of the file -- but usually the root index block is at the end
    # of the file.) So, we just test it directly:
    f_partial1 = BytesIO(b"\x05" + b"\x01" * 4)
    assert_raises(ZSSCorrupt, _get_raw_block_unchecked, f_partial1)
    f_partial2 = BytesIO(b"\x05" + b"\x01" * 5 + "\x02" * 3)
    assert_raises(ZSSCorrupt, _get_raw_block_unchecked, f_partial2)

def _check_block(offset, raw_block, checksum):
    if encoded_crc32c(raw_block) != checksum:
        raise ZSSCorrupt("checksum mismatch at %s" % (offset,))
    block_level = byte2int(raw_block[0])
    zdata = raw_block[1:]
    return (block_level, zdata)

class _ZSSMapStop(Exception):
    pass

class _ZSSMapSkip(object):
    pass

def _map_raw_helper(offset, block_length, raw_block, checksum,
                    skip_index, stop, fn, args, kwargs):
    block_level, zdata = _check_block(offset, raw_block, checksum)
    if skip_index and block_level > 0:
        return _ZSSMapSkip
    return fn(offset, block_length, block_level, zdata, stop, *args, **kwargs)

def _decompress_helper(offset, block_length,
                       block_level, zdata, stop, decompress_fn):
    # stopping has to be left to the next level up
    return decompress_fn(zdata)

def _map_helper(offset, block_length, block_level, zdata, stop, decompress_fn,
                user_fn, user_args, user_kwargs):
    data = decompress_fn(zdata)
    records = unpack_data_records(data)
    if stop is not None and records[0] >= stop:
        raise _ZSSMapStop()
    return user_fn(records, *user_args, **user_kwargs)

def _dump_helper(records, start, stop, terminator):
    if records[0] < start:
        records = records[bisect_left(records, start):]
    if stop is not None and records and records[-1] >= stop:
        records = records[:bisect_left(records, stop)]
    records.append(b"")
    return terminator.join(records)

def _fsck_helper(offset, block_length, block_level, zdata, stop,
                 decompress_fn):
    return (offset, block_length, block_level, decompress_fn(zdata))

class ZSS(object):
    def __init__(self, path=None, url=None,
                 parallelism="auto", index_block_cache=32):
        if path is not None and url is None:
            self._transport = FileTransport(path)
        elif path is None and url is not None:
            self._transport = HTTPTransport(url)
        else:
            raise ValueError("exactly one of path= or url= must be given")

        header = self._get_header()

        self._root_index_offset = header["root_index_offset"]
        self._root_index_length = header["root_index_length"]
        compression = header["compression"].rstrip(b"\x00")
        if compression not in codecs:
            raise ZSSCorrupt("unrecognized compression format %r"
                             % (compression,))
        self._decompress = codecs[compression][-1]
        # Some public attributes for the user to peek at if they want:
        self.compression = compression
        self.uuid = header["uuid"]
        self.metadata = header["metadata"]
        if not isinstance(self.metadata, dict):
            raise ZSSCorrupt("bad metadata")

        if parallelism == "auto":
            # XX put an upper bound on this
            parallelism = multiprocessing.cpu_count()
        self._parallelism = parallelism
        if self._parallelism < 0:
            raise ValueError("parallelism must be >= 0 or \"auto\"")
        if parallelism == 0:
            self._executor = SerialExecutor()
        else:
            self._executor = ProcessPoolExecutor(parallelism)

        self._get_index_block = LRU(self._get_index_block_impl,
                                    index_block_cache)

        self._closed = False

    def _check_closed(self):
        if self._closed:
            raise ZSSError("attemped operation on closed ZSS file")

    def _get_header(self):
        chunk = self._transport.chunk_read(0, HEADER_SIZE_GUESS)
        stream = BytesIO(chunk)

        magic = read_n(stream, len(MAGIC))
        if magic == INCOMPLETE_MAGIC:
            raise ZSSCorrupt("%s: looks like this ZSS file was only "
                             "partially written" % (self._transport.name,))
        if magic != MAGIC:
            raise ZSSCorrupt("%s: bad magic number (are you sure this is "
                             "a ZSS file?)" % (self._transport.name))
        header_data_length, = read_format(stream, header_data_length_format)

        needed = header_data_length + CRC_LENGTH
        remaining = len(chunk) - stream.tell()
        if remaining < needed:
            rest = self._transport.chunk_read(len(chunk), needed - remaining)
            stream = BytesIO(stream.read() + rest)

        header_encoded = read_n(stream, header_data_length)
        header_crc = read_n(stream, CRC_LENGTH)
        if encoded_crc32c(header_encoded) != header_crc:
            raise ZSSCorrupt("%s: header checksum mismatch"
                             % (self._transport.name,))

        return _decode_header_data(header_encoded)

    def _get_index_block_impl(self, offset, block_length):
        chunk = self._transport.chunk_read(offset, block_length)
        if len(chunk) != block_length:
            raise ZSSCorrupt("partial read on index block @ %s, length %s"
                             % (offset, block_length))
        raw_block, checksum = _get_raw_block_unchecked(BytesIO(chunk))
        block_level, zdata = _check_block(offset, raw_block, checksum)
        assert block_level > 0
        data = self._decompress(zdata)
        return (block_level, unpack_index_records(data))

    # Returns offset of either the first or second data (level-0) block which
    # contains entries that are >= the needle.
    #
    # Suppose these are our blocks:
    #    [b c d e] [f g h i j]
    # and our needle is "d". We can't return a pointer to "d" directly, we
    # have to return a pointer to one of the blocks, i.e., to either "a" or
    # "f". round_down=True means we return a pointer to the "a" block,
    # round_down=False means we return a pointer to the "f" block.
    #
    # There are two corner cases. If we get asked to look for "a", then we
    # always return the "b" block. If we get asked to look for "m", then if
    # round_down=True we return the "f" block, and if round_down=False we
    # return None.
    def _find_ge_block(self, needle, round_down):
        offset = self._root_index_offset
        block_length = self._root_index_length
        while True:
            block_level, values = self._get_index_block(offset, block_length)
            assert block_level > 0
            keys, offsets, block_lengths = values
            # This gives us the index of the first block whose *first* entry
            # is >= the needle.
            idx = bisect_left(keys, needle)
            # The first block which can potentially contain *any* entries >=
            # the needle is the block *before* the one we found -- if there is
            # such a block.
            if round_down and idx != 0:
                idx -= 1
            if idx >= len(offsets):
                # there are no blocks whose first entry is >= needle. (This
                # can only happen if round_down=False.)
                return None
            offset = offsets[idx]
            block_length = block_lengths[idx]
            # Our (offset, block_length) now point to a block whose block
            # level is (block_level - 1).
            if block_level - 1 == 0:
                return offset

    def _norm_search_args(self, start, stop, prefix):
        # we intersect start/stop/prefix together
        if start is None:
            start = b""
        if prefix is None:
            prefix = b""
        # unfortunately, there is no concrete value we can put for "stop" that
        # is equivalent to leaving it unspecified, so we have to do some
        # if-then-wrangling everywhere 'stop' is used.
        start = max(prefix, start)
        if prefix:
            prefix_stop = prefix[:-1] + int2byte(byte2int(prefix[-1]) + 1)
        else:
            prefix_stop = None
        if stop is None:
            stop = prefix_stop
        elif prefix_stop is not None:
            stop = min(stop, prefix_stop)
        # now start is always a string, and stop is either a string or None,
        # and we can ignore 'prefix', because its effect has been incorporated
        # into start/stop.
        return start, stop

    def _sloppy_stream(self, start, stop):
        start_offset = self._find_ge_block(start, True)
        stop_offset = None
        if self._transport.remote and stop is not None:
            # This can return None. Fortunately stream_read can accept None as
            # a stop offset.
            stop_offset = self._find_ge_block(stop, False)
        return self._transport.stream_read(start_offset, stop_offset)

    # This is a low-level function with somewhat fiddly semantics. (But all
    # the more user-friendly functions are implemented in terms of it.)
    #
    # It finds all blocks which may contain records that are >= start, and
    # then for each such block it calls
    #
    #   fn(offset, block_length, block_level, zdata, stop, *args, **kwargs)
    #
    # Key points:
    # - If skip_index is true, then it skips over index blocks (i.e.,
    #   block_level will always be 0).
    # - These calls are performed in parallel on however many workers were
    #   configured when this ZSS object was created; therefore, fn, args, and
    #   kwargs must all be pickleable (unless you use parallelism=0).
    # - The iteration may or may not stop when the 'stop' key is reached. If
    #   you want it to stop for sure at any point, you must either (a) raise a
    #   _ZSSMapStop from your callback function, or (b) call .close() on this
    #   generator.
    def _map_raw_block(self, start, stop, skip_index, fn, *args, **kwargs):
        with closing(self._sloppy_stream(start, stop)) as stream:
            q = deque()
            eof = False
            try:
                while q or not eof:
                    value = _ZSSMapSkip
                    # If we have n workers, then when we yield we want to
                    # leave at least n jobs in the queue, so the workers all
                    # have something to do while our caller is getting on with
                    # things in the main process. Therefore we have to push at
                    # least n + 1 jobs into the queue, so we can yield one and
                    # have the appropriate number left over. (Notice that this
                    # reasoning still works for n == 0.)
                    while not eof and len(q) < self._parallelism + 1:
                        offset = stream.tell()
                        (raw_block, checksum) = _get_raw_block_unchecked(stream)
                        block_length = stream.tell() - offset
                        if raw_block is None:
                            eof = True
                            continue
                        f = self._executor.submit(_map_raw_helper,
                                                  offset, block_length,
                                                  raw_block, checksum,
                                                  skip_index, stop,
                                                  fn, args, kwargs)
                        q.append(f)
                    if q:
                        value = q.popleft().result()
                        if value is not _ZSSMapSkip:
                            yield value
            except _ZSSMapStop:
                # Some job requested early termination of the loop.
                pass
            finally:
                while q:
                    q.pop().cancel()

    def sloppy_block_search(self, start=None, stop=None, prefix=None):
        # This does decompression in the worker, and unpacking in the main
        # process (because no point in unpacking, then pickling, then
        # unpickling)
        self._check_closed()
        start, stop = self._norm_search_args(start, stop, prefix)
        mrb = self._map_raw_block
        with closing(mrb(start, stop, True,
                         _decompress_helper, self._decompress)) as it:
            for data in it:
                records = unpack_data_records(data)
                if stop is not None and records[0] >= stop:
                    break
                yield records

    def sloppy_block_map(self, fn, start=None, stop=None, prefix=None,
                         args=(), kwargs={}):
        # NB in the docs: anything you return from this fn has to be pickled
        # and then unpickled. So if you're going to be looking at records in
        # detail in the main process, then it's probably better to use the
        # regular search function.
        self._check_closed()
        start, stop = self._norm_search_args(start, stop, prefix)
        mrb = self._map_raw_block
        with closing(mrb(start, stop, True, _map_helper,
                         self._decompress, fn, args, kwargs)) as it:
            for result in it:
                yield result

    def sloppy_block_exec(self, fn, start=None, stop=None, prefix=None,
                          args=(), kwargs={}):
        self._check_closed()
        with closing(self.sloppy_block_map(fn, start, stop, prefix,
                                           args, kwargs)) as it:
            for _ in it:
                pass

    def search(self, start=None, stop=None, prefix=None):
        self._check_closed()
        start, stop = self._norm_search_args(start, stop, prefix)
        with closing(self.sloppy_block_search(start, stop)) as block_iter:
            for records in block_iter:
                if records[0] < start:
                    records = records[bisect_left(records, start):]
                if stop is not None and records and records[-1] >= stop:
                    records = records[:bisect_left(records, stop)]
                for record in records:
                    yield record

    def __iter__(self):
        self._check_closed()
        return self.search()

    def dump(self, out_file, start=None, stop=None, prefix=None,
             terminator=b"\n"):
        self._check_closed()
        start, stop = self._norm_search_args(start, stop, prefix)
        sbm = self.sloppy_block_map
        with closing(sbm(_dump_helper,
                         start=start, stop=stop,
                         args=(start, stop, terminator))) as it:
            out_file.writelines(it)

    def fsck(self):
        self._check_closed()
        def fail(offset, msg):
            raise ZSSCorrupt("%s at %s: %s"
                             % (self._transport.name, offset, msg))

        last_record_by_level = {}
        # prev_last_record may be None
        UnrefBlock = namedtuple("UnrefBlock",
                                ["block_level", "prev_last_record",
                                 "first_record", "block_length"])
        unref_blocks_by_offset = {}

        mrb = self._map_raw_block
        with closing(mrb(None, None, False,
                         _fsck_helper, self._decompress)) as it:
            for offset, block_length, block_level, data in it:
                if block_level == 0:
                    records = unpack_data_records(data)
                else:
                    (records, offsets, block_lengths
                     ) = unpack_index_records(data)
                if not sorted(records) == records:
                    fail(offset, "unsorted records within block")
                if block_level in last_record_by_level:
                    if records[0] < last_record_by_level[block_level]:
                        fail(offset, "unsorted records across blocks")
                assert offset not in unref_blocks_by_offset
                unref_blocks_by_offset[offset] = (
                    UnrefBlock(block_level,
                               last_record_by_level.get(block_level),
                               records[0],
                               block_length))
                last_record_by_level[block_level] = records[-1]
                if block_level > 0:
                    if not sorted(offsets) == offsets:
                        fail(offset, "unsorted offsets in index block")
                    for (ref_key, ref_offset, ref_block_length) in zip(
                            records, offsets, block_lengths):
                        if ref_offset not in unref_blocks_by_offset:
                            fail(offset,
                                 "dangling or multiple ref to %s"
                                 % (ref_offset,))
                        ref = unref_blocks_by_offset.pop(ref_offset)
                        if ref.block_level != block_level - 1:
                            fail(offset, "level %s ref to level %s" % (
                                block_level, ref.block_level))
                        if not (ref.prev_last_record
                                <= ref_key
                                <= ref.first_record):
                            fail(offset,
                                 "bad key in ref to %s" % (ref_offset,))
                        if ref.block_length != ref_block_length:
                            fail(offset,
                                 "index length %s != actual length %s"
                                 % (ref_block_length, ref.block_length))

        # check the root block
        root_ref = unref_blocks_by_offset.pop(self._root_index_offset, None)
        if root_ref is None:
            fail(self._root_index_offset, "missing root block")
        if root_ref.block_length != self._root_index_length:
            fail(self._root_index_offset,
                 "wrong root index length in header (%s != %s)"
                 % (self._root_index_length, root_ref.block_length))

        for offset in unref_blocks_by_offset:
            fail(offset, "unreferenced block")

        return "PASS"

    def close(self):
        self._transport.close()
        self._executor.shutdown()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
