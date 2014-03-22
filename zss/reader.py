# Notes on HTTP reading
# - for streaming read:
#   requests.get(url, stream=True, headers={"Range": "..."})
#   - check the headers on the response to make sure they actually
#     respected the Range request!
#   - use r.iter_content to fetch actual data
# - for non-streaming chunk fetch:
#   requests.get(url, headers={"Range": "..."})
#   r.content
#
# for header, should just fetch a big block, and then fetch more if that
#   wasn't enough.
# for general reading, need API that fetches block given offset + length
# and also ability to spawn a stream at a given offset and then read through
#   it
#
# and at the decode level, need to be able to read a block out of a fetched
# chunk, or else read a block out of a stream
#
# becomes more useful:
#   wrap an LRU around the block fetcher (e.g. to cache the root!)
#   calculate the end offset of a range request up front, and limit the
#     stream to that

import sys
import struct
import json
from cStringIO import StringIO
from bisect import bisect_left
import re
from contextlib import closing
from collections import namedtuple, deque
import multiprocessing
from concurrent.futures import ThreadPoolExecuter, ProcessPoolExecuter
from zss.common import (ZSSCorrupt,
                        MAGIC,
                        INCOMPLETE_MAGIC,
                        encoded_crc32c,
                        CRC_LENGTH,
                        header_data_length_format,
                        header_data_format,
                        header_framing,
                        block_length_format,
                        codecs,
                        read_n,
                        read_format)
from zss._zss import unpack_data_records, unpack_index_records

# How much data to read from the header on our first request on slow
# transports. If the header is shorter than this, then we waste a bit of
# bandwidth reading too much. OTOH if the header is longer than this, then we
# have to make a second request, incurring the relevant latency. So we want a
# number that's bigger than most headers, but not *too* big. (Also, it *must*
# be larger than the magic + header length field, which is currently 16.)
HEADER_SIZE_GUESS = 8192

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

class FileTransport(object):
    remote = False

    def __init__(self, path):
        self._path = path
        self._file = open(path, "rb")

    # This allows partial reads (i.e., if EOF falls in the middle of the
    # requested chunk, then we return the part before the EOF). Fortunately,
    # this is how both normal Python read() and how HTTP Range work
    # out-of-the-box.
    def chunk_read(self, offset, length):
        self._file.seek(offset)
        return self._file.read(length)

    # Returns a file-like object which will return bytes from the given
    # position. 'end_offset', if given, is a hint -- the returned file-like
    # object may or may not EOF after reaching this point.
    def stream_read(self, offset, end_offset=None):
        new_file = os.fdopen(os.dup(self._file.fileno()), "rb")
        new_file.seek(offset)
        return new_file

class HTTPTransport(object):
    remote = True

    def __init__(self, url):
        # XX proper checking on import
        import requests
        self._url = url

    _crange_re = re.compile(r"^bytes (\d+)-")
    def _response_offset(self, response):
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.16
        # Content-Range tells you what data you actually got, and looks like:
        #   "bytes X-Y/Z"
        # or
        #   "bytes */Z"
        # where X & Y are integers, and Z is either an integers or "*"
        # The second form is only allowed on error responses.
        crange = response.header.get("Content-Range", "")
        match = self._crange_re.match(crange)
        if not match:
            return 0
        return int(match.group(1))

    # XX put an LRU cache on this
    def chunk_read(self, offset, length):
        headers = {"Range": "bytes=%s-%s" % (offset, offset + length)}
        response = requests.get(self._url, headers=headers)
        # if we got an error response, raise an exception
        response.raise_for_status()
        if offset != self._response_offset(response):
            raise ZSSError("HTTP server did not respect Range: request")
        # .content is the byte (not text) version of the response
        return response.content

    def stream_read(self, offset, end_offset=None):
        if end_offset is None:
            end_offset = ""
        # Limited range is "100-200", endless range is "100-".
        headers = {"Range": "bytes=%s-%s" % (offset, end_offset)}
        response = requests.get(self._url, header=headers, stream=True)
        response.raise_for_status()
        if offset != self._response_offset(response):
            raise ZSSError("HTTP server did not respect Range: request")
        return _HTTPStream(offset, response)

class _HTTPStream(object):
    def __init__(self, offset, response):
        self._response = response
        self._offset = offset

    def tell(self):
        return self._offset

    def read(self, length):
        for chunk in self._response.iter_content(length):
            offset += len(chunk)
            return chunk
        return ""

    def close(self):
        self._response.close()

class _ZSSMapStop(Exception):
    pass

def _decompress_helper(block_level, zdata, stop, decompress_fn):
    # stopping has to be left to the next level up
    return decompress_fn(zdata)

def _map_helper(block_level, zdata, stop, decompress_fn,
                user_fn, user_args, user_kwargs):
    data = decompress_fn(zdata)
    records = unpack_data_records(data)
    if records[0] >= stop:
        raise _ZSSMapStop()
    return user_fn(records, *user_args, **user_kwargs)

def _dump_helper(records, start, stop, terminator):
    if records[0] >= stop:
        raise _ZSSMapStop()
    if records[0] < start:
        records = records[bisect_left(records, start):]
    if records[-1] >= stop:
        records = records[:bisect_left(records, stop)]
    records.append("")
    return terminator.join(records)

class ZSS(object):
    def __init__(self, path, workers="auto"):
        self._path = path
        self._transport = FileTransport(path)

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

        if workers == "auto":
            # XX put an upper bound on this
            workers = multiprocessing.cpu_count()
        self._workers = workers
        if workers == 1:
            self._executor = ThreadPoolExecuter(workers)
        else:
            self._executor = ProcessPoolExecuter(workers)

    def _get_header(self):
        chunk = self._transport.read_chunk(0, HEADER_SIZE_GUESS)
        stream = StringIO(chunk)

        magic = read_n(stream, len(MAGIC))
        if magic == INCOMPLETE_MAGIC:
            raise ZSSCorrupt("%s: looks like this ZSS file was only "
                             "partially written" % (self._path,))
        if magic != MAGIC:
            raise ZSSCorrupt("%s: bad magic number (are you sure this is "
                             "a ZSS file?)" % (self._path))
        header_data_length = read_format(stream, header_data_length_format)

        needed = header_data_length + CRC_LENGTH
        remaining = len(chunk) - stream.tell()
        if remaining < needed:
            rest = self._transport.read_chunk(len(chunk), needed - remaining)
            stream = StringIO(stream.read() + rest)

        header_encoded = read_n(stream, header_data_length)
        header_crc = read_n(stream, CRC_LENGTH)
        if encoded_crc32c(header_encoded) != header_crc:
            raise ZSSCorrupt("%s: header checksum mismatch" % (self._path,))

        return _decode_header_data(header_encoded)

    def _get_next_raw_block(self, stream):
        # Returns (None, None) for EOF
        try:
            (block_level,
             zdata_length) = read_format(stream, block_prefix_format)
        except ZSSCorrupt:
            # EOF
            return (None, None)
        zdata = stream.read(zdata_length)
        return (block_level, zdata)

    def _get_block(self, offset, block_length):
        chunk = self._transport.chunk_read(offset, block_length)
        block_level = ord(chunk[0])
        assert block_level > 0
        zdata = chunk[struct.calcsize(block_prefix_format):]
        data = self._decompress(zdata)
        return (block_level, unpack_index_records(data))

    # Returns offset of either the first or second data (level-0) block which
    # contains entries that are >= the needle.
    #
    # Suppose these are our blocks:
    #    [a b c d e] [f g h i j]
    # and our needle is "d". We can't return a pointer to "d" directly, we
    # have to return a pointer to one of the blocks, i.e., to either "a" or
    # "f". round_down=True means we return a pointer to the "a" block,
    # round_down=False means we return a pointer to the "f" block.
    def _find_ge_block(self, needle, round_down):
        offset = self._root_index_offset
        block_length = self._root_index_length
        while True:
            block_level, values = self._get_block(offset, block_length)
            assert block_level > 0
            keys, voffsets, block_lengths = values
            # This gives us the index of the first block whose *first* entry
            # is >= the needle.
            idx = bisect_left(keys, needle)
            # The first block which can potentially contain *any* entries >=
            # the needle is the block *before* the one we found -- if there is
            # such a block.
            if round_down and idx != 0:
                idx -= 1
            offset = offsets[idx]
            block_length = block_lengths[idx]
            # Our (offset, block_length) now point to a block whose block
            # level is (block_level - 1).
            if block_level - 1 == 0:
                return offset

    def _norm_search_args(self, start, stop, prefix):
        # we intersect start/stop/prefix together
        if start is None:
            start = ""
        # unfortunately, there is no concrete value we can put for "stop" that
        # is equivalent to leaving it unspecified, so we have to do some
        # if-then-wrangling below.
        if prefix is not None:
            start = max(prefix, start)
            if prefix:
                prefix_stop = prefix[:-1] + chr(ord(prefix[-1]) + 1)
            if stop is None:
                stop = prefix_stop
            else:
                stop = min(stop, prefix_stop)
        # now start is always a string, and stop is either a string or None,
        # and we can ignore 'prefix', because its effect has been incorporated
        # into start/stop.
        return start, stop

    def _sloppy_stream(self, start, stop):
        start_offset = self._find_ge_block(start, True)
        stop_offset = None
        if self._transport.remote and stop is not None:
            stop_offset = self._find_ge_key(stop, False)
        return self._transport.stream_read(start_offset, stop_offset)

    def _map_raw_block(self, start, stop, skip_index, fn, *args, **kwargs):
        stream = self._sloppy_stream(start, stop)
        q = deque()
        eof = False
        try:
            while not eof or q:
                while not eof and len(q) < self._workers:
                    block_level, zdata = self._get_next_raw_block(stream)
                    if block_level is None:
                        eof = True
                        continue
                    if skip_index and block_level > 0:
                        continue
                    q.append(self._executor.submit(fn, block_level, zdata,
                                                   stop, *args, **kwargs))
                yield q.popleft().result()
        except _ZSSMapStop:
            pass
        finally:
            while q:
                q.pop().cancel()

    def sloppy_block_search(self, start=None, stop=None, prefix=None):
        start, stop = self._norm_search_args(start, stop, prefix)
        mrb = self._map_raw_block
        with closing(mrb(start, stop, True,
                         _decompress_helper, self._decompress)) as it:
            for data in it:
                records = unpack_data_records(data)
                if records[0] >= stop:
                    break
                yield records

    def sloppy_block_map(self, fn, start=None, stop=None, prefix=None,
                         args=(), kwargs={}):
        start, stop = self._norm_search_args(start, stop, prefix)
        mrb = self._map_raw_block
        with closing(mrb(start, stop, True, _map_helper,
                         self._decompress, fn, args, kwargs)) as it:
            for result in it:
                yield result

    def sloppy_block_exec(self, fn, start=None, stop=None, prefix=None,
                          args=(), kwargs={}):
        for result in self.sloppy_block_map(fn, start, stop, prefix,
                                            args, kwargs):
            continue

    def search(self, start=None, stop=None, prefix=None):
        start, stop = self._norm_search_args(start, stop, prefix)
        with closing(self.sloppy_block_search(start, stop)) as block_iter:
            for records in block_iter:
                if records[0] < start:
                    records = records[bisect_left(records, start):]
                if records[-1] >= stop:
                    records = records[:bisect_left(records, stop)]
                for record in records:
                    yield record

    def __iter__(self):
        return self.search()

    def dump(self, out_file, start=None, stop=None, prefix=None,
             terminator="\n"):
        start, stop = self._norm_search_args(start, stop, prefix)
        sbm = self.sloppy_block_map
        with closing(sbm(_dump_helper,
                         start=start, stop=stop,
                         args=(start, stop, terminator))) as it:
            out_file.writelines(it)

    def fsck(self):
        def fail(offset, msg):
            raise ZSSCorrupt("%s at %s: %s" % (self._path, offset, msg))

        block_prefix_length = struct.calcsize(block_prefix_format)
        last_record_by_level = {}
        # prev_last_record may be None
        UnrefBlock = namedtuple("UnrefBlock",
                                ["block_level", "prev_last_record",
                                 "first_record", "block_length"])
        unref_blocks_by_offset = {}

        with closing(self._transport.stream_read(start)) as stream:
            while True:
                offset = stream.tell()
                block_level, contents = self._get_next_block(stream)
                block_length = stream.tell() - offset
                if block_level is None:
                    break
                if block_level == 0:
                    records = contents
                else:
                    records, offsets, block_lengths = contents
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
                            fail(offset, "dangling ref to %s" % (ref_offset,))
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
