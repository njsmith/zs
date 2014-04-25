# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import struct
import json
from bisect import bisect_left
from contextlib import closing, contextmanager
from collections import namedtuple, OrderedDict
import multiprocessing
import threading
import weakref
import hashlib
import binascii

from six import Iterator, BytesIO, indexbytes, int2byte, reraise
from six.moves import queue

from .futures import SerialExecutor, ProcessPoolExecutor
from .common import (ZSError,
                     ZSCorrupt,
                     MAGIC,
                     INCOMPLETE_MAGIC,
                     FIRST_EXTENSION_LEVEL,
                     encoded_crc64xz,
                     CRC_LENGTH,
                     header_data_length_format,
                     header_data_format,
                     codecs,
                     read_n,
                     read_format,
                     write_length_prefixed)
from ._zs import (unpack_data_records, unpack_index_records, read_uleb128)
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
            length, = read_format(f, "<Q")
            data = read_n(f, length)
            chars = data.decode("utf-8")
            fields[field] = json.loads(chars)
        elif field_format == "NUL-padded-ascii-16":
            value = read_format(f, "16s")[0]
            value = value.rstrip(b"\x00")
            value = value.decode("ascii")
            fields[field] = value
        else:
            fields[field] = read_format(f, field_format)[0]
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
        raise ZSCorrupt("unexpected EOF")
    return block_contents, checksum

def test__get_raw_block_unchecked():
    f = BytesIO(b"\x05" + b"\x01" * 5 + b"\x02" * 8 + b"\x03")
    assert _get_raw_block_unchecked(f) == (b"\x01" * 5, b"\x02" * 8)
    from nose.tools import assert_raises
    f_partial1 = BytesIO(b"\x05" + b"\x01" * 4)
    assert_raises(ZSCorrupt, _get_raw_block_unchecked, f_partial1)
    f_partial2 = BytesIO(b"\x05" + b"\x01" * 5 + b"\x02" * 3)
    assert_raises(ZSCorrupt, _get_raw_block_unchecked, f_partial2)

def _check_block(offset, raw_block, checksum):
    if encoded_crc64xz(raw_block) != checksum:
        raise ZSCorrupt("checksum mismatch at %s" % (offset,))
    block_level = indexbytes(raw_block, 0)
    zpayload = raw_block[1:]
    return (block_level, zpayload)

# exception that can be raised by map_raw_block callback functions
class _ZSMapStop(Exception):
    pass

# sentinel used for communication between _map_raw_helper and main process,
# and also between _block_map_helper and main process.
class _ZS_MAP_SKIP(object):
    pass

def _map_raw_helper(offset, block_length, raw_block, checksum,
                    skip_index, start, stop, fn, args, kwargs):
    block_level, zpayload = _check_block(offset, raw_block, checksum)
    if block_level >= FIRST_EXTENSION_LEVEL:
        return _ZS_MAP_SKIP
    if skip_index and block_level > 0:
        return _ZS_MAP_SKIP
    return fn(offset, block_length, block_level, zpayload, start, stop,
              *args, **kwargs)

def _decompress_helper(offset, block_length,
                       block_level, zpayload, start, stop, decompress_fn):
    # stopping has to be left to the next level up
    return decompress_fn(zpayload)

def _trim_records(records, start, stop):
    if records[0] < start:
        records = records[bisect_left(records, start):]
    if stop is not None and records and records[-1] >= stop:
        records = records[:bisect_left(records, stop)]
    return records

def _block_map_helper(offset, block_length, block_level, zpayload,
                      start, stop, decompress_fn,
                      user_fn, user_args, user_kwargs):
    payload = decompress_fn(zpayload)
    records = unpack_data_records(payload)
    if stop is not None and records[0] >= stop:
        raise _ZSMapStop()
    records = _trim_records(records, start, stop)
    if not records:
        return _ZS_MAP_SKIP
    return user_fn(records, *user_args, **user_kwargs)

def _dump_helper(records, terminator, length_prefixed):
    if length_prefixed is None:
        records.append(b"")
        return terminator.join(records)
    else:
        out = BytesIO()
        write_length_prefixed(out, records, length_prefixed)
        return out.getvalue()

def _validate_helper(offset, block_length, block_level, zpayload, start, stop,
                 decompress_fn):
    return (offset, block_length, block_level, decompress_fn(zpayload))

# A simple LRU cache. This has a somewhat awkward API because we don't want it
# to ever hold a reference to the ZS object, because that would create a
# reference loop. And in particular, this means that it can't hold a reference
# to the bound method that's being cached, so we have to pass that in on every
# use.
class _LRU(object):
    def __init__(self, max_size):
        self._max_size = max_size
        self._data = OrderedDict()

    # Notice that *only* 'args' is used as a key for the cache -- you must
    # always pass the same 'fn' when calling this method.
    def lru_call(self, fn, *args):
        if args in self._data:
            # remove item so that reinserting it will move it to the end
            value = self._data.pop(args)
        else:
            value = fn(*args)
        self._data[args] = value
        while len(self._data) > self._max_size:
            self._data.popitem(last=False)
        return value

def test_LRU():
    calls = []
    def f(x):
        calls.append(x)
        return x ** 2

    cache = _LRU(3)
    assert cache.lru_call(f, 2) == 4
    assert cache.lru_call(f, 3) == 9
    assert cache.lru_call(f, 4) == 16
    assert calls == [2, 3, 4]
    # cache hits do not call fn
    assert cache.lru_call(f, 2) == 4
    assert cache.lru_call(f, 3) == 9
    assert calls == [2, 3, 4]
    # when cache is full, least-recently-used items get evicted first
    assert cache.lru_call(f, 5) == 25 # drops 4
    assert calls == [2, 3, 4, 5]
    assert cache.lru_call(f, 4) == 16 # drops 2
    assert calls == [2, 3, 4, 5, 4]
    assert cache.lru_call(f, 2) == 4
    assert calls == [2, 3, 4, 5, 4, 2]

class ZS(object):
    """Object representing a .zs file opened for reading.

    :arg path: A string containing an on-disk file to be opened. Exactly one
      of ``path`` or ``url`` must be specified.

    :arg url: An HTTP (or HTTPS) URL pointing to a .zs file, which will be
      accessed directly from the server. The server must support Range:
      queries. Exactly one of ``path`` or ``url`` must be specified.

    :arg parallelism: When querying a ZS file, there are always at least 2
      threads working in parallel: the main thread, where you iterate over the
      results and presumably do something with them, and a second thread used
      for IO. In addition, we can spawn any number of worker processes which
      will be used internally for decompression and other CPU-intensive
      tasks. ``parallelism=1`` means to spawn 1 worker process; if you want to
      perform decompression and other such tasks in serial in your main
      thread, then use ``parallelism=0``. The default of
      ``parallelism="guess"`` means to spawn one worker process per available
      CPU.

      Note that if you know that you are going to read just a few records on
      each search, then parallelism=0 may be slightly faster; this saves the
      overhead of setting up the worker processes, and they only really help
      when doing large bulk reads.

    :arg index_block_cache: The number of index blocks to keep cached in
      memory. This speeds up repeated queries. Larger values provide better
      caching, but take more memory. Usually you'll want this to at least be
      as large as the depth of your .zs file's index tree, to ensure that the
      root block stays cached.

    This object can be used as a context manager, e.g.::

        with ZS("./my/favorite.zs") as z:
            ...

    """
    def __init__(self, path=None, url=None,
                 parallelism="guess", index_block_cache=32):
        if path is not None and url is None:
            self._transport = FileTransport(path)
        elif path is None and url is not None:
            self._transport = HTTPTransport(url)
        else:
            raise ValueError("exactly one of path= or url= must be given")

        header, self._header_end = self._get_header()

        self.root_index_offset = header["root_index_offset"]
        self.root_index_length = header["root_index_length"]
        codec = header["codec"]
        if codec not in codecs:
            raise ZSCorrupt("unrecognized compression codec %r"
                             % (codec,))
        self._decompress = codecs[codec][-1]
        self.total_file_length = header["total_file_length"]
        # Necessary to check this to meet our guarantee that we will never
        # miss returning data that should have been returned.
        actual_length = self._transport.length()
        if actual_length != self.total_file_length:
            raise ZSCorrupt("file is %s bytes, but header says it should "
                             "be %s"
                             % (actual_length, self.total_file_length))
        self.codec = codec
        self.data_sha256 = header["sha256"]
        self.metadata = header["metadata"]
        if not isinstance(self.metadata, dict):
            raise ZSCorrupt("bad metadata")

        if parallelism == "guess":
            # XX put an upper bound on this
            parallelism = multiprocessing.cpu_count()
        self._parallelism = parallelism
        if self._parallelism < 0:
            raise ValueError("parallelism must be >= 0 or \"guess\"")
        if parallelism == 0:
            self._executor = SerialExecutor()
        else:
            self._executor = ProcessPoolExecutor(parallelism)

        self._index_block_lru = _LRU(index_block_cache)

        self._mrbs = weakref.WeakKeyDictionary()
        self._closed = False

    def _check_closed(self):
        if self._closed:
            raise ZSError("attemped operation on closed ZS file")

    def _get_header(self):
        chunk = self._transport.chunk_read(0, HEADER_SIZE_GUESS)
        stream = BytesIO(chunk)

        magic = read_n(stream, len(MAGIC))
        if magic == INCOMPLETE_MAGIC:
            raise ZSCorrupt("%s: looks like this ZS file was only "
                             "partially written" % (self._transport.name,))
        if magic != MAGIC:
            raise ZSCorrupt("%s: bad magic number (are you sure this is "
                             "a ZS file?)" % (self._transport.name))
        header_data_length, = read_format(stream, header_data_length_format)

        needed = header_data_length + CRC_LENGTH
        header_end = stream.tell() + needed
        remaining = len(chunk) - stream.tell()
        if remaining < needed:
            rest = self._transport.chunk_read(len(chunk), needed - remaining)
            stream = BytesIO(stream.read() + rest)

        header_encoded = read_n(stream, header_data_length)
        header_crc = read_n(stream, CRC_LENGTH)
        if encoded_crc64xz(header_encoded) != header_crc:
            raise ZSCorrupt("%s: header checksum mismatch"
                             % (self._transport.name,))

        return _decode_header_data(header_encoded), header_end

    @property
    def root_index_level(self):
        """The level of the root index.

        Starting from scratch, finding an arbitrary record in a ZS file
        requires that we fetch the header, fetch the root block, and then
        fetch this many blocks to traverse the index tree. So that's a total
        of ``root_index_level + 2`` fetches. (On local disk, each "fetch" is a
        disk seek; over HTTP, each "fetch" is a round-trip to the server.) For
        later queries on the same :class:`ZS` object, at least the header and
        root will be cached, and (if you're lucky) other blocks may be as
        well.

        """
        level, _ = self._get_index_block(self.root_index_offset,
                                         self.root_index_length)
        return level

    def _get_index_block(self, offset, block_length):
        return self._index_block_lru.lru_call(self._get_index_block_impl,
                                              offset, block_length)

    def _get_index_block_impl(self, offset, block_length):
        chunk = self._transport.chunk_read(offset, block_length)
        if len(chunk) != block_length:
            raise ZSCorrupt("partial read on index block @ %s, length %s"
                             % (offset, block_length))
        raw_block, checksum = _get_raw_block_unchecked(BytesIO(chunk))
        block_level, zpayload = _check_block(offset, raw_block, checksum)
        if block_level == 0:
            raise ZSCorrupt("%s:%s: "
                             "expecting index block but found data block"
                             % (self._transport.name, offset))
        if block_level >= FIRST_EXTENSION_LEVEL:
            raise ZSCorrupt("%s:%s: "
                             "expecting index block but found "
                             "level %s extension block"
                             % (self._transport.name, offset, block_level))
        payload = self._decompress(zpayload)
        return (block_level, unpack_index_records(payload))

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
        assert needle is not None
        offset = self.root_index_offset
        block_length = self.root_index_length
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
            prefix_stop = prefix[:-1] + int2byte(indexbytes(prefix, -1) + 1)
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

    def _span_stream(self, start, stop):
        if start == b"":
            start_offset = self._header_end
        else:
            start_offset = self._find_ge_block(start, True)
        stop_offset = None
        if self._transport.remote and stop is not None:
            # This can return None. Fortunately stream_read can accept None as
            # a stop offset.
            stop_offset = self._find_ge_block(stop, False)
        return self._transport.stream_read(start_offset, stop_offset)

    # map_raw theory of operation:
    #
    # For each map_raw generator, there are several pieces:
    # - The generator object itself (_map_raw_block_gen), which executes in
    #   the main thread on demand, whenever user code calls next().
    # - The readahead thread, which runs in an independent thread in the main
    #   process. There is one readahead thread for each active map_raw
    #   generator.
    # - The pool of worker processes, which is shared by all iterators over a
    #   single ZS object. These are accessed via the concurrent.futures API,
    #   so we don't deal with them directly, we just dispatch work and get
    #   back results. If parallelism == 0, this might not even exist -- but
    #   this is invisible to map_raw.. (FIXME: maybe there should just be a
    #   single global worker pool shared by all ZS objects?  This would
    #   require a global parallelism setting, of course. And the
    #   start/shutdown/change lifespan becomes complicated...)
    #
    # The picture to keep in mind:
    #
    # +=============+                 +==================+
    # | generator   |                 | readahead thread |
    # |           ---- command_queue --->                |
    # | (main     <--- future_queue  ----                |
    # |   thread)   |    |            |       |          |
    # +=============+    |            +=======|==========+
    #                    |                    |
    #                    |   +~~~~~~~~~~~~~+  |
    #                    +---- worker pool <--+
    #                        +~~~~~~~~~~~~~+
    #
    # The generator is responsible for:
    # - starting up the readahead thread
    # - shutting down the readahead thread when finished
    # - providing finished results on demand to user code (when they call
    #   next())
    # - communicating flow control information to the readahead thread, so
    #   that the workers are kept busy, but not too busy.
    #
    # The readahead thread is responsible for:
    # - performing IO on the actual ZS file (this ensures that it is done in
    #   a serial manner, but without blocking the main thread).
    # - taking the bytes read from the ZS file, and dispatching them to
    #   workers to unpack and process.
    # - sending the work handles ('futures') back to the main thread. Again,
    #   this is done serially, ensuring that the main thread will get results
    #   in order (regardless of what order the actual work finishes).
    #
    # When a map_raw generator finishes, is garbage collected, or is ended by
    # an explicit call to its .close() method, then it tells the readahead
    # thread to shut down, and waits for it to finish. This ensures that by
    # the time .close() completes, the IO stream will be closed, and that no
    # more work will be enqueued.
    #
    # In addition, the ZS object keeps weak references to all active map_raw
    # generators, and when a ZS object is closed it explicitly closes all
    # generators. Once they are closed, we know that the threads are all dead,
    # so it is safe to shut down the worker processes etc.
    #
    # Each map_raw generator and readahead thread keeps a strong reference to
    # the ZS object, so the ZS object cannot be garbage-collected while any
    # map_raw iterations are active.

    # Sentinels used for communication between the readahead thread and the
    # main thread.
    class _MAP_CONTINUE(object):
        pass

    class _MAP_QUIT(object):
        pass

    class _MAP_EOF(object):
        pass

    class _MapErrorFuture(object):
        def __init__(self, exc_info):
            self._exc_info = exc_info

        def result(self):
            reraise(*self._exc_info)

        def cancel(self):
            pass

    def _readahead_thread(self, stream, start, stop, skip_index,
                          fn, args, kwargs,
                          command_queue, future_queue):
        try:
            while command_queue.get() is not self._MAP_QUIT:
                try:
                    offset = stream.tell()
                    (raw_block, checksum) = _get_raw_block_unchecked(stream)
                    block_length = stream.tell() - offset
                    if raw_block is None:
                        future_queue.put(self._MAP_EOF)
                        return
                    f = self._executor.submit(_map_raw_helper,
                                              offset, block_length,
                                              raw_block, checksum,
                                              skip_index, start, stop,
                                              fn, args, kwargs)
                    future_queue.put(f)
                # This can happen if, e.g., _get_raw_block_unchecked errors
                # out in a corrupt file.
                except Exception:
                    future_queue.put(self._MapErrorFuture(sys.exc_info()))
            else:
                # we got a QUIT, which means our consumer has disappeared, so
                # it would be polite to try and cancel any outstanding jobs.
                try:
                    while True:
                        f = future_queue.get_nowait()
                        assert f is not self._MAP_EOF
                        f.cancel()
                except queue.Empty:
                    pass
        finally:
            stream.close()

    def _map_raw_block(self, *args, **kwargs):
        """This is a low-level function with somewhat fiddly semantics.

        All the more user-friendly functions are implemented in terms of it.

        It finds all blocks which may contain records that are >= start, and
        then for each such block it calls::

          fn(offset, block_length, block_level, zpayload, start, stop,
             *args, **kwargs)

        Key points:

        * If skip_index is true, then it skips over index blocks (i.e.,
          block_level will always be 0).

        * It unconditionally skips over "extension blocks" (those with level
          >= 64).

        * These calls are performed in parallel in however many worker
          processes were configured when this ZS object was created;
          therefore, fn, args, and kwargs must all be pickleable (unless you
          use parallelism=0).

        * The iteration may or may not stop when the 'stop' key is reached. If
          you want it to stop for sure at any point, you must either (a) raise
          a _ZSMapStop from your callback function, or (b) call .close() on
          this generator.
        """
        gen = self._map_raw_block_gen(*args, **kwargs)
        self._mrbs[gen] = 1
        return gen

    def _map_raw_block_gen(self, start, stop, skip_index, fn, *args, **kwargs):
        stream = self._span_stream(start, stop)
        command_queue = queue.Queue()
        future_queue = queue.Queue()
        for i in range(self._parallelism):
            command_queue.put(self._MAP_CONTINUE)
        rt = threading.Thread(target=self._readahead_thread,
                              args=(stream,
                                    start, stop, skip_index, fn, args, kwargs,
                                    command_queue, future_queue))
        try:
            rt.start()
            while True:
                command_queue.put(self._MAP_CONTINUE)
                future = future_queue.get()
                if future is self._MAP_EOF:
                    return
                value = future.result()
                if value is not _ZS_MAP_SKIP:
                    yield value
        except _ZSMapStop:
            # Some job requested early termination of the loop
            return
        finally:
            # We can reach this point in a number of situations:
            # - regular exit from above loop
            # - error exit from above loop
            # - .close() called on generator
            # - generator is garbage collected
            command_queue.put(self._MAP_QUIT)
            rt.join()

    def search(self, start=None, stop=None, prefix=None):
        """Iterate over all records matching the given query.

        A record is considered to "match" if:

        * ``start <= record``, and
        * ``record < stop``, and
        * ``record.startswith(prefix)``

        Any or all of the arguments can be left as ``None``, in which case the
        corresponding check or checks are not performed.

        Note the asymmetry between ``start`` and ``stop`` -- this is analogous
        to other Python constructs which use half-open [start, stop) ranges,
        like :func:`range`.

        If no arguments are given, iterates over the entire contents of the
        .zs file.

        Records are always returned in sorted order.
        """
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
                records = _trim_records(records, start, stop)
                for record in records:
                    yield record

    def block_map(self, fn, start=None, stop=None, prefix=None,
                  args=(), kwargs={}):
        """Apply a given function -- in parallel -- to records matching a
        given query. This function is lazy -- you have to iterate

        Using this method (or its friend, :meth:`block_exec`) is the
        best way to perform large bulk operations on ZS files.

        The way to think about this is, first we find all records matching the
        given query::

            matches = zs_obj.search(start=start, stop=stop, prefix=prefix)

        and then we divide the resulting list of records up into arbitrarily
        sized chunks, and for each chunk we call the given function, and yield
        the result::

            while there are matches:
                chunk = list(get arbitrarily many matches)
                yield fn(chunk, *args, **kwargs)

        But, there is a trick: in fact many copies of the function are run in
        parallel in different worker processes, and then the results are
        passed back to the main process for you to collect. (Think "poor-man's
        map-reduce".)

        This means that your ``fn``, ``args``, ``kwargs``, and return values
        must all be pickleable. In particular, ``fn`` probably has to either
        be a global function in a named module, or else an object with a
        ``__call__`` method that is an instance of a globally defined class in
        a named module. (Sorry, I didn't make the rules. Feel free to submit
        patches to use a more featureful serialization library like 'dill',
        esp. if you can demonstrate that they don't add too much overhead.)

        This will be most efficient if ``fn`` performs non-trivial work, and
        especially if it can avoid returning large/complicated structures from
        ``fn`` -- after all, the whole idea is that the code that's looping
        over the results from :meth:`block_map` should have less work to do than
        it would if it were just calling :meth:`search` directly.

        If you manage to take this to the extreme where you have nothing to
        return from :meth:`block_map` (maybe your ``fn`` is writing to a
        database or something), then you can use :meth:`block_exec` instead to
        save a bit of boilerplate.

        If you pass ``parallelism=0`` when creating your :class:`ZS` object,
        then this method will perform all work within the main process. This
        makes debugging a lot easier, because it will let you get real
        backtraces if (when) your ``fn`` crashes.

        """
        self._check_closed()
        start, stop = self._norm_search_args(start, stop, prefix)
        mrb = self._map_raw_block
        with closing(mrb(start, stop, True, _block_map_helper,
                         self._decompress, fn, args, kwargs)) as it:
            for result in it:
                if result is not _ZS_MAP_SKIP:
                    yield result

    def block_exec(self, fn, start=None, stop=None, prefix=None,
                   args=(), kwargs={}):
        """Eager version of :meth:`block_map`.

        This is equivalent to calling :meth:`block_map`, iterating over the
        results, and throwing them all away.

        """
        self._check_closed()
        with closing(self.block_map(fn, start, stop, prefix,
                                    args, kwargs)) as it:
            for _ in it:
                pass

    def __iter__(self):
        """Equivalent to ``zs_obj.search()``."""
        self._check_closed()
        return self.search()

    def dump(self, out_file, start=None, stop=None, prefix=None,
             terminator=b"\n", length_prefixed=None):
        """Decompress a given range of the .zs file to another file. This is
        performed in the most efficient available way.

        :arg terminator: A terminator appended to the end of each
          record. Default is a newline. (Ignored if ``length_prefixed`` is
          given.)
        :type terminator: byte string

        :arg length_prefixed: If given, records are output in a
          length-prefixed format, and ``terminator`` is ignored. Valid values
          are the strings ``"uleb128"`` or ``"u64le"``, or None.

        See :meth:`search` for the definition of ``start``, ``stop``, and
        ``prefix``.

        On Python 3, ``out_file`` must be opened in binary mode.

        For a convenient command-line interface to this method, see :ref:`zs
        dump`.

        """
        self._check_closed()
        with closing(self.block_map(_dump_helper,
                                    start=start, stop=stop, prefix=prefix,
                                    args=(terminator, length_prefixed))) as it:
            # .writelines() has the same semantics, but for some reason (at
            # least on Python 2.7) it likes to queue up a whole bunch of
            # records and then write them all at once (presumably to amortize
            # the IO costs). We're already amortizing the cost of operations
            # over blocks, so it's better to just reimplement .writelines():
            #out_file.writelines(it)
            for chunk in it:
                out_file.write(chunk)

    def validate(self):
        """Validate this .zs file for correctness.

        This method does an exhaustive check of the current file, to validate
        it for self-consistency and compliance with the ZS specification. It
        should catch all cases of disk corruption (with high probability), and
        all cases of incorrectly constructed files.

        This reads and decompresses the entire file, so may take some time.

        For a convenient command-line interface to this method, see :ref:`zs
        validate`.

        """
        self._check_closed()
        failures = []
        def add_fail(offset, msg):
            failures.append((offset, msg))

        hasher = hashlib.sha256()

        UnrefBlock = namedtuple("UnrefBlock",
                                ["block_level", "first_record",
                                 "last_record", "block_length"])
        unref_blocks_by_offset = {}

        def check_index(offset, block_level, records, offsets, block_lengths):
            if not sorted(offsets) == offsets:
                add_fail(offset, "unsorted offsets in index block")
            # The idea is that these will end up indicating the first record
            # and the last record of the underlying data blocks (recursively)
            # referenced by this index block. We walk last_record along the
            # blocks we point to as we go, so we can check each key as we go.
            first_record = None
            last_record = None
            for i, ref_offset in enumerate(offsets):
                if ref_offset not in unref_blocks_by_offset:
                    add_fail(offset,
                             "dangling or multiple refs to %s" % (ref_offset,))
                    continue
                ref = unref_blocks_by_offset.pop(ref_offset)
                if first_record is None:
                    first_record = ref.first_record
                if ref.block_level != block_level - 1:
                    add_fail(offset, "bad index ref from level %s to level %s"
                             % (block_level, ref.block_level))
                if (last_record is not None
                    and not (last_record <= records[i])):
                    add_fail(offset, "key %s is too small for block at %s"
                             % (records[i], ref_offset))
                if not (records[i] <= ref.first_record):
                    add_fail(offset, "key %s is too large for block at %s"
                             % (records[i], ref_offset))
                # advance, to use for next round
                last_record = ref.last_record
                if ref.block_length != block_lengths[i]:
                    add_fail(offset,
                             "index length %s != actual length %s for "
                             "block at %s"
                             % (block_lengths[i], ref.block_length, ref_offset))

            unref_blocks_by_offset[offset]= (
                UnrefBlock(block_level,
                           first_record, last_record,
                           block_length))

        mrb = self._map_raw_block
        with closing(mrb(b"", None, False,
                         _validate_helper, self._decompress)) as it:
            for offset, block_length, block_level, data in it:
                if block_level == 0:
                    hasher.update(data)
                    records = unpack_data_records(data)
                else:
                    (records, offsets, block_lengths
                     ) = unpack_index_records(data)
                if not sorted(records) == records:
                    add_fail(offset, "unsorted records within block")
                assert offset not in unref_blocks_by_offset
                if block_level > 0:
                    check_index(offset, block_level,
                                records, offsets, block_lengths)
                else:
                    unref_blocks_by_offset[offset] = (
                        UnrefBlock(block_level,
                                   records[0], records[-1],
                                   block_length))

        # check the root block
        root_ref = unref_blocks_by_offset.pop(self.root_index_offset, None)
        if root_ref is None:
            add_fail(self.root_index_offset,
                     "root block missing or doubly-referenced")
        else:
            if not 0 < root_ref.block_level < FIRST_EXTENSION_LEVEL:
                add_fail(self.root_index_offset,
                         "root index has bad level %s"
                         % (root_ref.block_level,))
            if root_ref.block_length != self.root_index_length:
                add_fail(self.root_index_offset,
                         "wrong root index length in header (%s != %s)"
                         % (self.root_index_length, root_ref.block_length))

        for offset in unref_blocks_by_offset:
            add_fail(offset, "unreferenced block")

        if hasher.digest() != self.data_sha256:
            add_fail(0, "data hash mismatch: header says %s, but I found %s"
                     % (binascii.hexlify(self.data_sha256),
                        binascii.hexlify(hasher.digest())))

        if failures:
            failure_strs = ["offset %s: %s" % (offset, msg)
                            for (offset, msg) in failures]
            raise ZSCorrupt("Integrity check failed:\n  "
                             + "\n  ".join(failure_strs))
        else:
            return "PASS"

    def close(self):
        """Close this file.

        This frees all resources. Further operations on this file will raise
        an error.

        .. note:: If you have any active iterators (from :meth:`search`,
           :meth:`block_map`, etc.), then they will be closed as well. This
           means that any further attempts to iterate them will raise
           :exc:`StopIteration`.

        """
        if self._closed:
            return
        # Slightly weird construct b/c the way garbage collection works, an
        # mrb can disappear at any time, and invalidate our iterator over
        # ._mrbs(). So safest not to use any iterator for more than one step.
        while self._mrbs:
            for mrb in self._mrbs:
                # This is guaranteed to work, because now *we* hold a strong
                # reference to mrb.
                self._mrbs.pop(mrb)
                mrb.close()
                break
        self._transport.close()
        self._executor.shutdown()
        self._closed = True

    def __del__(self):
        # __del__ gets called even if we error out during __init__
        if hasattr(self, "_closed"):
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
