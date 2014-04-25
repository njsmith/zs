# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import json
import hashlib
import os
import os.path
import multiprocessing
import struct
import sys
import getpass
import socket
import traceback
from contextlib import contextmanager
from datetime import datetime

import six

import zs
from zs.common import (ZSError,
                       MAGIC,
                       INCOMPLETE_MAGIC,
                       FIRST_EXTENSION_LEVEL,
                       CRC_LENGTH,
                       encoded_crc64xz,
                       header_data_format,
                       header_data_length_format,
                       codec_shorthands,
                       codecs,
                       read_format,
                       read_length_prefixed)
from zs._zs import (pack_data_records, pack_index_records,
                      unpack_data_records,
                      write_uleb128)

# how often to poll for pipeline errors while blocking in the main thread, in
# seconds
ERROR_CHECK_FREQ = 0.1

# update the spinner every time we write this many bytes to the file
SPIN_UPDATE_BYTES = 10 * 2 ** 20

def _flush_file(f):
    f.flush()
    os.fsync(f.fileno())

def _encode_header(header):
    enc_fields = []
    for (field, format) in header_data_format:
        if format == "length-prefixed-utf8-json":
            # In py2, json.dumps always returns str if ensure_ascii=True (the
            # default); if ensure_ascii=False it may or may not return a str
            # at its whim. In py3, json.dumps always returns unicode.
            str_encoded = json.dumps(header[field], ensure_ascii=True)
            # On py3, this is necessary. On py2, this implicitly coerces to
            # unicode and then encodes -- but because we know the string only
            # contains ascii, the implicit conversion is safe.
            encoded = str_encoded.encode("utf-8")
            enc_fields.append(struct.pack("<Q", len(encoded)))
            enc_fields.append(encoded)
        elif format == "NUL-padded-ascii-16":
            enc_fields.append(struct.pack("16s",
                                          header[field].encode("ascii")))
        else:
            enc_fields.append(struct.pack(format, header[field]))
    return b"".join(enc_fields)

def test__encode_header():
    got = _encode_header({
        "root_index_offset": 0x1234567890123456,
        "root_index_length": 0x2468864213577531,
        "total_file_length": 0x0011223344556677,
        "sha256": b"abcdefghijklmnopqrstuvwxyz012345",
        "codec": "superzip",
        # Carefully chosen to avoid containing any dicts with multiple items,
        # so as to ensure a consistent serialization in the face of dict
        # randomization.
        "metadata": {"this": ["is", "awesome", 10]},
        })
    expected_metadata = b"{\"this\": [\"is\", \"awesome\", 10]}"
    expected = (b"\x56\x34\x12\x90\x78\x56\x34\x12"
                b"\x31\x75\x57\x13\x42\x86\x68\x24"
                b"\x77\x66\x55\x44\x33\x22\x11\x00"
                b"abcdefghijklmnopqrstuvwxyz012345"
                b"superzip\x00\x00\x00\x00\x00\x00\x00\x00"
                # hex(len(expected_metadata)) == 0x1f
                b"\x1f\x00\x00\x00\x00\x00\x00\x00"
                + expected_metadata)
    assert got == expected

# A sentinel used to signal that a worker should quit.
class _QUIT(object):
    pass

def box_exception():
    e_type, e_obj, tb = sys.exc_info()
    return (e_type, e_obj, traceback.extract_tb(tb))

def reraise_boxed(box):
    e_type, e_obj, extracted_tb = box
    orig_tb_str = "".join(traceback.format_list(extracted_tb))
    raise ZSError("Error in worker: %s\n\n"
                   "(Original traceback:\n"
                   "    %s"
                   "    %s: %s\n"
                   ")"
                   % (e_obj,
                      orig_tb_str.replace("\n", "\n    "),
                      e_type.__name__,
                      e_obj,
                      )
                  )

# We have a very strict policy on exceptions: any exception anywhere in
# ZSWriter is non-recoverable.

# This context manager is wrapped around all out-of-process code, to ship
# errors back to the main process.
@contextmanager
def errors_to(q):
    try:
        yield
    except:
        # we really and truly do want a bare except: here, because even
        # KeyboardException should get forwarded to the main process so it has
        # a chance to know that the child is dead.
        q.put(box_exception())

# This context manager is wrapped around in-process code, to catch errors and
# enforce non-recoverability.
@contextmanager
def errors_close(obj):
    try:
        yield
    except:
        obj.close()
        raise

class ZSWriter(object):
    def __init__(self, path, metadata, branching_factor,
                 parallelism="guess", codec="bz2", codec_kwargs={},
                 show_spinner=True, include_default_metadata=True):
        """Create a ZSWriter object.

        .. note:: In many cases it'll be easier to just use the command line
           'zs make' tool, which is a wrapper around this class.

        :arg path: File to write to. Must not already exist.

        :arg metadata: Dict or dict-like containing arbitrary metadata for the
          .zs file. See :ref:`metadata-conventions`.

        :arg branching_factor: The number of entries to put into each *index*
          block. We use a simple greedy packing strategy, where we fill up
          index blocks until they reach this limit.

        :arg parallelism: The number of CPUs to use for compression, or "guess"
          to auto-detect. Must be >= 1.

        :arg codec: The compression method to use.

        :arg codec_kwargs: kwargs to pass to the codec compress function. All
          codecs except 'none' support a compress_level argument. The 'lzma'
          codec also supports an extreme=True/False argument.

        :arg show_spinner: Whether to show the progress meter.

        :arg include_default_metadata: Whether to auto-add some default
          metadata (time, host, user).

        Once you have a ZSWriter object, you can use the
        :meth:`add_data_block` and :meth:`add_file_contents` methods to write
        data to it. It is your job to ensure that all records are added in
        (ASCIIbetical/memcmp) sorted order.

        Once you are done adding records, you must call :meth:`close`. This
        will not be done automatically. (This is a feature, to make sure that
        errors that cause early termination leave obviously-invalid ZS files
        behind.)

        The most optimized way to build a ZS file is to use
        :meth:`add_file_contents` with terminated (not length-prefixed)
        records. However, this is only possible if your records have some
        fixed terminator that you can be sure never occurs within a record
        itself.

        """

        self._path = path
        # The testsuite writes lots of ZS files to temporary storage, so
        # better take the trouble to use O_EXCL to prevent exposing everyone
        # who runs the test suite to security holes...
        open_flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
        # O_CLOEXEC is better to use than not, but platform specific
        # O_BINARY is necessary on windows, unavailable elsewhere
        for want_if_available in ["O_CLOEXEC", "O_BINARY"]:
            open_flags |= getattr(os, want_if_available, 0)
        try:
            fd = os.open(path, open_flags, 0o666)
        except OSError as e:
            raise ZSError("%s: %s" % (path, e))
        self._file = os.fdopen(fd, "w+b")
        self.metadata = dict(metadata)
        if include_default_metadata:
            build_info = {"user": getpass.getuser(),
                          "host": socket.getfqdn(),
                          "time": datetime.utcnow().isoformat() + "Z",
                          "version": "zs %s" % (zs.__version__,),
                          }
            self.metadata.setdefault("build-info", build_info)
        self.branching_factor = branching_factor
        self._show_spinner = show_spinner
        if parallelism == "guess":
            # XX put an upper bound on this
            parallelism = multiprocessing.cpu_count()
        self._parallelism = parallelism
        self.codec = codec_shorthands.get(codec)
        if self.codec is None:
            raise ZSError("unknown codec %r (should be one of: %s)"
                          % (codec, ", ".join(codec_shorthands)))
        self._compress_fn = codecs[self.codec][0]
        self._codec_kwargs = codec_kwargs
        self._header = {
            "root_index_offset": 2 ** 63 - 1,
            "root_index_length": 0,
            "total_file_length": 0,
            "sha256": b"\x00" * 32,
            "codec": self.codec,
            "metadata": self.metadata,
            }

        self._file.write(INCOMPLETE_MAGIC)
        encoded_header = _encode_header(self._header)
        self._file.write(struct.pack(header_data_length_format,
                                     len(encoded_header)))
        self._file.write(encoded_header)
        # Put an invalid CRC on the initial header as well, for good measure
        self._file.write(b"\x00" * CRC_LENGTH)

        # It is critical that we flush the file before we re-open it in append
        # mode in the writer process!
        self._file.flush()

        self._next_job = 0
        assert parallelism > 0
        self._compress_queue = multiprocessing.Queue(2 * parallelism)
        self._write_queue = multiprocessing.Queue(2 * parallelism)
        self._finish_queue = multiprocessing.Queue(1)
        self._error_queue = multiprocessing.Queue()
        self._compressors = []
        for i in range(parallelism):
            compress_args = (self._compress_fn, self._codec_kwargs,
                             self._compress_queue, self._write_queue,
                             self._error_queue)
            p = multiprocessing.Process(target=_compress_worker,
                                        args=compress_args)
            p.start()
            self._compressors.append(p)
        writer_args = (self._path,
                       self.branching_factor,
                       self._compress_fn, self._codec_kwargs,
                       self._write_queue, self._finish_queue,
                       self._show_spinner, self._error_queue)
        self._writer = multiprocessing.Process(target=_write_worker,
                                               args=writer_args)
        self._writer.start()

        self.closed = False

    def _check_open(self):
        if self.closed:
            raise ZSError("attempted operation on closed ZSWriter")

    def _check_error(self):
        try:
            box = self._error_queue.get_nowait()
        except six.moves.queue.Empty:
            return
        else:
            self.close()
            reraise_boxed(box)

    def _safe_put(self, q, obj):
        # put can block, but it might never unblock if the pipeline has
        # clogged due to an error. so we have to check for errors occasionally
        # while waiting.
        while True:
            try:
                q.put(obj, timeout=ERROR_CHECK_FREQ)
            except six.moves.queue.Full:
                self._check_error()
            else:
                break

    def _safe_join(self, process):
        while process.is_alive():
            self._check_error()
            process.join(ERROR_CHECK_FREQ)

    def add_data_block(self, records):
        """Append the given set of records to the ZS file as a single data
        block.

        (See :ref:`format` for details on what a data block is.)

        :arg records: A list of byte strings giving the contents of each
          record.
        """

        self._check_open()
        with errors_close(self):
            if not records:
                return
            self._safe_put(self._compress_queue,
                           (self._next_job, "list", records))
            self._next_job += 1

    def add_file_contents(self, file_handle, approx_block_size,
                          terminator=b"\n", length_prefixed=None):
        """Split the contents of file_handle into records, and write them to
        the ZS file.

        The arguments determine how the contents of the file are divided into
        records and blocks.

        :arg file_handle: A file-like object whose contents are read. This
          file is always closed.

        :arg approx_block_size: The approximate size of each data block, in
          bytes, *before* compression is applied.

        :arg terminator: A byte string containing a terminator appended to the
          end of each record. Default is a newline.

        :arg length_prefixed: If given, records are output in a
          length-prefixed format, and ``terminator`` is ignored. Valid values
          are the strings ``"uleb128"`` or ``"u64le"``, or ``None``.

        """
        self._check_open()
        with errors_close(self):
            try:
                if length_prefixed is None:
                    return self._afc_terminator(file_handle,
                                                approx_block_size,
                                                terminator)
                else:
                    return self._afc_length_prefixed(file_handle,
                                                     approx_block_size,
                                                     length_prefixed)
            finally:
                file_handle.close()

    def _afc_terminator(self, file_handle, approx_block_size,
                        terminator):
        # optimized version that doesn't process records one at a time, but
        # instead slurps up whole chunks, resynchronizes, and leaves the
        # compression worker to do the splitting/rejoining.
        partial_record = b""
        next_job = self._next_job
        read = file_handle.read
        while True:
            buf = file_handle.read(approx_block_size)
            if not buf:
                # File should have ended with a newline (and we don't write
                # out the trailing empty record that this might imply).
                if partial_record:
                    raise ZSError("file did not end with terminator")
                break
            buf = partial_record + buf
            try:
                buf, partial_record = buf.rsplit(terminator, 1)
            except ValueError:
                assert terminator not in buf
                partial_record = buf
                continue
            #print "PUTTING %s" % (next_job,)
            self._safe_put(self._compress_queue,
                           (next_job, "chunk-sep", buf, terminator))
            next_job += 1
        self._next_job = next_job

    def _afc_length_prefixed(self, file_handle, approx_block_size,
                             length_prefixed):
        records = []
        this_block_size = 0
        for record in read_length_prefixed(file_handle, length_prefixed):
            records.append(record)
            this_block_size += len(record)
            if this_block_size >= approx_block_size:
                self.add_data_block(records)
                records = []
                this_block_size = 0
        if records:
            self.add_data_block(records)

    def finish(self):
        """Declare this file finished.

        This method writes out the root block, updates the header, etc.

        Importantly, we do not write out the correct magic number until this
        method completes, so no ZS reader will be willing to read your file
        until this is called (see :ref:`magic-numbers`).

        Do not call this method unless you are sure you have added the right
        records. (In particular, you definitely don't want to call this from a
        ``finally`` block, or automatically from a ``with`` block context
        manager.)

        Calls :meth:`close`.

        """
        self._check_open()
        with errors_close(self):
            # Stop all the processing queues and wait for them to finish.
            for i in range(self._parallelism):
                #sys.stderr.write("putting QUIT\n"); sys.stderr.flush()
                self._safe_put(self._compress_queue, _QUIT)
            for compressor in self._compressors:
                self._safe_join(compressor)
            #sys.stdout.write("All compressors finished; waiting for writer\n")
            # All compressors have now finished their work, and submitted
            # everything to the write queue.
            self._safe_put(self._write_queue, _QUIT)
            self._safe_join(self._writer)
        # The writer and compressors have all exited, so any errors they've
        # encountered have definitely been enqueued.
        self._check_error()
        sys.stdout.write("zs: Updating header...\n")
        root_index_offset, root_index_length, sha256 = self._finish_queue.get()
        #sys.stdout.write("zs: Root index offset: %s\n" % (root_index_offset,))
        # Now we have the root offset
        self._header["root_index_offset"] = root_index_offset
        self._header["root_index_length"] = root_index_length
        self._header["sha256"] = sha256
        # And can get the total file length
        self._file.seek(0, 2)
        self._header["total_file_length"] = self._file.tell()
        new_encoded_header = _encode_header(self._header)
        self._file.seek(len(MAGIC))
        # Read the header length and make sure it hasn't changed
        old_length, = read_format(self._file, header_data_length_format)
        if old_length != len(new_encoded_header):
            raise ZSError("header data length changed")
        self._file.write(new_encoded_header)
        self._file.write(encoded_crc64xz(new_encoded_header))
        # Flush the file to disk to make sure that all data is consistent
        # before we mark the file as complete.
        _flush_file(self._file)
        # And now we can write the MAGIC value to mark the file as complete.
        self._file.seek(0)
        self._file.write(MAGIC)
        _flush_file(self._file)
        # Done!
        self.close()

    def close(self):
        """Close the file and terminate all background processing.

        Further operations on this ZSWriter object will raise an error.

        If you call this method before calling :meth:`finish`, then you will
        not have a working ZS file.

        This object can be used as a context manager in a ``with`` block, in
        which case :meth:`close` will be called automatically, but
        :meth:`finish` will not be.
        """
        if self.closed:
            return
        self.closed = True
        self._file.close()
        for worker in self._compressors + [self._writer]:
            worker.terminate()
            worker.join()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __del__(self):
        # __del__ gets called even if we error out during __init__
        if hasattr(self, "closed"):
            self.close()

# This worker loop compresses data blocks and passes them to the write
# worker.
def _compress_worker(compress_fn, codec_kwargs,
                     compress_queue, write_queue, error_queue):
    # me = os.getpid()
    # def fyi(msg):
    #     sys.stderr.write("compress_worker:%s: %s\n" % (me, msg))
    #     sys.stderr.flush()
    with errors_to(error_queue):
        # Local variables for speed
        get = compress_queue.get
        pdr = pack_data_records
        put = write_queue.put
        while True:
            job = get()
            #fyi("got %r" % (job,))
            if job is _QUIT:
                #fyi("QUIT")
                return
            if job[1] == "chunk-sep":
                idx, job_type, buf, sep = job
                records = buf.split(sep)
                payload = pdr(records, 2 * len(buf))
            elif job[1] == "list":
                idx, job_type, records = job
                payload = pdr(records)
            else:  # pragma: no cover
                assert False
            zpayload = compress_fn(payload, **codec_kwargs)
            #fyi("putting")
            put((idx, records[0], records[-1], payload, zpayload))

def _write_worker(path, branching_factor,
                  compress_fn, codec_kwargs,
                  write_queue, finish_queue,
                  show_spinner, error_queue):
    with errors_to(error_queue):
        data_appender = _ZSDataAppender(path, branching_factor,
                                        compress_fn, codec_kwargs,
                                        show_spinner)
        pending_jobs = {}
        wanted_job = 0
        get = write_queue.get
        write_block = data_appender.write_block
        while True:
            job = get()
            #sys.stderr.write("write_worker: got\n")
            if job is _QUIT:
                assert not pending_jobs
                header_info = data_appender.close_and_get_header_info()
                finish_queue.put(header_info)
                return
            pending_jobs[job[0]] = job[1:]
            while wanted_job in pending_jobs:
                #sys.stderr.write("write_worker: writing %s\n" % (wanted_job,))
                write_block(0, *pending_jobs[wanted_job])
                del pending_jobs[wanted_job]
                wanted_job += 1

# This class coordinates writing actual data blocks to the file, and also
# handles generating the index. The hope is that indexing has low enough
# overhead that handling it in serial with the actual writes won't create a
# bottleneck...
class _ZSDataAppender(object):
    def __init__(self, path, branching_factor, compress_fn, codec_kwargs,
                 show_spinner):
        self._file = open(path, "ab")
        # Opening in append mode should put us at the end of the file, but
        # just in case...
        self._file.seek(0, 2)
        assert self._file.tell() > 0

        self._branching_factor = branching_factor
        self._compress_fn = compress_fn
        self._codec_kwargs = codec_kwargs
        # For each level, a list of entries
        # each entry is a tuple (first_record, last_record, offset)
        # last_record is kept around to ensure that records at each level are
        # sorted and non-overlapping, and because in principle we could use
        # them to find shorter keys (XX).
        self._level_entries = []
        self._level_lengths = []
        self._hasher = hashlib.sha256()

        # spinner-related stuff
        self._written_bytes = 0
        self._written_blocks = 0
        self._show_spinner = show_spinner

    def _spin(self, written_bytes, written_blocks, done):
        if not self._show_spinner:
            return
        self._written_blocks += written_blocks
        old_tick = self._written_bytes % SPIN_UPDATE_BYTES
        self._written_bytes += written_bytes
        new_tick = self._written_bytes % SPIN_UPDATE_BYTES
        if done or old_tick != new_tick:
            if old_tick > 0:
                sys.stdout.write("\r")
            sys.stdout.write("zs: Blocks written: %s"  # no \n
                             % (self._written_blocks,))
            if done:
                sys.stdout.write("\n")
            sys.stdout.flush()

    def write_block(self, level, first_record, last_record, payload, zpayload):
        if not (0 <= level < FIRST_EXTENSION_LEVEL):
            raise ZSError("invalid level %s" % (level,))

        if level == 0:
            self._hasher.update(payload)

        block_offset = self._file.tell()
        block_contents = six.int2byte(level) + zpayload
        write_uleb128(len(block_contents), self._file)
        self._file.write(block_contents)
        self._file.write(encoded_crc64xz(block_contents))
        total_block_length = self._file.tell() - block_offset

        self._spin(total_block_length, 1, False)

        if level >= len(self._level_entries):
            # First block we've seen at this level
            assert level == len(self._level_entries)
            self._level_entries.append([])
            # This can only happen if all the previous levels just flushed.
            for i in range(level):
                assert not self._level_entries[i]
        entries = self._level_entries[level]
        entries.append((first_record, last_record,
                        block_offset, total_block_length))
        if len(entries) >= self._branching_factor:
            self._flush_index(level)

    def _flush_index(self, level):
        entries = self._level_entries[level]
        assert entries
        self._level_entries[level] = []
        keys = [entry[0] for entry in entries]
        offsets = [entry[2] for entry in entries]
        block_lengths = [entry[3] for entry in entries]
        payload = pack_index_records(keys, offsets, block_lengths)
        zpayload = self._compress_fn(payload, **self._codec_kwargs)
        first_record = entries[0][0]
        last_record = entries[-1][1]
        self.write_block(level + 1, first_record, last_record,
                         payload, zpayload)

    def close_and_get_header_info(self):
        # We need to create index blocks referring to all dangling
        # unreferenced blocks. If at any point we have only a single
        # unreferenced index block, then this is our root index.
        def have_root():
            # Useful invariant: we know that there is always at least one
            # unreferenced block at the highest level.
            assert len(self._level_entries[-1]) > 0
            # If all we have are data blocks, then we aren't done; root must
            # be an index block.
            if len(self._level_entries) == 1:
                return False
            # If there's a non-referenced at the non-highest level, we aren't
            # done.
            for entries in self._level_entries[:-1]:
                if entries:
                    return False
            # If the highest level has multiple blocks, we aren't done.
            if len(self._level_entries[-1]) > 1:
                return False
            # Otherwise, we are done!
            return True

        if not self._level_entries:
            raise ZSError("cannot create empty ZS file")

        while not have_root():
            for level in range(FIRST_EXTENSION_LEVEL):
                if self._level_entries[level]:
                    self._flush_index(level)
                    break

        # wait until the root has been flushed
        self._spin(0, 0, True)

        _flush_file(self._file)
        self._file.close()
        root_entry = self._level_entries[-1][0]
        return root_entry[-2:] + (self._hasher.digest(),)
        assert False  # pragma: no cover
