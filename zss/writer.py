# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import json
from uuid import uuid4
import os
import os.path
import multiprocessing
import struct
import sys
import getpass
import socket
from datetime import datetime

import six

from zss.common import (ZSSError,
                        MAGIC,
                        INCOMPLETE_MAGIC,
                        MAX_LEVEL,
                        CRC_LENGTH,
                        encoded_crc32c,
                        header_data_format,
                        header_data_length_format,
                        codecs,
                        read_format)
from zss._zss import pack_data_records, pack_index_records, write_uleb128

def _flush_file(f):
    f.flush()
    os.fsync(f.fileno())

def _encode_header(header):
    bytes = []
    for (field, format) in header_data_format:
        if format == "length-prefixed-utf8-json":
            encoded = json.dumps(header[field], ensure_ascii=True)
            bytes.append(struct.pack("<I", len(encoded)))
            bytes.append(encoded)
        else:
            bytes.append(struct.pack(format, header[field]))
    return b"".join(bytes)

def test__encode_header():
    got = _encode_header({
        "root_index_offset": 0x1234567890123456,
        "root_index_length": 0x2468864213577531,
        "uuid": b"abcdefghijklmnop",
        "compression": "superzip",
        "metadata": {"this": "is", "awesome": 10},
        })
    expected_metadata = b"{\"this\": \"is\", \"awesome\": 10}"
    expected = (b"\x56\x34\x12\x90\x78\x56\x34\x12"
                b"\x31\x75\x57\x13\x42\x86\x68\x24"
                b"abcdefghijklmnop"
                b"superzip\x00\x00\x00\x00\x00\x00\x00\x00"
                # hex(len(expected_metadata)) == 0x1d
                b"\x1d\x00\x00\x00"
                + expected_metadata)
    assert got == expected

# A sentinel used to signal that a worker should quit.
class _QUIT(object):
    pass

class ZSSWriter(object):
    def __init__(self, path, metadata, branching_factor, approx_block_size,
                 parallelism, compression="bz2", compress_kwargs={},
                 uuid=None, show_spinner=True, include_auto_metadata=True):
        self._path = path
        # Technically there is a race condition here, but oh well. This is
        # just a safety/sanity check; it's not worth going through the
        # contortions to use O_EXCL.
        if os.path.exists(path):
            raise ZSSError("%s: file already exists" % (path,))
        self._file = open(path, "w+b")
        self.metadata = dict(metadata)
        if include_auto_metadata:
            self.metadata.setdefault("build-user", getpass.getuser())
            self.metadata.setdefault("build-host", socket.getfqdn())
            self.metadata.setdefault("build-time",
                                     datetime.utcnow().isoformat() + "Z")
        self.branching_factor = branching_factor
        self.approx_block_size = approx_block_size
        self._show_spinner = show_spinner
        self._parallelism = parallelism
        self.compression = compression
        if self.compression not in codecs:
            raise ZSSError("unknown compression %r (should be one of: %s)"
                           % (compression, ", ".join(codecs)))
        self._compress_fn = codecs[self.compression][0]
        self._compress_kwargs = compress_kwargs
        if uuid is None:
            uuid = uuid4().bytes
        if not isinstance(uuid, six.binary_type):
            raise TypeError("uuid= must be str (on py2) or bytes (on py3)")
        if len(uuid) != 16:
            raise ValueError("uuid (if given) must be exactly 16 bytes")
        self.uuid = uuid
        self._header = {
            "root_index_offset": 2 ** 63 - 1,
            "root_index_length": 0,
            "uuid": uuid,
            "compression": self.compression,
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
        self._compressors = []
        for i in xrange(parallelism):
            compress_args = (self.approx_block_size,
                             self._compress_fn, self._compress_kwargs,
                             self._compress_queue, self._write_queue)
            p = multiprocessing.Process(target=_compress_worker,
                                        args=compress_args)
            p.start()
            self._compressors.append(p)
        writer_args = (self._path,
                       self.branching_factor,
                       self._compress_fn, self._compress_kwargs,
                       self._write_queue, self._finish_queue,
                       self._show_spinner)
        self._writer = multiprocessing.Process(target=_write_worker,
                                               args=writer_args)
        self._writer.start()

    def from_file(self, file_handle, terminator=b"\n"):
        partial_record = b""
        next_job = self._next_job
        read = file_handle.read
        compress_queue_put = self._compress_queue.put
        while True:
            buf = file_handle.read(self.approx_block_size)
            if not buf:
                # File should have ended with a newline (and we don't write
                # out the trailing empty record that this might imply).
                assert not partial_record
                self.close()
                return
            buf = partial_record + buf
            buf, partial_record = buf.rsplit(terminator, 1)
            compress_queue_put((next_job, "chunk-sep", buf, terminator))
            next_job += 1
        self._next_job = next_job

    def close(self):
        # Stop all the processing queues and wait for them to finish.
        sys.stderr.write("\rzss: Waiting for write thread to finish\n")
        for i in xrange(self._parallelism):
            self._compress_queue.put(_QUIT)
        for compressor in self._compressors:
            compressor.join()
        #sys.stdout.write("All compressors finished; waiting for writer\n")
        # All compressors have now finished their work, and submitted
        # everything to the write queue.
        self._write_queue.put(_QUIT)
        self._writer.join()
        sys.stderr.write("\rzss: All data written; updating header\n")
        root_index_offset, root_index_length = self._finish_queue.get()
        sys.stderr.write("zss: Root index offset: %s\n" % (root_index_offset,))
        # Now we have the root offset; write it to the header.
        self._header["root_index_offset"] = root_index_offset
        self._header["root_index_length"] = root_index_length
        new_encoded_header = _encode_header(self._header)
        self._file.seek(len(MAGIC))
        # Read the header length and make sure it hasn't changed
        old_length, = read_format(self._file, header_data_length_format)
        if old_length != len(new_encoded_header):
            raise ZSSError("header data length changed")
        self._file.write(new_encoded_header)
        self._file.write(encoded_crc32c(new_encoded_header))
        # Flush the file to disk to make sure that all data is consistent
        # before we mark the file as complete.
        _flush_file(self._file)
        # And now we can write the MAGIC value to mark the file as complete.
        self._file.seek(0)
        self._file.write(MAGIC)
        _flush_file(self._file)
        # Done!
        self._file.close()

    # Lack of a __del__ method is intentional -- if an error occurs, we want
    # to leave a file which is obviously incomplete, rather than create a file
    # which *looks* complete but isn't.

def _compress_worker(approx_block_size, compress_fn, compress_kwargs,
                     compress_queue, write_queue):
    # Local variables for speed
    get = compress_queue.get
    pdr = pack_data_records
    put = write_queue.put
    while True:
        job = get()
        #sys.stderr.write("compress_worker: got\n")
        if job is _QUIT:
            #sys.stderr.write("compress_worker: QUIT\n")
            return
        # XX FIXME should really have a second (slower) API where the records
        # are encoded in some way that allows for arbitrary contents... but
        # this will suffice for now.
        assert job[1] == "chunk-sep"
        idx, job_type, buf, sep = job
        records = buf.split(sep)
        data = pdr(records, 2 * approx_block_size)
        zdata = compress_fn(data, **compress_kwargs)
        #sys.stderr.write("compress_worker: putting\n")
        put((idx, records[0], records[-1], zdata))

def _write_worker(path, branching_factor,
                  compress_fn, compress_kwargs,
                  write_queue, finish_queue,
                  show_spinner):
    data_appender = _ZSSDataAppender(path, branching_factor,
                                     compress_fn, compress_kwargs)
    pending_jobs = {}
    wanted_job = 0
    get = write_queue.get
    write_block = data_appender.write_block
    while True:
        job = get()
        #sys.stderr.write("write_worker: got\n")
        if job is _QUIT:
            assert not pending_jobs
            root_offset, root_len = data_appender.close_and_get_root_offset()
            finish_queue.put((root_offset, root_len))
            return
        pending_jobs[job[0]] = job[1:]
        while wanted_job in pending_jobs:
            #sys.stderr.write("write_worker: writing %s\n" % (wanted_job,))
            write_block(0, *pending_jobs[wanted_job])
            if show_spinner and wanted_job % 100 == 0:
                sys.stderr.write(".")
            del pending_jobs[wanted_job]
            wanted_job += 1

# This class coordinates writing actual data blocks to the file, and also
# handles generating the index. The hope is that indexing has low enough
# overhead that handling it in serial with the actual writes won't create a
# bottleneck...
class _ZSSDataAppender(object):
    def __init__(self, path, branching_factor, compress_fn, compress_kwargs):
        self._file = open(path, "ab")
        # Opening in append mode should put us at the end of the file, but
        # just in case...
        self._file.seek(0, 2)
        assert self._file.tell() > 0

        self._branching_factor = branching_factor
        self._compress_fn = compress_fn
        self._compress_kwargs = compress_kwargs
        # For each level, a list of entries
        # each entry is a tuple (first_record, last_record, offset)
        # last_record is kept around to ensure that records at each level are
        # sorted and non-overlapping, and because in principle we could use
        # them to find shorter keys (XX).
        self._level_entries = []
        self._level_lengths = []

    def write_block(self, level, first_record, last_record, zdata):
        if not (0 <= level <= MAX_LEVEL):
            raise ZSSError("invalid level %s" % (level,))

        block_offset = self._file.tell()
        block_contents = six.int2byte(level) + zdata
        write_uleb128(len(block_contents), self._file)
        self._file.write(block_contents)
        self._file.write(encoded_crc32c(block_contents))
        total_block_length = self._file.tell() - block_offset

        if level >= len(self._level_entries):
            # First block we've seen at this level
            assert level == len(self._level_entries)
            self._level_entries.append([])
            # This can only happen if all the previous levels just flushed.
            for i in xrange(level):
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
        for i in xrange(1, len(entries)):
            if entries[i][0] < entries[i - 1][1]:
                raise ZSSError("non-sorted spans")
        keys = [entry[0] for entry in entries]
        offsets = [entry[2] for entry in entries]
        block_lengths = [entry[3] for entry in entries]
        data = pack_index_records(keys, offsets, block_lengths,
                                  # Just a random guess at average record size
                                  # Doesn't have to be accurate, just reduces
                                  # reallocs if it is.
                                  self._branching_factor * 300)
        zdata = self._compress_fn(data, **self._compress_kwargs)
        first_record = entries[0][0]
        last_record = entries[-1][1]
        self.write_block(level + 1, first_record, last_record, zdata)

    def close_and_get_root_offset(self):
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

        while not have_root():
            for level in xrange(MAX_LEVEL):
                if self._level_entries[level]:
                    self._flush_index(level)
                    break
        _flush_file(self._file)
        self._file.close()
        root_entry = self._level_entries[-1][0]
        return root_entry[-2:]
        assert False
