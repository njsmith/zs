# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# Transports couple the ZS reader to a data source.

import os
import re

from six import BytesIO
import requests

from .common import ZSError

class FileTransport(object):
    remote = False

    def __init__(self, path):
        self._file = open(path, "rb")
        # To include in user-directed error messages etc.
        self.name = path

    # This allows partial reads (i.e., if EOF falls in the middle of the
    # requested chunk, then we return the part before the EOF). Fortunately,
    # this is how both normal Python read() and how HTTP Range work
    # out-of-the-box.
    def chunk_read(self, offset, length):
        self._file.seek(offset)
        return self._file.read(length)

    # Returns a file-like object which will return bytes from the given
    # position. 'stop_offset', if given, is a hint -- the returned file-like
    # object may or may not EOF after reaching this point.
    def stream_read(self, offset, stop_offset=None):
        new_file = os.fdopen(os.dup(self._file.fileno()), "rb")
        new_file.seek(offset)
        return new_file

    def length(self):
        stat = os.fstat(self._file.fileno())
        return stat.st_size

    def close(self):
        self._file.close()

class HTTPTransport(object):
    remote = True

    def __init__(self, url):
        self._url = url
        self.name = url
        self._length = None

    _crange_re = re.compile(r"^bytes (\d+)-\d+/(\d+|\*)")
    def _check_offset(self, response, desired_offset):
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.16
        # Content-Range tells you what data you actually got, and looks like:
        #   "bytes X-Y/Z"
        # or
        #   "bytes */Z"
        # where X & Y are integers, and Z is either an integer or "*"
        # The second form is only allowed on error responses.
        crange = response.headers.get("Content-Range", "")
        match = self._crange_re.match(crange)
        if not match:
            offset = 0
        else:
            offset = int(match.group(1))
        if offset != desired_offset:
            raise ZSError("HTTP server did not respect Range: request")
        if match and match.group(2) != "*":
            self._length = int(match.group(2))

    def chunk_read(self, offset, length):
        # -1 because Range: is inclusive
        headers = {"Range": "bytes=%s-%s" % (offset, offset + length - 1)}
        response = requests.get(self._url, headers=headers)
        # if we got an error response, raise an exception
        response.raise_for_status()
        self._check_offset(response, offset)
        # .content is the byte (not text) version of the response
        return response.content

    def stream_read(self, offset, stop_offset=None):
        if stop_offset is None:
            stop_offset = ""
        else:
            stop_offset -= 1
            if stop_offset < offset:
                # server will just return 416, Requested range not satisfiable
                return BytesIO(b"")
        # Limited range is "100-200", endless range is "100-".
        headers = {"Range": "bytes=%s-%s" % (offset, stop_offset)}
        response = requests.get(self._url, headers=headers, stream=True)
        response.raise_for_status()
        self._check_offset(response, offset)
        return _HTTPStream(offset, response)

    def length(self):
        if self._length is None:
            response = requests.head(self._url)
            self._length = int(response.headers["Content-Length"])
        return self._length

    def close(self):
        pass

class _HTTPStream(object):
    def __init__(self, offset, response):
        self._response = response
        self._offset = offset

    def tell(self):
        return self._offset

    def read(self, length):
        for chunk in self._response.iter_content(length):
            self._offset += len(chunk)
            return chunk
        return b""

    def close(self):
        self._response.close()
