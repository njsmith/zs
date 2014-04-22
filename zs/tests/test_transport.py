# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

from nose.tools import assert_raises
from requests import HTTPError

from zs import ZSError
from .util import test_data_path
from .http_harness import web_server, simplehttpserver
from ..transport import FileTransport, HTTPTransport

contents = open(test_data_path("transport-test/alphabet"), "rb").read()

def check_transport(t):
    assert isinstance(t.name, str)

    assert t.length() == len(contents)

    assert t.chunk_read(0, 2) == contents[0:2]
    assert t.chunk_read(2, 2) == contents[2:4]
    # partial reads are okay
    assert t.chunk_read(0, 100) == contents

    s = t.stream_read(0)
    assert s.tell() == 0
    assert s.read(1) == contents[0:1]
    assert s.tell() == 1
    assert s.read(1) == contents[1:2]
    assert s.tell() == 2
    assert s.read(4) == contents[2:6]
    assert s.tell() == 6
    assert s.read(len(contents) - 6) == contents[6:]
    s.close()

    s = t.stream_read(10)
    assert s.tell() == 10
    assert s.read(2) == contents[10:12]
    assert s.tell() == 12
    s.close()

    s = t.stream_read(10, 12)
    assert s.tell() == 10
    assert s.read(1) == contents[10:11]
    assert s.read(1) == contents[11:12]
    # might not continue past here, that's okay

    t.close()

def test_FileTransport():
    check_transport(FileTransport(test_data_path("transport-test/alphabet")))
    assert_raises(IOError, FileTransport, test_data_path("SDFASDFASDFAS"))

def test_HTTPTransport():
    with web_server(test_data_path("transport-test")) as base_url:
        url = base_url + "/alphabet"
        check_transport(HTTPTransport(url))
        non_existent = HTTPTransport(url + "ASDFASDFASDF")
        assert_raises(HTTPError, non_existent.chunk_read, 0, 1)

        # check that HTTP streaming respects stop
        ht = HTTPTransport(url)
        s = ht.stream_read(5, 10)
        assert s.read(3) == contents[5:8]
        assert s.read(2) == contents[8:10]
        assert s.read(3) == b""

        # check that length() works properly both before and after calling the
        # various methods, to exercise the clever caching logic
        assert HTTPTransport(url).length() == len(contents)

        ht = HTTPTransport(url)
        ht.chunk_read(5, 8)
        assert ht.length() == len(contents)

        ht = HTTPTransport(url)
        ht.stream_read(5, 8)
        assert ht.length() == len(contents)

    with web_server(test_data_path("transport-test"),
                    range_support=False) as base_url:
        url = base_url + "/alphabet"
        ht = HTTPTransport(url)
        assert_raises(ZSError, ht.chunk_read, 10, 1)
        assert_raises(ZSError, ht.stream_read, 10)
