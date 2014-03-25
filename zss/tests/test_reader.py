# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path

from six import int2byte, byte2int, BytesIO
from nose.tools import assert_raises

from .util import test_data_path
from .http_harness import web_server, simplehttpserver
from zss import ZSS, ZSSError, ZSSCorrupt
import zss.common

# letters.zss contains records:
#   [b, bb, d, dd, f, ff, ..., z, zz]
letters_records = []
for i in xrange(1, 26, 2):
    letter = int2byte(byte2int(b"a") + i)
    letters_records += [letter, 2 * letter]

def _check_map_helper(records, arg1, arg2):
    assert arg1 == 1
    assert arg2 == 2
    return records

def _check_raise_helper(records, exc):
    raise exc

def check_letters_zss(z, codec):
    assert z.compression == codec
    assert z.uuid == (b"\x00\x01\x02\x03\x04\x05\x06\x07"
                      b"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f")
    assert z.metadata == {
        u"test-data": u"letters",
        u"build-user": u"test-user",
        u"build-host": u"test-host",
        u"build-time": u"2000-01-01T00:00:00.000000Z",
        }

    assert list(z) == letters_records
    assert list(z.search()) == letters_records

    if "ZSS_QUICK_TEST" in os.environ:
        chars = "m"
    else:
        chars = "abcdefghijklmnopqrstuvwxyz"
    for char in chars:
        byte = char.encode("ascii")
        for (start, stop, prefix) in [
                (None, None, None),
                (byte, None, None),
                (None, byte, None),
                (None, None, byte),
                (byte, byte, None),
                (byte, int2byte(byte2int(byte) + 1), None),
                (byte, int2byte(byte2int(byte) + 2), None),
                (byte, int2byte(byte2int(byte) + 3), None),
                (byte, b"q", None),
                (None, 2 * byte, byte),
                (b"m", b"s", byte),
                ]:
            expected = letters_records
            if start is not None:
                expected = [r for r in expected if r >= start]
            if stop is not None:
                expected = [r for r in expected if not r >= stop]
            if prefix is not None:
                expected = [r for r in expected if r.startswith(prefix)]
            assert list(z.search(start=start, stop=stop, prefix=prefix)
                        ) == expected

             # sloppy_block_search guarantees that it will return a superset
             # of records, that it will never return a block which is entirely
             # >= stop, and that it will return at most 1 block that contains
             # anything that's *not* >= start.
            sloppy_blocks = list(z.sloppy_block_search(start=start,
                                                       stop=stop,
                                                       prefix=prefix))
            # bit of a kluge, but the .search() tests up above do exhaustive
            # testing of norm_search_args, so at least it shouldn't invalidate
            # the testing:
            norm_start, norm_stop = z._norm_search_args(start, stop, prefix)
            contains_start_slop = 0
            sloppy_records = set()
            for records in sloppy_blocks:
                if records[0] < norm_start:
                    contains_start_slop += 1
                assert norm_stop is None or records[0] < norm_stop
                sloppy_records.update(records)
            assert contains_start_slop <= 1
            assert sloppy_records.issuperset(expected)

            sloppy_map_blocks = list(z.sloppy_block_map(
                _check_map_helper,
                # test args and kwargs argument passing
                args=(1,), kwargs={"arg2": 2},
                start=start, stop=stop, prefix=prefix))
            assert sloppy_map_blocks == sloppy_blocks

            for term in [b"\n", b"\x00"]:
                expected_dump = term.join(expected + [""])
                out = BytesIO()
                z.dump(out, start=start, stop=stop, prefix=prefix,
                       terminator=term)
                assert out.getvalue() == expected_dump

    assert list(z.search(stop=b"bb", prefix=b"b")) == [b"b"]

    assert_raises(ValueError, list,
                  z.sloppy_block_map(_check_raise_helper, args=(ValueError,)))
    assert_raises(ValueError, z.sloppy_block_exec,
                  _check_raise_helper, args=(ValueError,))

    z.fsck()

def test_zss():
    for codec in zss.common.codecs:
        p = test_data_path("letters-%s.zss" % (codec,))
        for parallelism in [0, 2, "auto"]:
            with ZSS(path=p, parallelism=parallelism) as z:
                check_letters_zss(z, codec)

# This is much slower, and the above test will have already exercised most of
# the tricky code, so we make this test less exhaustive.
def test_http_zss():
    with web_server(test_data_path()) as root_url:
        codec = "bz2"
        url = "%s/letters-%s.zss" % (root_url, codec)
        for parallelism in [0, 2]:
            with ZSS(url=url, parallelism=parallelism) as z:
                check_letters_zss(z, codec)

def test_http_notices_lack_of_range_support():
    with simplehttpserver(test_data_path()) as root_url:
        codec = "bz2"
        url = "%s/letters-%s.zss" % (root_url, codec)
        assert_raises(ZSSError, lambda: list(ZSS(url=url)))

def test_zss_args():
    p = test_data_path("letters-none.zss")
    # can't pass both path and url
    assert_raises(ValueError, ZSS, path=p, url="x")
    # parallelism must be >= 0
    assert_raises(ValueError, ZSS, path=p, parallelism=-1)

def test_zss_close():
    z = ZSS(test_data_path("letters-none.zss"))
    z.close()
    for call in [[list, z.search()],
                 [list, z.sloppy_block_search()],
                 [list,
                  z.sloppy_block_map(_check_raise_helper, AssertionError)],
                 [list, z],
                 [z.dump, BytesIO()],
                 [z.fsck],
                 ]:
        print(repr(call))
        assert_raises(ZSSError, *call)

def test_context_manager_closes():
    with ZSS(test_data_path("letters-none.zss")) as z:
        assert list(z.search()) == letters_records
    assert_raises(ZSSError, list, z.search())

def test_sloppy_block_exec():
    # This function tricky to test in a multiprocessing world, because we need
    # some way to communicate back from the subprocesses that the execution
    # actually happened... instead we just test it in serial
    # mode. (Fortunately it is a super-trivial function.)
    z = ZSS(test_data_path("letters-none.zss"), parallelism=0)
    # b/c we're in serial mode, the fn doesn't need to be pickleable
    class CountBlocks(object):
        def __init__(self):
            self.count = 0
        def __call__(self, records):
            self.count += 1
    count_blocks = CountBlocks()
    z.sloppy_block_exec(count_blocks)
    assert count_blocks.count > 1
    assert count_blocks.count == len(list(z.sloppy_block_search()))

def test_big_headers():
    from zss.reader import _lower_header_size_guess
    with _lower_header_size_guess():
        z = ZSS(test_data_path("letters-none.zss"))
        assert z.compression == "none"
        assert z.uuid == (b"\x00\x01\x02\x03\x04\x05\x06\x07"
                          b"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f")
        assert z.metadata == {
            u"test-data": u"letters",
            u"build-user": u"test-user",
            u"build-host": u"test-host",
            u"build-time": u"2000-01-01T00:00:00.000000Z",
        }
        assert list(z) == letters_records

def test_broken_files():
    def open_and_read(p):
        list(ZSS(p))
    def open_and_fsck(p):
        ZSS(p).fsck()
    # Files that should fail even on casual use (no fsck)
    for basename in ["partial-root", "bad-magic", "incomplete-magic",
                     "header-checksum", "root-checksum", "bad-codec",
                     "non-dict-metadata",
                     ]:
        p = test_data_path("broken-files/%s.zss" % (basename,))
        assert_raises(ZSSCorrupt, open_and_read, p)