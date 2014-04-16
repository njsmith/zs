# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path
import hashlib

from six import int2byte, byte2int, BytesIO
from nose.tools import assert_raises

from .util import test_data_path
from .http_harness import web_server
from zss import ZSS, ZSSError, ZSSCorrupt
import zss.common
from zss._zss import pack_data_records

# letters.zss contains records:
#   [b, bb, d, dd, f, ff, ..., z, zz]
letters_records = []
for i in xrange(1, 26, 2):
    letter = int2byte(byte2int(b"a") + i)
    letters_records += [letter, 2 * letter]

letters_sha256 = hashlib.sha256(pack_data_records(letters_records, 1)).digest()

def _check_map_helper(records, arg1, arg2):
    assert arg1 == 1
    assert arg2 == 2
    return records

def _check_raise_helper(records, exc):
    raise exc

def check_letters_zss(z, codec):
    assert z.compression == codec
    assert z.data_sha256 == letters_sha256
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
            print("start=%r, stop=%r, prefix=%r" % (start, stop, prefix))
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

    z.validate()

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
    with web_server(test_data_path(), range_support=False) as root_url:
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
                 [z.validate],
                 ]:
        print(repr(call))
        assert_raises(ZSSError, *call)
    # But calling .close() twice is fine.
    z.close()

    # smoke test for __del__ method
    ZSS(test_data_path("letters-none.zss"))

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
        assert z.data_sha256 == letters_sha256
        assert z.metadata == {
            u"test-data": u"letters",
            u"build-user": u"test-user",
            u"build-host": u"test-host",
            u"build-time": u"2000-01-01T00:00:00.000000Z",
        }
        assert list(z) == letters_records

def test_broken_files():
    import glob
    unchecked_paths = set(glob.glob(test_data_path("broken-files/*.zss")))
    # Files that should fail even on casual use (no validate)
    for basename, msg_fragment in [
            ("short-root", "partial read"),
            ("truncated-root", "unexpected EOF"),
            ("bad-magic", "bad magic"),
            ("incomplete-magic", "partially written"),
            ("header-checksum", "header checksum"),
            ("root-checksum", "checksum mismatch"),
            ("bad-codec", "unrecognized compression"),
            ("non-dict-metadata", "bad metadata"),
            ("truncated-data-1", "unexpectedly ran out of data"),
            ("truncated-data-2", "unexpected EOF"),
            ("truncated-data-3", "unexpected EOF"),
            ("wrong-root-offset", "checksum mismatch"),
            ("root-is-data", "expecting index block"),
            ("wrong-root-level-1", "expecting index block"),
            ("partial-data-1", "past end of block"),
            ("partial-data-2", "end of buffer"),
            ("empty-data", "empty block"),
            ("partial-index-1", "end of buffer"),
            ("partial-index-2", "end of buffer"),
            ("partial-index-3", "past end of block"),
            ("partial-index-4", "past end of block"),
            ("empty-index", "empty block"),
            ("bad-total-length", "header says it should"),
            ("bad-level-root", "extension block"),
            ("bad-level-index", "extension block"),
            ]:
        print(basename)
        # to prevent accidental false success:
        assert msg_fragment not in basename
        p = test_data_path("broken-files/%s.zss" % (basename,))
        with assert_raises(ZSSCorrupt) as cm:
            with ZSS(p) as z:
                list(z)
        assert msg_fragment in str(cm.exception)
        with assert_raises(ZSSCorrupt) as cm:
            with ZSS(p) as z:
                z.validate()
        assert msg_fragment in str(cm.exception)
        unchecked_paths.discard(p)

    # Files that might look okay locally, but validate should detect problems
    for basename, msg_fragment in [
            ("unref-data", "unreferenced"),
            ("unref-index", "unreferenced"),
            ("wrong-root-length", "root index length"),
            ("wrong-root-level-2", "level 3 to level 1"),
            ("repeated-index", "multiple ref"),
            ("bad-ref-length", "!= actual length"),
            ("bad-index-order", "unsorted offsets"),
            ("bad-index-order", "unsorted records"),
            ("bad-data-order", "unsorted records"),
            ("bad-index-key-1", "too large for block"),
            ("bad-index-key-2", "too small for block"),
            ("bad-index-key-3", "too small for block"),
            ("bad-sha256", "data hash mismatch"),
            ]:
        print(basename)
        # to prevent accidental false success:
        assert msg_fragment not in basename
        p = test_data_path("broken-files/%s.zss" % (basename,))
        with ZSS(p) as z:
            with assert_raises(ZSSCorrupt) as cm:
                z.validate()
        assert msg_fragment in str(cm.exception)
        unchecked_paths.discard(p)

    # Files that are a bit tricky, but should in fact be okay
    for basename in [
            "good-index-key-1",
            "good-index-key-2",
            "good-index-key-3",
            "good-extension-blocks",
            "good-extension-header-fields",
            ]:
        print(basename)
        p = test_data_path("broken-files/%s.zss" % (basename,))
        with ZSS(p) as z:
            list(z)
            z.validate()
        unchecked_paths.discard(p)

    assert not unchecked_paths

def test_extension_blocks():
    # Check that the reader happily skips over the extension blocks in the
    # middle of the file.
    with ZSS(test_data_path("broken-files/good-extension-blocks.zss")) as z:
        assert list(z) == ["a", "b", "c", "d"]

def test_ref_loops():
    # Had a bunch of trouble eliminating reference loops in the ZSS object.
    with ZSS(test_data_path("letters-none.zss")) as z:
        # 1 for 'z', one for the temporary psased to sys.getrefcount
        assert sys.getrefcount(z) == 2
        list(z)
        assert sys.getrefcount(z) == 2
