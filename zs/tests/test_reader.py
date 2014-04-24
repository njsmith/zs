# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path
import sys
import hashlib

from six import int2byte, byte2int, BytesIO, integer_types
from nose.tools import assert_raises

from .util import test_data_path
from .http_harness import web_server
from zs import ZS, ZSError, ZSCorrupt
from zs._zs import pack_data_records
from zs.common import read_length_prefixed, codec_shorthands

# letters.zs contains records:
#   [b, bb, d, dd, f, ff, ..., z, zz]
letters_records = []
for i in range(1, 26, 2):
    letter = int2byte(byte2int(b"a") + i)
    letters_records += [letter, 2 * letter]

letters_sha256 = hashlib.sha256(pack_data_records(letters_records)).digest()

def identity(x):
    return x

def _check_map_helper(records, arg1, arg2):
    assert arg1 == 1
    assert arg2 == 2
    return records

def _check_raise_helper(records, exc):
    raise exc

def check_letters_zs(z, codec_shorthand):
    assert isinstance(z.root_index_offset, integer_types)
    assert isinstance(z.root_index_length, integer_types)
    assert isinstance(z.total_file_length, integer_types)
    assert z.codec == codec_shorthands[codec_shorthand]
    assert z.data_sha256 == letters_sha256
    assert z.metadata == {
        u"test-data": u"letters",
        u"build-info": {
            u"user": u"test-user",
            u"host": u"test-host",
            u"time": u"2000-01-01T00:00:00.000000Z",
            u"version": u"zs test",
            },
        }
    assert isinstance(z.root_index_level, integer_types)

    assert list(z) == letters_records
    assert list(z.search()) == letters_records

    if "ZS_QUICK_TEST" in os.environ:
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

            map_blocks = list(z.block_map(
                _check_map_helper,
                # test args and kwargs argument passing
                args=(1,), kwargs={"arg2": 2},
                start=start, stop=stop, prefix=prefix))
            assert sum(map_blocks, []) == expected

            for term in [b"\n", b"\x00"]:
                expected_dump = term.join(expected + [b""])
                out = BytesIO()
                z.dump(out, start=start, stop=stop, prefix=prefix,
                       terminator=term)
                assert out.getvalue() == expected_dump
            out = BytesIO()
            z.dump(out, start=start, stop=stop, prefix=prefix,
                   length_prefixed="uleb128")
            assert (list(read_length_prefixed(BytesIO(out.getvalue()), "uleb128"))
                    == expected)
            out = BytesIO()
            z.dump(out, start=start, stop=stop, prefix=prefix,
                   length_prefixed="u64le")
            assert (list(read_length_prefixed(BytesIO(out.getvalue()), "u64le"))
                    == expected)

    assert list(z.search(stop=b"bb", prefix=b"b")) == [b"b"]

    assert_raises(ValueError, list,
                  z.block_map(_check_raise_helper, args=(ValueError,)))
    assert_raises(ValueError, z.block_exec,
                  _check_raise_helper, args=(ValueError,))

    z.validate()

def test_zs():
    for codec in codec_shorthands:
        p = test_data_path("letters-%s.zs" % (codec,))
        for parallelism in [0, 2, "guess"]:
            with ZS(path=p, parallelism=parallelism) as z:
                check_letters_zs(z, codec)

# This is much slower, and the above test will have already exercised most of
# the tricky code, so we make this test less exhaustive.
def test_http_zs():
    with web_server(test_data_path()) as root_url:
        codec = "bz2"
        url = "%s/letters-%s.zs" % (root_url, codec)
        for parallelism in [0, 2]:
            with ZS(url=url, parallelism=parallelism) as z:
                check_letters_zs(z, codec)

def test_http_notices_lack_of_range_support():
    with web_server(test_data_path(), range_support=False) as root_url:
        codec = "bz2"
        url = "%s/letters-%s.zs" % (root_url, codec)
        assert_raises(ZSError, lambda: list(ZS(url=url)))

def test_zs_args():
    p = test_data_path("letters-none.zs")
    # can't pass both path and url
    assert_raises(ValueError, ZS, path=p, url="x")
    # parallelism must be >= 0
    assert_raises(ValueError, ZS, path=p, parallelism=-1)

def test_zs_close():
    z = ZS(test_data_path("letters-none.zs"))
    z.close()
    for call in [[list, z.search()],
                 [list,
                  z.block_map(_check_raise_helper, AssertionError)],
                 [list, z],
                 [z.dump, BytesIO()],
                 [z.validate],
                 ]:
        print(repr(call))
        assert_raises(ZSError, *call)
    # But calling .close() twice is fine.
    z.close()

    # smoke test for __del__ method
    ZS(test_data_path("letters-none.zs"))

def test_context_manager_closes():
    with ZS(test_data_path("letters-none.zs")) as z:
        assert list(z.search()) == letters_records
    assert_raises(ZSError, list, z.search())

def test_block_exec():
    # This function tricky to test in a multiprocessing world, because we need
    # some way to communicate back from the subprocesses that the execution
    # actually happened... instead we just test it in serial
    # mode. (Fortunately it is a super-trivial function.)
    z = ZS(test_data_path("letters-none.zs"), parallelism=0)
    # b/c we're in serial mode, the fn doesn't need to be pickleable
    class CountBlocks(object):
        def __init__(self):
            self.count = 0
        def __call__(self, records):
            self.count += 1
    count_blocks = CountBlocks()
    z.block_exec(count_blocks)
    assert count_blocks.count > 1
    assert count_blocks.count == len(list(z.block_map(identity)))

def test_big_headers():
    from zs.reader import _lower_header_size_guess
    with _lower_header_size_guess():
        z = ZS(test_data_path("letters-none.zs"))
        assert z.codec == "none"
        assert z.data_sha256 == letters_sha256
        assert z.metadata == {
            u"test-data": u"letters",
            u"build-info": {
                u"user": u"test-user",
                u"host": u"test-host",
                u"time": u"2000-01-01T00:00:00.000000Z",
                u"version": u"zs test",
                },
            }
        assert list(z) == letters_records

def test_broken_files():
    import glob
    unchecked_paths = set(glob.glob(test_data_path("broken-files/*.zs")))
    # Files that should fail even on casual use (no validate)
    for basename, msg_fragment in [
            ("short-root", ["partial read", "root index length"]),
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
            ("wrong-root-offset", ["checksum mismatch", "root block missing"]),
            ("root-is-data", ["expecting index block", "bad level"]),
            ("wrong-root-level-1", ["expecting index block", "bad index ref"]),
            ("partial-data-1", "past end of block"),
            ("partial-data-2", "end of buffer"),
            ("empty-data", "empty block"),
            ("partial-index-1", "end of buffer"),
            ("partial-index-2", "end of buffer"),
            ("partial-index-3", "past end of block"),
            ("partial-index-4", "past end of block"),
            ("empty-index", "empty block"),
            ("bad-total-length", "header says it should"),
            ("bad-level-root", ["extension block", "root block missing"]),
            ("bad-level-index-2", ["extension block", "dangling or multiple refs"]),
            ("post-header-junk", "checksum mismatch"),
            ]:
        print(basename)
        def any_match(mfs, haystack):
            if isinstance(mfs, str):
                mfs = [mfs]
            for mf in mfs:
                if mf in haystack:
                    return True
            return False
        # to prevent accidental false success:
        assert not any_match(msg_fragment, basename)
        p = test_data_path("broken-files/%s.zs" % (basename,))
        with assert_raises(ZSCorrupt) as cm:
            with ZS(p) as z:
                list(z)
                # use start= to ensure that we do an index traversal
                list(z.search(start=b"\x00"))
        assert any_match(msg_fragment, str(cm.exception))
        with assert_raises(ZSCorrupt) as cm:
            with ZS(p) as z:
                z.validate()
        assert any_match(msg_fragment, str(cm.exception))
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
            # not really an accurate message -- this file has a level 1 index
            # pointing to an extension block. the reader doesn't blow up at
            # this because it knows that below a level 1 index is data and
            # switches to streaming read, and then streaming read ignores
            # extension blocks, so only fsck() will catch it. And fsck() uses
            # a streaming read so extension blocks are invisible to it, and
            # all it sees is that there's this reference pointing into an
            # invisible hole in space, which looks like a dangling reference.
            ("bad-level-index-1", "dangling"),
            ]:
        print(basename)
        # to prevent accidental false success:
        assert msg_fragment not in basename
        p = test_data_path("broken-files/%s.zs" % (basename,))
        with ZS(p) as z:
            with assert_raises(ZSCorrupt) as cm:
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
        p = test_data_path("broken-files/%s.zs" % (basename,))
        with ZS(p) as z:
            list(z)
            z.validate()
        unchecked_paths.discard(p)

    assert not unchecked_paths

def test_extension_blocks():
    # Check that the reader happily skips over the extension blocks in the
    # middle of the file.
    with ZS(test_data_path("broken-files/good-extension-blocks.zs")) as z:
        assert list(z) == [b"a", b"b", b"c", b"d"]

def test_ref_loops():
    # Had a bunch of trouble eliminating reference loops in the ZS object.
    # Don't use 'with' statement here b/c that keeps another ref which just
    # confuses things.
    z = ZS(test_data_path("letters-none.zs"))
    try:
        # 1 for 'z', one for the temporary passed to sys.getrefcount
        print(sys.getrefcount(z))
        assert sys.getrefcount(z) == 2
        list(z)
        assert sys.getrefcount(z) == 2
    finally:
        z.close()
