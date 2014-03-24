# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import os
import os.path

from six import int2byte, byte2int, BytesIO
from nose.tools import assert_raises

from .test import test_data_path
from zss import ZSS
import zss.common

# XX provide some export that test_http can use
# or, import from test_http and run tests

# letters.zss contains records:
#   [b, bb, d, dd, f, ff, ..., z, zz]

letters_records = []
for i in xrange(1, 26, 2):
    letter = int2byte(byte2int(b"a") + i)
    letters_records += [letter, 2 * letter]

def test_zss():
    for codec in zss.common.codecs:
        p = test_data_path("letters-%s.zss" % (codec,))
        for parallelism in [1, 2, "auto"]:
            with ZSS(p, parallelism=parallelism) as z:
                check_letters_zss(z, codec)
    # XX FIXME: web as well (in another test, b/c might skip that one)

def _check_map_helper(records, arg1, arg2):
    assert arg1 == 1
    assert arg2 == 2
    return records

def _check_raise_helper(records, exc=None):
    if exc is not None:
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

# next:
# - add real fsck tests
# - add a test to writer that just writes, checks metadata, checks contents,
#   and calls fsck()
# - make http work
# - check coverage
# - docs
# - add some close/context manager functionality tests
