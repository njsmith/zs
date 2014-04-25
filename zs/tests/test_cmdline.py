# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

import sys
import subprocess
import os
import os.path
from collections import namedtuple
from contextlib import contextmanager
import json
import binascii
from unittest.case import SkipTest
import random

from six import BytesIO

import zs
from zs import ZS, ZSWriter
from .util import tempname, test_data_path
from .http_harness import web_server

RunResult = namedtuple("RunResult", ["returncode", "stdout", "stderr"])

# To let the test suite run without installing, we only try the '-m zs'
# version, not the 'zs' script itself. .travis.yml has a little test to make
# sure that setup.py does install the 'zs' script.
CMD = [sys.executable, "-m", "zs"]

def run(args, expected_returncode=0, input=b"", zs_cmd=CMD):
    p = subprocess.Popen(zs_cmd + args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(input=input)
    returncode = p.wait()
    try:
        if expected_returncode is not None:
            assert expected_returncode == returncode
    except:
        print("args: %r" % (args,))
        print("stdout: %r" % (stdout,))
        print("stderr: %r" % (stderr,))
        print("returncode: %r" % (returncode,))
        raise
    return RunResult(returncode, stdout, stderr)

RECORDS = [b"", b"a", b"b", b"bb", b"c"]
NEWLINE_RECORDS = b"\n".join(RECORDS + [b""])

@contextmanager
def simple_zs(records=RECORDS):
    with tempname(".zs", unlink_first=True) as p:
        with ZSWriter(p, {"temp": 1}, 2) as zw:
            zw.add_data_block(records)
            zw.finish()
        yield p

def test_basic():
    assert b"--help" in run(["--asdf"], expected_returncode=2).stderr
    assert b"--help" in run(["asdf"], expected_returncode=2).stderr
    assert b"subcommands" in run(["--help"]).stdout

def test_dump():
    with simple_zs() as p:
        assert run(["dump", p]).stdout == NEWLINE_RECORDS
        assert run(["dump", p, "--prefix=b"]).stdout == b"b\nbb\n"
        assert run(["dump", p, "--prefix=z"]).stdout == b""
        assert run(["dump", p, "--start=a", "--stop=b"]).stdout == b"a\n"

        # smoke test for -j and -o
        with tempname(".txt") as p2:
            assert run(["dump", "-j", "3", "-o", p2, p]).stdout == b""
            assert open(p2, "rb").read() == NEWLINE_RECORDS

        assert (run(["dump", p, "--terminator=\\r\\n"]).stdout
                == NEWLINE_RECORDS.replace(b"\n", b"\r\n"))
        assert (run(["dump", p, "--terminator=\\x00"]).stdout
                == NEWLINE_RECORDS.replace(b"\n", b"\x00"))

        assert (run(["dump", p, "--length-prefixed=uleb128"]).stdout
                == b"\x00\x01a\x01b\x02bb\x01c")
        assert (run(["dump", p,
                     "--length-prefixed=u64le", "--prefix=b"]).stdout
                == (b"\x01\x00\x00\x00\x00\x00\x00\x00b"
                    b"\x02\x00\x00\x00\x00\x00\x00\x00bb"))

        # make sure --length-prefixed is validated
        run(["dump", p, "--length-prefixed=asdf"],
            expected_returncode=2)

        # ditto for -j
        run(["dump", p, "-j", "asdf"], expected_returncode=2)

    with simple_zs([b"\x00", b"\x01", b"\x01a", b"\x02"]) as p:
        assert (run(["dump", p, "--length-prefixed=uleb128"]).stdout
                == b"\x01\x00\x01\x01\x02\x01a\x01\x02")
        assert (run(["dump", p,
                     "--length-prefixed=uleb128",
                     "--prefix=\\x01"]).stdout
                == b"\x01\x01\x02\x01a")

def test_validate():
    run(["validate", test_data_path("letters-none.zs")])

    r = run(["validate", test_data_path("broken-files/unref-data.zs")],
            expected_returncode=1)
    assert b"unreferenced" in r.stdout

def test_info():
    with simple_zs() as p:
        out = run(["info", p])
        info = json.loads(out.stdout.decode("ascii"))
        with ZS(p) as z:
            assert info["codec"] == z.codec
            assert binascii.unhexlify(info["data_sha256"]) == z.data_sha256
            assert info["metadata"] == z.metadata

        just_metadata = json.loads(run(["info", p, "--metadata-only"]).stdout
                                   .decode("ascii"))
        assert info["metadata"] == just_metadata

def test_urls():
    with web_server(test_data_path()) as root_url:
        url = root_url + "/letters-none.zs"
        path = test_data_path("letters-none.zs")

        assert run(["dump", url]).stdout == run(["dump", path]).stdout
        run(["validate", url])
        # can't compare stdout directly b/c of dict randomization
        url_info = json.loads(run(["info", url]).stdout.decode("ascii"))
        path_info = json.loads(run(["info", path]).stdout.decode("ascii"))
        assert url_info == path_info

def nothing(x):
    return None

def test_make():
    from .test_writer import records as big_records, temp_zs_path

    with simple_zs(big_records) as p_in:
        for format_opt in ["--terminator=\\n",
                           "--terminator=\\x00",
                           "--length-prefixed=uleb128",
                           "--length-prefixed=u64le",
                          ]:
            input = run(["dump", p_in, format_opt]).stdout
            with temp_zs_path() as p_out:
                run(["make", format_opt, "{}", "-", p_out], input=input)

                with ZS(p_out) as z:
                    z.validate()
                    assert list(z) == big_records

        big_input = b"\n".join(big_records + [b""])

        # smoke test -j
        with temp_zs_path() as p_out:
            run(["make", "{}", "-", p_out, "-j", "3"], input=NEWLINE_RECORDS)

        # --no-spinner produces less chatter
        with temp_zs_path() as p_out:
            r1 = run(["make", "{}", "-", p_out], input=big_input)
        with temp_zs_path() as p_out:
            r2 = run(["make", "{}", "-", p_out, "--no-spinner"],
                     input=big_input)
        assert len(r2.stdout) < len(r1.stdout)

        # codecs and compress level
        # we need some non-trivial input (so the compression algorithms have
        # some work to do), that's large enough for things like the bz2 window
        # size to make a difference.
        r = random.Random(0)
        scrambled_letters = "".join(r.sample("abcdefghijklmnopqrstuvwxyz", 26))
        scrambled_letters = scrambled_letters.encode("ascii")
        pieces = []
        for i in range(200000):
            low = r.randrange(25)
            high = r.randrange(low, 26)
            pieces.append(scrambled_letters[low:high])
        # put a really big piece in to make long-distance memory matter more
        pieces.append(b"m" * 2 ** 18)
        pieces.sort()
        pieces.append(b"")
        bigger_input = b"\n".join(pieces)

        sizes = {}
        for settings in ["--codec=none",
                         "--codec=bz2",
                         "--codec=bz2 -z 1",
                         "--codec=deflate",
                         "--codec=deflate --compress-level 1",
                         "--codec=lzma",
                         "--codec=lzma --compress-level 0e",
                         "--codec=lzma --compress-level 0",
                         "--codec=lzma --compress-level 1e",
                         "--codec=lzma --compress-level 1",
                         ]:
            with temp_zs_path() as p_out:
                run(["make", "{}", "-", p_out,
                     # bigger than both the bz2 -z 1 blocksize of 100k
                     # and the lzma -z 0 blocksize of 256k
                     "--approx-block-size", "400000"] + settings.split(),
                    input=bigger_input)
                sizes[settings] = os.stat(p_out).st_size
        assert (sizes["--codec=lzma --compress-level 0e"]
                == sizes["--codec=lzma"])
        for big, small in [("none", "deflate"),
                           ("deflate", "bz2"),
                           ("bz2", "lzma"),
                           ("bz2 -z 1", "bz2"),
                           ("deflate --compress-level 1", "deflate"),
                           ("lzma --compress-level 0",
                            "lzma --compress-level 1"),
                           ("lzma --compress-level 0e",
                            "lzma --compress-level 1e"),
                           ("lzma --compress-level 0",
                            "lzma --compress-level 0e"),
                           ("lzma --compress-level 1",
                            "lzma --compress-level 1e"),
                           ]:
            assert sizes["--codec=" + big] > sizes["--codec=" + small]

        # metadata and no-default-metadata
        for no_default in [True, False]:
            with temp_zs_path() as p_out:
                args = []
                if no_default:
                    args.append("--no-default-metadata")
                run(["make", "{\"foo\": 1}", "-", p_out] + args,
                    input=NEWLINE_RECORDS)
                with ZS(p_out) as z:
                    assert z.metadata["foo"] == 1
                    if no_default:
                        assert "build-info" not in z.metadata
                    else:
                        assert "build-info" in z.metadata
        with temp_zs_path() as p_out:
            # bad metadata
            run(["make", "{", "-", p_out], input=NEWLINE_RECORDS,
                expected_returncode=2)

        # approx-block-size
        with temp_zs_path() as p_small, temp_zs_path() as p_big:
            run(["make", "{}", "-", p_small, "--approx-block-size", "1000"],
                input=big_input)
            run(["make", "{}", "-", p_big, "--approx-block-size", "10000"],
                input=big_input)

            with ZS(p_small) as z_small, ZS(p_big) as z_big:
                assert list(z_small) == list(z_big)
                # count how many blocks are in each file
                assert (len(list(z_small.block_map(nothing)))
                        > len(list(z_big.block_map(nothing))))

        # branching-factor
        with temp_zs_path() as p_b2, temp_zs_path() as p_b100:
            run(["make", "{}", "-", p_b2, "--approx-block-size", "1000",
                 "--branching-factor", "2"],
                input=big_input)
            run(["make", "{}", "-", p_b100, "--approx-block-size", "1000",
                 "--branching-factor", "100"],
                input=big_input)

            with ZS(p_b2) as z_b2, ZS(p_b100) as z_b100:
                assert list(z_b2) == list(z_b100)
                assert z_b2.root_index_level > z_b100.root_index_level

        # from file, not just stdin
        with tempname(".txt") as in_p, temp_zs_path() as out_p:
            with open(in_p, "wb") as in_f:
                in_f.write(big_input)
            run(["make", "{}", in_p, out_p])

            with ZS(out_p) as z:
                assert list(z) == big_records

        # integer checking
        for opt in ["--branching-factor", "--approx-block-size",
                    "--compress-level", "-z"]:
            with temp_zs_path() as p:
                run(["make", "{}", "-", p, opt, "NOT-AN-INT"],
                    input=NEWLINE_RECORDS,
                    expected_returncode=2)
        # bad json
        with temp_zs_path() as p:
            run(["make", "{}", "-", p, "--metadata", "{"],
                input=NEWLINE_RECORDS,
                expected_returncode=2)

def test_script_entry_point():
    # the above tests are all run using "python -m zs foo"; this tests that
    # "zs foo" also works -- but only if we are actually installed.
    expected = run(["--help"]).stdout
    try:
        got = run(["--help"], zs_cmd=["zs"]).stdout
    except OSError:
        if "ZS_REQUIRE_SCRIPT_TEST" in os.environ:
            raise
        else:
            raise SkipTest("'zs' script not found")

    assert expected == got
