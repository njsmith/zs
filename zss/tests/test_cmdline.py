# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# TODO:
# - add simple 'zss' test directly to .travis.yml
#     or just have travis.yml set a variable we check here?
#     try running test with skip if zss not on path
#       and if var is set then fail instead of skip
import sys
import subprocess
import os
import os.path
from collections import namedtuple
from contextlib import contextmanager
import json
import binascii
from unittest.case import SkipTest

from six import BytesIO

import zss
from zss import ZSS, ZSSWriter
from .util import tempname, test_data_path
from .http_harness import web_server

RunResult = namedtuple("RunResult", ["returncode", "stdout", "stderr"])

# To let the test suite run without installing, we only try the '-m zss'
# version, not the 'zss' script itself. .travis.yml has a little test to make
# sure that setup.py does install the 'zss' script.
CMD = [sys.executable, "-m", "zss"]

def run(args, expected_returncode=0, input=b"", zss_cmd=CMD):
    p = subprocess.Popen(zss_cmd + args,
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
        print("stderr: %r" % (stdout,))
        print("returncode: %r" % (returncode,))
        raise
    return RunResult(returncode, stdout, stderr)

RECORDS = [b"", b"a", b"b", b"bb", b"c"]
NEWLINE_RECORDS = b"\n".join(RECORDS + [b""])

@contextmanager
def simple_zss(records=RECORDS):
    with tempname(".zss", unlink_first=True) as p:
        with ZSSWriter(p, {"temp": 1}, 2) as zw:
            zw.add_data_block(records)
            zw.finish()
        yield p

def test_basic():
    assert b"--help" in run(["--asdf"], expected_returncode=2).stderr
    assert b"--help" in run(["asdf"], expected_returncode=2).stderr
    assert b"subcommands" in run(["--help"]).stdout

def test_dump():
    with simple_zss() as p:
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

    with simple_zss([b"\x00", b"\x01", b"\x01a", b"\x02"]) as p:
        assert (run(["dump", p, "--length-prefixed=uleb128"]).stdout
                == b"\x01\x00\x01\x01\x02\x01a\x01\x02")
        assert (run(["dump", p,
                     "--length-prefixed=uleb128",
                     "--prefix=\\x01"]).stdout
                == b"\x01\x01\x02\x01a")

def test_validate():
    run(["validate", test_data_path("letters-none.zss")])

    r = run(["validate", test_data_path("broken-files/unref-data.zss")],
            expected_returncode=1)
    assert "unreferenced" in r.stdout

def test_info():
    with simple_zss() as p:
        out = run(["info", p])
        info = json.loads(out.stdout)
        with ZSS(p) as z:
            assert info["codec"] == z.codec
            assert binascii.unhexlify(info["data_sha256"]) == z.data_sha256
            assert info["metadata"] == z.metadata

def test_urls():
    with web_server(test_data_path()) as root_url:
        url = root_url + "/letters-none.zss"
        path = test_data_path("letters-none.zss")

        assert run(["dump", url]).stdout == run(["dump", path]).stdout
        run(["validate", url])
        assert run(["info", url]).stdout == run(["info", path]).stdout

def test_make():
    from .test_writer import records as big_records, temp_zss_path

    with simple_zss(big_records) as p_in:
        for format_opt in ["--terminator=\\n",
                           "--terminator=\\x00",
                           "--length-prefixed=uleb128",
                           "--length-prefixed=u64le",
                          ]:
            input = run(["dump", p_in, format_opt]).stdout
            with temp_zss_path() as p_out:
                run(["make", format_opt, "-", p_out], input=input)

                with ZSS(p_out) as z:
                    z.validate()
                    assert list(z) == big_records

        big_input = b"\n".join(big_records + [b""])

        # smoke test -j
        with temp_zss_path() as p_out:
            run(["make", "-", p_out, "-j", "3"], input=NEWLINE_RECORDS)

        # --no-spinner produces less chatter
        with temp_zss_path() as p_out:
            r1 = run(["make", "-", p_out], input=big_input)
        with temp_zss_path() as p_out:
            r2 = run(["make", "-", p_out, "--no-spinner"], input=big_input)
        assert len(r2.stdout) < len(r1.stdout)

        # codecs and compress level
        # some nice big input so that bz2 -1 and -9 will differ:
        bigger_input = b"\n".join([("%050i" % (i,)).encode("utf-8")
                                   for i in xrange(10000)]
                                  + [b""])
        sizes = {}
        for settings in ["--codec=none",
                         "--codec=bz2",
                         "--codec=bz2 -z 1",
                         "--codec=deflate",
                         "--codec=deflate --compress-level 1"]:
            with temp_zss_path() as p_out:
                run(["make", "-", p_out] + settings.split(),
                    input=bigger_input)
                sizes[settings] = os.stat(p_out).st_size
        assert sizes["--codec=none"] > sizes["--codec=deflate"]
        assert sizes["--codec=deflate"] > sizes["--codec=bz2"]
        assert sizes["--codec=bz2 -z 1"] >= sizes["--codec=bz2"]
        assert (sizes["--codec=deflate --compress-level 1"]
                > sizes["--codec=deflate"])

        # metadata and no-default-metadata
        for no_default in [True, False]:
            with temp_zss_path() as p_out:
                args = []
                if no_default:
                    args.append("--no-default-metadata")
                run(["make", "-", p_out, "--metadata={\"foo\": 1}"] + args,
                    input=NEWLINE_RECORDS)
                with ZSS(p_out) as z:
                    assert z.metadata["foo"] == 1
                    if no_default:
                        assert "build-info" not in z.metadata
                    else:
                        assert "build-info" in z.metadata

        # approx-block-size
        with temp_zss_path() as p_small, temp_zss_path() as p_big:
            run(["make", "-", p_small, "--approx-block-size", "1000"],
                input=big_input)
            run(["make", "-", p_big, "--approx-block-size", "10000"],
                input=big_input)

            with ZSS(p_small) as z_small, ZSS(p_big) as z_big:
                assert list(z_small) == list(z_big)
                assert (len(list(z_small.sloppy_block_search()))
                        > len(list(z_big.sloppy_block_search())))

        # branching-factor
        with temp_zss_path() as p_b2, temp_zss_path() as p_b100:
            run(["make", "-", p_b2, "--approx-block-size", "1000",
                 "--branching-factor", "2"],
                input=big_input)
            run(["make", "-", p_b100, "--approx-block-size", "1000",
                 "--branching-factor", "100"],
                input=big_input)

            with ZSS(p_b2) as z_b2, ZSS(p_b100) as z_b100:
                assert list(z_b2) == list(z_b100)
                assert z_b2.root_index_level > z_b100.root_index_level

        # from file, not just stdin
        with tempname(".txt") as in_p, temp_zss_path() as out_p:
            with open(in_p, "wb") as in_f:
                in_f.write(big_input)
            run(["make", in_p, out_p])

            with ZSS(out_p) as z:
                assert list(z) == big_records

        # integer checking
        for opt in ["--branching-factor", "--approx-block-size",
                    "--compress-level", "-z"]:
            with temp_zss_path() as p:
                run(["make", "-", p, opt, "NOT-AN-INT"],
                    input=NEWLINE_RECORDS,
                    expected_returncode=2)
        # bad json
        with temp_zss_path() as p:
            run(["make", "-", p, "--metadata", "{"],
                input=NEWLINE_RECORDS,
                expected_returncode=2)

def test_script_entry_point():
    # the above tests are all run using "python -m zss foo"; this tests that
    # "zss foo" also works -- but only if we are actually installed.
    expected = run(["--help"]).stdout
    try:
        got = run(["--help"], zss_cmd=["zss"]).stdout
    except OSError:
        if "ZSS_REQUIRE_SCRIPT_TEST" in os.environ:
            raise
        else:
            raise SkipTest("'zss' script not found")

    assert expected == got
