from cStringIO import StringIO
from nose.tools import assert_raises
import pickle

from .common import *

def test_encoded_crc32c():
    assert encoded_crc32c(b"foo") == b"\x1d\xae\xc4\xcf"
    assert encoded_crc32c(b"\x00" * 32) == b"\x43\xab\xa8\x62"

def test_codecs():
    for test_vector in [b"",
                        b"foo",
                        b"".join([chr(i) for i in xrange(256)]),
                        b"a" * 32768,
                        ]:
        for name, (comp, decomp) in codecs.items():
            assert decomp(comp(test_vector)) == test_vector
            corrupted = comp(test_vector)[:-1] + chr(ord(test_vector[-1]) ^ 1)
            assert_raises(Exception, decomp, corrupted)
            # check pickleability
            assert pickle.loads(pickle.dumps(comp)) is comp
            assert pickle.loads(pickle.dumps(decomp)) is decomp

def test_read_n():
    f = StringIO(b"abcde")
    assert read_n(f, 3) == b"abc"
    assert_raises(ZSSCorrupt, read_n, 3)

def test_read_format():
    f = StringIO(b"\x01\x02\x03")
    a, b = read_format(f, "<BS")
    assert a == 1
    assert b == 0x0302
