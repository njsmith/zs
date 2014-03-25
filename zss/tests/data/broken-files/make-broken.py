# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

#!/usr/bin/env python

import os
import shutil
from six import BytesIO
import zss
import zss.common

shutil.copy("../letters-none.zss", "partial-root.zss")
with open("partial-root.zss", "r+b") as f:
    os.ftruncate(f.fileno(),
                 os.stat("partial-root.zss").st_size - 1)

shutil.copy("../letters-none.zss", "bad-magic.zss")
open("bad-magic.zss", "r+b").write(b"Q")

shutil.copy("../letters-none.zss", "incomplete-magic.zss")
open("incomplete-magic.zss", "r+b").write(zss.common.INCOMPLETE_MAGIC)

shutil.copy("../letters-none.zss", "header-checksum.zss")
with open("header-checksum.zss", "r+b") as f:
    # 28 bytes places us at the beginning of the uuid field, so semantically a
    # bunch of zeros are totally legal here.
    f.seek(28)
    f.write(b"\x00" * 8)

shutil.copy("../letters-none.zss", "root-checksum.zss")
with open("root-checksum.zss", "r+b") as f:
    f.seek(-4, 2)
    f.write(b"\x00" * 4)

if os.path.exists("bad-codec.zss"):
    os.unlink("bad-codec.zss")
zss.common.codecs["XXX-bad-codec-XXX"] = zss.common.codecs["none"]
w = zss.ZSSWriter("bad-codec.zss", {}, 10, 100, 2,
                  compression="XXX-bad-codec-XXX")
w.from_file(BytesIO(b"\n".join(list(b"abcdefghijklmnopqrstuvwxyz") + [b""])))

from zss.writer import _encode_header
import json
import struct
shutil.copy("../letters-none.zss", "non-dict-metadata.zss")
with zss.ZSS("non-dict-metadata.zss") as z:
    header = z._get_header()
    old_metadata_str = json.dumps(header["metadata"])
with open("non-dict-metadata.zss", "r+b") as f:
    # hack: we replace the metadata with a non-dict object that has the same
    # length json encoding
    f.seek(len(zss.common.MAGIC))
    header["metadata"] = u"x" * (len(old_metadata_str) - 2)
    old_length, = zss.common.read_format(f, zss.common.header_data_length_format)
    new_header = _encode_header(header)
    assert len(new_header) == old_length
    f.write(new_header)
    f.write(zss.common.encoded_crc32c(new_header))
