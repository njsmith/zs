# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

#!/usr/bin/env python

import os
import shutil
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



# EOF in the middle of a block encountered while streaming
# unknown compression format
# metadata is invalid json, or not a dict
