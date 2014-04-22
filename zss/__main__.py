# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# This file is executed when 'python -m zs' is run.

import sys

import zs.cmdline.main

sys.exit(zs.cmdline.main.entrypoint())
