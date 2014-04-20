# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# This file is executed when 'python -m zss' is run.

import sys

import zss.cmdline.main

sys.exit(zss.cmdline.main.entrypoint())
