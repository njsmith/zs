#!/usr/bin/env python

# FIXME: this ignores indexing overhead (which becomes smaller for larger
# blocks, and grows with total file size -- but very slowly.)

import sys
import bz2
import zlib
import time

from zss._zss import pack_data_block

test_path = sys.argv[1]

# Try:
#   -- different split sizes
#   -- different compression algorithms

SPLIT_SIZES = [4 * 2**10, 6 * 2**10, 8 * 2**10, 12 * 2**10, 16 * 2**10, 24 *
               2**10, 32 * 2**10, 40 * 2**10, 48 * 2**10, 56 * 2**10, 64 *
               2**10, 128 * 2**10, 2**20]

ALGORITHMS = [
    (zlib, 1),
    (zlib, 6),
    (zlib, 9),
    (bz2, 9),
    ]

BLOCKS = {}
for split_size in SPLIT_SIZES:
    leftover = ""
    f = open(test_path)
    blocks = []
    total_bytes = 0
    while True:
        buf = f.read(split_size)
        total_bytes += len(buf)
        if not buf:
            assert not leftover
            break
        lines = buf.split("\n")
        lines[0] = leftover + lines[0]
        leftover = lines.pop()
        blocks.append(pack_data_block(lines, split_size))
    packed_bytes = sum([len(b) for b in blocks])
    print "%s: %s -> %s (%0.2fx)" % (
        split_size, total_bytes, packed_bytes,
        packed_bytes * 1.0 / total_bytes)
    BLOCKS[split_size] = blocks
#import pdb; pdb.set_trace()

NUMBER = 5
for (alg, level) in ALGORITHMS:
    print "%s, %s" % (alg.__name__, level)
    for split_size in SPLIT_SIZES:
        blocks = BLOCKS[split_size]
        start = time.time()
        comp = [alg.compress(b, level) for b in blocks]
        stop = time.time()
        comp_time = stop - start
        # t = timeit.timeit("[%s.compress(b, %r) for b in "
        #                   % (alg.__name__, level),
        #                   setup="from __main__ import BLOCKS; "
        #                         "blocks = BLOCKS[%s]" % (split_size,),
        #                   number=NUMBER)
        start = time.time()
        decomp = [alg.decompress(c) for c in comp]
        assert sum([len(d) for d in decomp]) == total_bytes
        stop = time.time()
        decomp_time = stop - start
        comp_mib = len("".join(comp)) * 1.0 / (2 ** 20)
        print ("  %s: comp %0.2f s, decomp %0.2f s (%0.1f ms/block), %0.3f MiB"
               % (split_size, comp_time, decomp_time,
                  decomp_time * 1.0 / len(blocks) * 1000, comp_mib))
