# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# A wrapper around GNU sort to merge pre-sorted compressed files.
#
# Usage:
#   python -m zss.util.merge_sorted FILE1.gz FILE2.gz ...
# where each file is already sorted.
#
# Files can be .bz2, .gz, or raw.

import sys
import os
import resource
import subprocess
import fcntl

codecs = [
    (".gz", ["gunzip", "-c"]),
    (".bz2", ["bunzip2", "-c"]),
    ]

def clear_cloexec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)

def main(progname, args):
    paths = args

    # Raise our soft limit on the number of available fds to match our hard
    # limit.
    (_, hard_fd_limit) = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard_fd_limit, hard_fd_limit))

    # sort will error out if we try setting --batch-size to larger than the fd
    # limit.
    sort_cmd = ["env", "LC_ALL=C", "sort", "-m",
                "--batch-size=%s" % (hard_fd_limit - 3,)]

    # Strategy:
    #  -- for each file, spawn a gunzip file writing into a pipe
    #  -- spawn a sort process that inherits these pipe file descriptors, and
    #     pass /dev/fd/<n> as the filenames to sort. (This is similar to how
    #     bash "process substitution" works.)

    # First do a scan to count how many fds will be needed, and error out if
    # it's too many. Uncompressed files that we can pass directly take only 1
    # fd. Compressed files take 2: one that's inherited, and then a second
    # when the sort program calls open("/dev/fd/<n>") (which is similar to
    # calling dup(n)).
    fd_count = 3 # stdin, stdout, stderr
    for path in paths:
        for suffix, _ in codecs:
            if path.endswith(suffix):
                fd_count += 2
                break
        else:
            fd_count += 1
    if fd_count >= hard_fd_limit:
        sys.stderr.write("Error: need %s fds, but hard limit is set to %s.\n")
        sys.stderr.write("Raise the maximum number of open files limit and "
                         "try again.\n")
        sys.exit(1)

    decompressors = []
    sort_paths = []
    for path in paths:
        for suffix, cmd in codecs:
            if path.endswith(suffix):
                sys.stderr.write("%s: spawning decompressor\n" % (path,))
                decompressor = subprocess.Popen(cmd + [path],
                                                stdout=subprocess.PIPE)
                fd = decompressor.stdout.fileno()
                clear_cloexec(fd)
                sort_paths.append("/dev/fd/%s" % (fd,))
                decompressors.append((path, decompressor))
                break
        else:
            sys.stderr.write("%s: not decompressing\n" % (path,))
            sort_paths.append(path)

    assert fd_count == 3 + len(decompressors) + len(sort_paths)

    sys.stderr.write("sort: starting\n")
    sort = subprocess.Popen(sort_cmd + sort_paths, close_fds=False)
    sort.wait()
    sys.stderr.write("sort: exited\n")
    last_error = 0
    for path, decompressor in decompressors:
        decompressor.poll()
        if decompressor.returncode is None:
            decompressor.terminate()
            decompressor.wait()
        if decompressor.returncode != 0:
            sys.stderr.write("%s: decompression failed with return code %s!\n"
                             % (path, decompressor.returncode))
            last_error = decompressor.returncode
    if sort.returncode != 0:
        sys.stderr.write("Sort failed with return code %s!\n"
                         % (sort.returncode,))
        last_error = sort.returncode
    sys.exit(last_error)

if __name__ == "__main__":
    main(sys.argv[0], sys.argv[1:])
