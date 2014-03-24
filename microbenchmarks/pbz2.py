#!/usr/bin/env python

# Simple test of parallelizing bz2 via threads

# Russula: 12 real cores: pbz2.py 128000 12 => ~ 115 uncompressed MiB/s
# Polypore: 48 real cores: pbz2.py 128000 20 => ~ 145 uncompressed MiB/s
#    ~175 MiB/s with 40 cores
# But, with 32768 buffer size, drops to 70 uMiB/s
#   65536 20 = ~133 uMiB/s

import sys
import bz2
import threading
from Queue import Queue

QUIT = None

def worker(in_queue, out_queue):
    while True:
        job = in_queue.get()
        if job is QUIT:
            break
        result = bz2.compress(job[1])
        out_queue.put((job[0], result))

def writer(in_queue):
    waiting_for = 0
    pending = {}
    while True:
        job = in_queue.get()
        if job is QUIT:
            assert not pending
            break
        idx, data = job
        pending[idx] = data
        while waiting_for in pending:
            sys.stdout.write(pending[waiting_for])
            del pending[waiting_for]
            waiting_for += 1

def main(progname, args):
    if len(args) != 2:
        sys.exit("Usage: %s BUFSIZE NUM-THREADS" % (progname,))
    bufsize = int(args[0])
    num_threads = int(args[1])
    worker_queue = Queue(num_threads * 2)
    writer_queue = Queue(num_threads * 2)
    writer_thread = threading.Thread(target=writer, args=(writer_queue,))
    writer_thread.start()
    worker_threads = []
    worker_args = (worker_queue, writer_queue)
    for i in xrange(num_threads):
        worker_threads.append(threading.Thread(target=worker,
                                               args=worker_args))
        worker_threads[-1].start()
    i = 0
    while True:
        data = sys.stdin.read(bufsize)
        if not data:
            break
        worker_queue.put((i, data))
        i += 1

    # Shut down
    sys.stderr.write("Shutting down\n")
    for i in xrange(num_threads):
        worker_queue.put(QUIT)
    for worker_thread in worker_threads:
        worker_thread.join()
    # All the worker threads have exited, so all their work has been enqueued
    # to the writer thread. So our QUIT will end up after all actual work in
    # the queue.
    writer_queue.put(QUIT)
    writer_thread.join()
    sys.stderr.write("Shutdown successful\n")

if __name__ == "__main__":
    main(sys.argv[0], sys.argv[1:])
