#!/usr/bin/env python

# Test of parallelizing bz2 and packing via multiprocessing

# Pushing record-lists through multiprocessing.Queue dies at 10 MiB/s
# Packing in the main thread seems to die at around 70 MiB/s, which is weird
# since split-lines can run at >140 MiB/s -- I guess packing + pickling is
# expensive.

import sys
import bz2
from multiprocessing import Process, Queue
from zs._zs import pack_data_records

QUIT = None

def worker(in_queue, out_queue, alloc_hint):
    while True:
        job = in_queue.get()
        if job is QUIT:
            break
        idx, data = job
        records = records.split("\n")
        result = bz2.compress(pack_data_records(records, alloc_hint))
        out_queue.put((idx, result))

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
    writer_thread = Process(target=writer, args=(writer_queue,))
    writer_thread.start()
    worker_threads = []
    worker_args = (worker_queue, writer_queue, bufsize * 2)
    for i in xrange(num_threads):
        worker_threads.append(Process(target=worker, args=worker_args))
        worker_threads[-1].start()
    i = 0
    partial_line = ""
    while True:
        data = sys.stdin.read(bufsize)
        if not data:
            break
        data = partial_line + data
        data, partial_line = data.rsplit("\n", 1)
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
