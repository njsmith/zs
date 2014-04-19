.. ZSS documentation master file, created by
   sphinx-quickstart on Sun Nov 24 18:21:57 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZSS - compressed sorted sets
============================

ZSS is a simple read-only file format designed for archiving,
distributing, and querying very large collections of arbitrary binary
values (up to tens of terabytes and beyond). It allows the data to be
stored in compressed form, while still supporting very fast queries
for either specific entries, or for all entries in a specified range
of values (e.g., prefix searches), and allows highly-CPU-parallel
decompression. It places an emphasis on data integrity -- all data is
protected by 64-bit CRC checksums. It's well suited to serve as a
basis for large static key/value stores.

Why is this useful? Consider just the 3-gram counts from the 2012 US
English release of the `Google N-grams
<http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_. Google
distributes this data as a set of gzipped text files. Uncompressed, it
amounts to more than 9 terabytes -- or even more if you tried to store
it in a conventional database. As distributed by Google, it takes a
mere 1.3 terabytes. However, with gzipped text files, the only way to
find a specific n-gram is to locate the file which contains it, and
then read through the entire file, which requires decompressing the
file. For gzip files, decompression is quite slow, and cannot take
advantage of multiple CPUs. For example, if we want to find statistics
for a phrase like "this is fun", we may have to decompress the entire
file containing "th" 3-grams, which on a fast computer requires 30-40
minutes.

When stored as a ZSS file with bzip2 compression, the 2012 US English
3-grams require only 0.9 terabytes of disk space (more than 30%
smaller), yet it takes only 4 disk seeks and less than 10 milliseconds
of CPU time to discover how many times "this is fun" was used in a
Google-scanned book in 1955 (answer: 27 times). And for bulk
operations (e.g., looking at all three-word phrases beginning "this
is"), decompression speed is limited only by the number of available
CPUs.

This manual documents both the details of the ZSS file format itself,
and the Python reference implementation, which provides a library
interface and a command-line ``zss`` tool that can be used to read and
write files in this format.

Contents:

.. toctree::
   :maxdepth: 2

   logistics.rst

   introduction.rst

   cmdline.rst

   read.rst

   write.rst

   metadata.rst

   format.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
