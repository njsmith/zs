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

.. image:: https://travis-ci.org/njsmith/zss.png?branch=master
   :target: https://travis-ci.org/njsmith/zss
.. image:: https://coveralls.io/repos/njsmith/zss/badge.png?branch=master
   :target: https://coveralls.io/r/njsmith/zss?branch=master

Documentation:
  http://zss.readthedocs.org/ [TODO]

Downloads:
  http://pypi.python.org/pypi/zss/ [TODO]

Dependencies:
  * Python 2.7 (or Py 3.3? XX check)
  * Python packages:
    * six
    * requests
    * docopt

Optional dependencies:
  * nose: needed to run tests
  * nginx: needed to run HTTP tests
  * lxml: needed by the the gbooksv2 downloader

Install:
  ``pip install zss`` (or, for traditionalists: ``python setup.py install``)

Code and bug tracker:
  https://github.com/njsmith/zss

Mailing list:
  * TODO

License:
  2-clause BSD, see LICENSE.txt for details.
