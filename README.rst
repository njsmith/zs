ZSS is a simple read-only file format for storing, distributing, and
querying very large sets of arbitrary binary values (up to tens of
terabytes and beyond). It allows the data to be stored in compressed
form, while still supporting very fast queries for either specific
entries, or for all entries in a specified range of values (e.g.,
prefix searches), and allows highly-CPU-parallel decompression. It's
well suited to serve as a basis for large static key/value stores, or
for storing even larger databases across multiple machines (though
each individual ZSS database must be contained within a single
operating-system level file.)

Why is this useful? Consider just the 3-gram counts from the 2012 US
English release of the `Google N-grams
<http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_. Google
distributes this data as a set of gzipped text files. Uncompressed, it
amounts to over than 9 terabytes -- or even more if you tried to store
it in a conventional database. Google's compressed files reduce this
to a mere 1.3 terabytes. However, when stored in this form, the only
way to find a specific n-gram is to locate the file which contains it,
and then read through the entire file, which requires decompressing
the file. For gzip files, decompression is quite slow. For example, if
we want to find statistics for a phrase like "this is fun", we may
have to decompress the entire file containing "th" 3-grams, which on a
fast computer requires 30-40 minutes, and because decompression is an
intrinsically serial operator, this cannot be reduced by using
multiple CPUs.

When stored as a ZSS file with bzip2 compression, the 2012 US English
3-grams require only 0.9 terabytes of disk space (more than 30%
smaller than Google's current distribution format), yet it takes only
4 disk seeks and less than 10 milliseconds of CPU time to discover how
many times "this is fun" was used in any Google-scanned book published
in 1955 (answer: 27 times) -- i.e., lookups go from a tens of minutes
to faster than a blink of an eye. And for bulk operations (e.g.,
looking at all three-word phrases beginning "this is"), ZSS
decompression speed is limited only by the number of available CPUs.

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