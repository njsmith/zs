ZSS is a simple, read-only, binary file format designed for
distributing, querying, and archiving very large collections of
arbitrary binary values (up to tens of terabytes and beyond). It
allows the data to be stored in compressed form, while still
supporting very fast queries for either specific entries, or for all
entries in a specified range of values (e.g., prefix searches), and
allows highly-CPU-parallel decompression. It places an emphasis on
data integrity -- all data is protected by 64-bit CRC checksums. It's
well suited to serve as a basis for large static key/value stores.

It was originally developed to be a better format for working with the
`Google N-grams
<http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_,
but may be useful in any case where you need a convenient way to stash
a small or large data set in a convenient form.

.. image:: https://travis-ci.org/njsmith/zss.png?branch=master
   :target: https://travis-ci.org/njsmith/zss
.. image:: https://coveralls.io/repos/njsmith/zss/badge.png?branch=master
   :target: https://coveralls.io/r/njsmith/zss?branch=master

Documentation:
  http://zss.readthedocs.org/

Downloads:
  http://pypi.python.org/pypi/zss/ [TODO]

Dependencies:
  * Python 2.7, or Python 3.3+
  * Python packages:

    * six
    * requests
    * docopt

Developer dependencies (only needed for hacking on source):
  * Cython: needed to build from checkout
  * nose: needed to run tests
  * nose-cov: needed to get useful test coverage information
  * nginx: needed to run HTTP tests

Install:
  ``pip install zss`` (or, for traditionalists: ``python setup.py install``)

Code and bug tracker:
  https://github.com/njsmith/zss

Mailing list:
  * TODO

License:
  2-clause BSD, see LICENSE.txt for details.
