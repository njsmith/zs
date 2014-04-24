ZS is a simple, read-only, binary file format designed for
distributing, querying, and archiving arbitrarily large
record-oriented datasets (up to tens of terabytes and beyond). It
allows the data to be stored in compressed form, while still
supporting very fast queries for either specific entries, or for all
entries in a specified range of values (e.g., prefix searches), and
allows highly-CPU-parallel decompression. It also places an emphasis
on data integrity -- all data is protected by 64-bit CRC checksums --
and on discoverability -- every ZS file includes arbitrarily detailed
structured metadata stored directly inside it.

Basically you can think of ZS as a turbo-charged replacement for
storing data in line-based text file formats. It was originally
developed to provide a better way to work with the massive `Google N-grams
<http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_,
but is potentially useful for data sets of any size.

.. image:: https://travis-ci.org/njsmith/zs.png?branch=master
   :target: https://travis-ci.org/njsmith/zs
.. image:: https://coveralls.io/repos/njsmith/zs/badge.png?branch=master
   :target: https://coveralls.io/r/njsmith/zs?branch=master

Documentation:
  http://zs.readthedocs.org/

Install:
  ``pip install zs`` (or, for traditionalists: ``python setup.py install``)

Downloads:
  http://pypi.python.org/pypi/zs/

Code and bug tracker:
  https://github.com/njsmith/zs

Mailing list:
  * TODO (in the mean time you can hassle nathaniel.smith@ed.ac.uk directly)

License:
  2-clause BSD, see LICENSE.txt for details.

Dependencies:
  * Python 2.7, or Python 3.3+
  * Python packages:

    * six
    * requests
    * docopt

Developer dependencies (only needed for hacking on source):
  * Cython: needed to build from checkout
  * nose: needed to run tests
  * nose-cov: needed to get useful test coverage information in the
    face of massive multiprocessing
  * nginx: needed to run HTTP tests
