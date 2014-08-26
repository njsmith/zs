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

Installation:
  You need either Python **2.7**, or else Python **3.3 or greater**.

  Because ``zs`` includes a C extension, you'll also need a C compiler
  and Python headers. On Ubuntu or Debian, for example, you get these
  with::

    sudo apt-get install build-essential python-dev

  Once you have the ability to build C extensions, then on Python
  3 you should be able to just run::

    pip install zs

  On Python 2.7, things are slightly more complicated: here, ``zs``
  requires the ``backports.lzma`` package, which in turn requires the
  liblzma library. On Ubuntu or Debian, for example, something like
  this should work::

    sudo apt-get install liblzma-dev
    pip install backports.lzma
    pip install zs

  ``zs`` also requires the following packages: ``six``, ``docopt``,
  ``requests``. However, these are all pure-Python packages which pip
  will install for you automatically when you run ``pip install zs``.

Downloads:
  http://pypi.python.org/pypi/zs/

Code and bug tracker:
  https://github.com/njsmith/zs

Contact:
  Nathaniel J. Smith <nathaniel.smith@ed.ac.uk>

Citation:
  If you use this software (or the ZS format in general) in
  work that leads to a scientific publication, and feel that a
  citation would be appropriate, then here is a possible citation:

  Smith, N. J. (submitted). ZS: A file format for efficiently
  distributing, using, and archiving record-oriented data sets of
  any size. Retrieved from http://vorpus.org/papers/draft/zs-paper.pdf

  In addition, if you wish to document exactly which version of this
  software you used, then each official release has its own DOI which
  you can find on the `change history page
  <http://zs.readthedocs.org/en/latest/changes.html>`_.

  It may make sense to cite either or both of these at the same time
  -- the paper gives a general introduction to the ZS format and why
  it is useful, the version-specific DOIs link directly to archived
  snapshots of a specific version of this specific implementation of
  the ZS format. FWIW, I personally benefit more from citations to the
  paper.

Developer dependencies (only needed for hacking on source):
  * Cython: needed to build from checkout
  * nose: needed to run tests
  * nose-cov: because we use multiprocessing, we need this package to
    get useful test coverage information
  * nginx: needed to run HTTP tests

License:
  2-clause BSD, see LICENSE.txt for details.
