Project overview
================

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
  If you use this software in work that leads to a scientific
  publication, and feel that a citation would be appropriate, then
  here is a possible citation:

  Smith, N. J. (submitted). ZS: A file format for efficiently
  distributing, using, and archiving record-oriented data sets of
  any size. Retrieved from http://vorpus.org/papers/draft/zs-paper.pdf

Developer dependencies (only needed for hacking on source):
  * Cython: needed to build from checkout
  * nose: needed to run tests
  * nose-cov: because we use multiprocessing, we need this package to
    get useful test coverage information
  * nginx: needed to run HTTP tests

License:
  2-clause BSD, see LICENSE.txt for details.
