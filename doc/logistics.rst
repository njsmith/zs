Project logistics
=================

Documentation:
  http://zs.readthedocs.org/

Installation:
  Because ``zs`` includes a C extension, you'll need a C compiler to
  install it. You'll also need either Python 2.7, or else Python 3.3
  or greater.

  Assuming you have a C compiler available, installation in Python 3
  should be as simple as::

    pip install zs

  On Python 2.7, ``zs`` also requires the ``backports.lzma`` package,
  which in turn requires the liblzma library. On Ubuntu or Debian, for
  example, something like this should work::

    sudo apt-get install liblzma-dev
    pip install backports.lzma
    pip install zs

  ``zs`` also requires the following packages: ``six``, ``docopt``,
  ``requests``. However, these are all pure-Python packages which pip
  will install for you automatically.

Downloads:
  http://pypi.python.org/pypi/zs/

Code and bug tracker:
  https://github.com/njsmith/zs

Contact:
  Nathaniel J. Smith <nathaniel.smith@ed.ac.uk>

Developer dependencies (only needed for hacking on source):
  * Cython: needed to build from checkout
  * nose: needed to run tests
  * nose-cov: because we use multiprocessing, we need this package to
    get useful test coverage information
  * nginx: needed to run HTTP tests

License:
  2-clause BSD, see LICENSE.txt for details.
