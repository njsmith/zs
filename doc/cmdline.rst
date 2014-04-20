The command-line ``zss`` tool
=============================

The ``zss`` tool can be used from the command-line to create, view,
and check ZSS files.

In case you have the Python ``zss`` library installed, but somehow do
not have the ``zss`` executable, it can also be invoked as ``python -m
zss``. E.g., these two commands do the same thing::

    zss dump myfile.zss
    python -m zss dump myfile.zss

``zss`` provides a number of 'subcommands' which are used to do the
real work. For an overview of usage, you can use ``zss --help``:

.. command-output:: zss --help

.. _zss make:

``zss make``
------------

``zss make`` allows you to create ZSS files.

.. _zss dump:

``zss dump``
------------

``zss dump`` is used to extract data from a ZSS file -- it's the
inverse of ``zss make``. In the simplest case, it simply dumps the
whole file to standard output, with one record per line. But it has
further options that allow you to specify where the output should be
placed, how it should be formatted, and to select a specific subset of
the data:

.. command-output:: zss dump --help

For example,

.. _zss make:

.. _zss info

``zss dump``
------------



``zss make``
------------

This is the command used to

.. programoutput::
