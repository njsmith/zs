The ``zss`` library for Python
==============================

.. module:: zss

Reading
-------

.. autoclass:: ZSS
   :members:

Writing
-------

In case you want a little more control over ZSS file writing than you
can get with the ``zss make`` command-line utility (see :ref:`zss
make`), you can also access the underlying ZSS-writing code directly
from Python by instantiating a :class:`ZSSWriter` object.

.. autoclass:: ZSSWriter
   :members:
