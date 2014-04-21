The ``zss`` Python package
==========================

.. module:: zss

Quickstart
----------

Using the example file from the section on :ref:`zss make`, we can
write:

.. ipython:: python

   from zss import ZSS

   z = ZSS("example/tiny-4grams.zss")

   for record in z:
       print(record)

   # Notice that on Python 3.x, we must pass a byte string.
   # (On Python 2.x, a regular string will do.)
   for record in z.search(prefix=b"not done extensive testing\t"):
       print(record)

   for record in z.search(prefix=b"not done extensive "):
       print(record)

   for record in z.search(start=b"not done ext", stop=b"not done fast"):
       print(record)

Error reporting
---------------

:mod:`zss` defines two exception types.

.. autoexception:: ZSSError

.. autoexception:: ZSSCorrupt


Reading
-------

Reading ZSS files is accomplished by instantiating an object of type
:class:`ZSS`:

.. autoclass:: ZSS

   **Basic searches**

   .. automethod:: search

   .. automethod:: __iter__

   **Metadata access**

   This class provides a number of read-only attributes that give
   general information about the ZSS file:

   .. attribute:: metadata
      :annotation:

      A .zss file can contain arbitrary metadata in the form of a
      JSON-encoded dictionary. This attribute contains this metadata in
      unpacked form.

   .. attribute:: root_index_offset
      :annotation:

      The file offset of the root index block, as stored in the
      :ref:`header <format-header>`.

   .. attribute:: root_index_length
      :annotation:

      The length of the root index block, as stored in the
      :ref:`header <format-header>`.

   .. attribute:: total_file_length
      :annotation:

      The proper length of this file, as stored in the :ref:`header
      <format-header>`.

   .. attribute:: codec
      :annotation:

      The compression codec used on this file, as a byte string.

   .. attribute:: data_sha256
      :annotation:

      A strong hash of the underlying data records contained in this
      file. If two files have the same value here, then they are
      guaranteed to represent exactly the same data (i.e., return the
      same records to the same queries), though they might be stored
      using different compression algorithms, have different metadata,
      etc.

   .. autoattribute:: root_index_level
      :annotation:

   **Fast bulk operations for experts**

   .. automethod:: block_map

   .. automethod:: block_exec

   **High-level operations**

   .. automethod:: dump

   .. automethod:: validate

Writing
-------

In case you want a little more control over ZSS file writing than you
can get with the ``zss make`` command-line utility (see :ref:`zss
make`), you can also access the underlying ZSS-writing code directly
from Python by instantiating a :class:`ZSSWriter` object.

.. autoclass:: ZSSWriter

   .. automethod:: add_data_block

   .. automethod:: add_file_contents

   .. automethod:: finish

   .. automethod:: close

   .. attribute:: closed

      Boolean attribute indicating whether this ZSSWriter is closed.
