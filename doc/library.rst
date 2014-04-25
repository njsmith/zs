The ``zs`` library for Python
=============================

.. module:: zs

Quickstart
----------

Using the example file we created when demonstrating :ref:`zs make`,
we can write:

.. ipython:: python

   from zs import ZS

   z = ZS("example/tiny-4grams.zs")

   for record in z:
       print(record.decode("utf-8"))

   # Notice that on Python 3.x, we search using byte strings, and we get
   # byte strings back.
   # (On Python 2.x, byte strings are the same as regular strings.)
   for record in z.search(prefix=b"not done extensive testing\t"):
       print(record.decode("utf-8"))

   for record in z.search(prefix=b"not done extensive "):
       print(record.decode("utf-8"))

   for record in z.search(start=b"not done ext", stop=b"not done fast"):
       print(record.decode("utf-8"))

Error reporting
---------------

:mod:`zs` defines two exception types.

.. autoexception:: ZSError

.. autoexception:: ZSCorrupt


Reading
-------

Reading ZS files is accomplished by instantiating an object of type
:class:`ZS`:

.. autoclass:: ZS

Basic searches
''''''''''''''

.. class:: ZS

   .. automethod:: search

   .. automethod:: __iter__

File attributes and metadata
''''''''''''''''''''''''''''

.. class:: ZS

   :class:`ZS` objects provides a number of read-only attributes
   that give general information about the ZS file:

   .. attribute:: metadata
      :annotation:

      A .zs file can contain arbitrary metadata in the form of a
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

Fast bulk operations
''''''''''''''''''''

If you want to perform some computation on many records (e.g., all the
records in your file), then these functions are the most efficient way
to do that.

.. class:: ZS

   .. automethod:: block_map

   .. automethod:: block_exec

High-level operations
'''''''''''''''''''''

.. class:: ZS

   .. automethod:: dump

   .. automethod:: validate

Writing
-------

In case you want a little more control over ZS file writing than you
can get with the ``zs make`` command-line utility (see :ref:`zs
make`), you can also access the underlying ZS-writing code directly
from Python by instantiating a :class:`ZSWriter` object.

.. autoclass:: ZSWriter

   .. automethod:: add_data_block

   .. automethod:: add_file_contents

   .. automethod:: finish

   .. automethod:: close

   .. attribute:: closed

      Boolean attribute indicating whether this ZSWriter is closed.
