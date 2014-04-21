The command-line ``zss`` tool
=============================

The ``zss`` tool can be used from the command-line to create, view,
and check ZSS files.

The main ``zss`` command on its own isn't very useful -- it can tell
you check what version you have:

.. command-output:: zss --version

And it can tell you what subcommands are available:

.. command-output:: zss --help

These subcommands are documented further below.

.. note:: In case you have the Python :mod:`zss` package installed,
  but somehow do not have the ``zss`` executable available on your
  path, then it can also be invoked as ``python -m zss``. E.g., these
  two commands do the same thing::

      $ zss dump myfile.zss
      $ python -m zss dump myfile.zss

.. _zss make:

``zss make``
------------

``zss make`` allows you to create ZSS files. In its simplest form, it
just reads in a text file, and writes out a ZSS file, treating each
line as a separate record.

For example, if we have this data file (a tiny excerpt from the `Web
1T <http://catalog.ldc.upenn.edu/LDC2006T13>`_ dataset released by
Google; note that the last whitespace in each line is a tab
character):

.. command-output:: cat tiny-4grams.txt
   :cwd: example/scratch

Then we can compress it into a ZSS file by running:

.. Note that if you change this command, then you should also update
   the copy of tiny-4grams.zss that is stored in the example/
   directory, so that the rest of the examples in the documentation
   will match:

.. command-output:: zss make '{"corpus": "doc-example"}' tiny-4grams.txt tiny-4grams.zss
   :cwd: example/scratch
   :shell:

The first argument specifies some arbitrary metadata that will be
saved into the ZSS file, in the form of a `JSON <http://json.org>`_
string, the second argument names the file we want to convert, and the
third argument names the file we want to create.

.. note:: You must ensure that your file is sorted before running
   ``zss make``. (If you don't, then it will error out and scold you.)
   GNU sort is very useful for this task -- but don't forget to set
   ``LC_ALL=C`` in your environment before calling sort, to make sure
   that it uses ASCIIbetical ordering instead of something
   locale-specific.

   When your file is too large to fit into RAM, GNU sort will spill
   the data onto disk in temporary files. When your file is too large
   to fit onto disk, then a useful incantation is::

       gunzip -c myfile.gz | env LC_ALL=C sort --compress-program=lzop \
          | zss make "{...}" - myfile.zss

   The ``--compress-program`` option tells sort to automatically
   compress and decompress the temporary files using the ``lzop``
   utility, so that you never end up with uncompressed data on
   disk. (``gzip`` also works, but will be slower.)

Many other options are also available:

.. command-output:: zss make --help

.. _zss info:

``zss info``
------------

``zss info`` displays some general information about a ZSS file. For example:

.. command-output:: zss info tiny-4grams.zss
   :cwd: example/

The most interesting part of this output might be the ``"metadata"``
field, which contains arbitrary metadata describing the file. Here we
see that our custom key was indeed added, and that ``zss make`` also
added some default metadata. (If we wanted to suppress this we could
have used the ``--no-default-metadata`` option.) The ``"data_sha256"``
field is, as you might expect, a `SHA-256
<https://en.wikipedia.org/wiki/SHA-256>`_ hash of the data contained
in this file -- two ZSS files will have the same value here if and
only if they contain exactly the same logical records, regardless of
compression and other details of physical file layout. The ``"codec"``
field tells us which kind of compression was used (this file uses the
`bzip2 format <https://en.wikipedia.org/wiki/Bzip2>`_); if we wanted
something different we could have passed ``--codec`` to ``zss
make``. The other fields have to do with more obscure technical
aspects of the ZSS file format; see the documentation for the
:class:`ZSS` class and the :ref:`file format specification <format>`
for details.

``zss info`` is fast, even on arbitrarily large files, because it
looks at only the header and the root index; it doesn't have to
uncompress the actual data. If you find a large ZSS file on the web
and want to see its metadata before downloading it, you can pass an
HTTP URL to ``zss info`` directly on the command line, and it will
download only as much of the file as it needs to.

``zss info`` doesn't take many options:

.. command-output:: zss info --help

.. _zss dump:

``zss dump``
------------

So ``zss info`` tells us *about* the contents of a ZSS file, but how
do we get our data back out? That's the job of ``zss dump``.  In the
simplest case, it simply dumps the whole file to standard output, with
one record per line -- the inverse of ``zss make``. For example, this
lets us "uncompress" our ZSS file to recover the original file:

.. command-output:: zss dump tiny-4grams.zss
   :cwd: example/

But we can also extract just a subset of the data. For example, we can
pull out a single line (notice the use of ``\t`` to specify a tab
character -- Python-style backslash character sequences are fully
supported):

.. command-output:: zss dump tiny-4grams.zss --prefix="not done extensive testing\t"
   :cwd: example/

Or a set of related ngrams:

.. command-output:: zss dump tiny-4grams.zss --prefix="not done extensive "
   :cwd: example/

Or any arbitrary range:

.. command-output:: zss dump tiny-4grams.zss --start="not done ext" --stop="not done fast"
   :cwd: example/

Just like ``zss info``, ``zss dump`` is fast -- it reads only the data
it needs to to satisfy your query. (Of course, if you request the
whole file, then it will read the whole file -- but it does this in an
optimized way; see the ``-j`` option if you want to tune how many CPUs
it uses for decompression.) And just like ``zss info``, ``zss dump``
can directly take an HTTP URL on the command line, and will download
only as much data as it has to.

We also have several options to let us control the output format. ZSS
files allow records to contain arbitrary data, which means that it's
possible to have a record that contains a newline embedded in
it. So we might prefer to use some other character to mark the ends of
records, like `NUL <https://en.wikipedia.org/wiki/Null_character>`_::

$ zss dump tiny-4grams.zss --terminator="\x00"

...but putting the output from that into these docs would be hard to
read. Instead we'll demonstrate with something sillier:

.. command-output:: zss dump tiny-4grams.zss --terminator="XYZZY" --prefix="not done extensive "
   :cwd: example/

Of course, this will still have a problem if any of our records
contained the string "XYZZY" -- in fact, our records could in theory
contain *anything* we might choose to use as a terminator, so if we
have an arbitrary ZSS file whose contents we know nothing about, then
none of the options we've seen so far is guaranteed to work. The
safest approach is to instead use a format in which each record is
explicitly prefixed by its length. ``zss dump`` can produce
length-prefixed output with lengths encoded in either u64le or uleb128
format (see :ref:`integer-representations` for details about what
these are).

.. command-output:: zss dump tiny-4grams.zss --prefix="not done extensive " --length-prefixed=u64le | hd
   :cwd: example/
   :shell:

Obviously this is mostly intended for when you want to read the data
into another program. For example, if you have a ZSS file that was
compressed using the bz2 codec and you want to convert it to the
deflate codec, the easiest and safest way to do that is with a command
like::

    $ zss dump --length-prefixed=uleb128 myfile-bz2.zss | \
      zss make --length-prefixed=uleb128 --codec=deflate \
          "$(zss info -m myfile-bz2.zss)" - myfile-deflate.zss

If you're using Python, of course, the most convenient way to read a
ZSS file is not to use ``zss dump`` at all, but to use the :mod:`zss`
library API directly.

Full options:

.. command-output:: zss dump --help

.. _zss validate:

``zss validate``
----------------

This command can be used to fully validate a ZSS file for
self-consistency and compliance with the specification (see
:ref:`format`); this makes it rather useful to anyone trying to write
new software to generate ZSS files.

It is also useful because it verifies the SHA-256 checksum and all of
the per-block checksums, providing extremely strong protection against
errors caused by disk failures, cosmic rays, and other such
annoyances. However, this is not usually necessary, since the ``zss``
commands and the :mod:`zss` library interface never return any data
unless it passes a 64-bit checksum. With ZSS you can be sure that your
results have not been corrupted by hardware errors, even if you never
run ``zss validate`` at all.

Full options:

.. command-output:: zss validate --help
