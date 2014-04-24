.. _conventions:

Conventions
===========

Our experience is that most data sets have various unique features, so
ZS is an unopinionated format: we give you a hunk of JSON and a pile
of binary records, and let you figure out what to put in them. But, it
is nice to have some conventions for how to handle common
situations. As more people use the format, these will probably evolve,
but for now here are some notes.

.. _metadata-conventions:

Metadata
--------

[XX document the metadata being used in the current gbooks files]::

    "build-info": {
        "host": "morel.ucsd.edu",
        "user": "njsmith",
        "time": "2014-04-21T23:56:47.225267Z"
    },
    "corpus": "google-books-eng-us-all-20120701",
    "subset": "2gram",
    "record-format": {
        "separator": "\t",
        "column-types": [
            "utf8",
            "int",
            "int",
            "int"
        ],
        "type": "separated-values",
        "column-names": [
            "ngram",
            "year",
            "match_count",
            "volume_count"
        ]
    }

Some other items you might want to consider including:

* Information on the preprocessing pipeline that led to this file (for
  answering questions like, "is this the version that had
  case-sensitivity enabled, or disabled?")

* Bibliographic references for papers that users of this data might
  want to refer to or cite.

* Your contact information.

* Any relevant DOIs or `ORCIDs <http://orcid.org/>`_.


.. _record-format-conventions:

Record format
-------------

The ZS format itself puts absolutely no limitations on the contents of
individual records. You can encode your data any way you feel
like. However, because indexing is done by ASCIIbetical sort order,
it's probably a good idea to choose an encoding which makes sort order
meaningful. Some general principles:

* **Put the field you want to index on first**; if you want to be able to
  index on multiple fields simultaneously, put them first, second,
  third, etc.

* **Beware of quoting.** This arises especially for common CSV formats,
  where fields containing the characters ``,`` or ``"`` often get
  special handling. For example, suppose we have some nicely organized
  n-grams::

      to be , or not to be
      to be disjoint and out of frame
      'Tis Hamlet 's character . " Naked ! "
      'Tis now the very witching time
      What a piece of work is a man !

  If we encode these as a column in CSV format, and then sort, we end
  up with::

      "'Tis Hamlet 's character . "" Naked ! """
      "to be , or not to be"
      'Tis now the very witching time
      What a piece of work is a man !
      to be disjoint and out of frame

  Notice that every entry that contained a ``,`` or ``"`` has been
  wrapped in ``"``\'s. If we want to find n-grams beginning ``to be``
  or ``'Tis`` then a simple prefix search will no longer work; when we
  want to find records with the prefix ``foo`` we have to remember
  always to search for both ``foo`` and ``"foo``.

  Ideally there is some character that you know will never occur in
  any field, and you can use that for your separator -- then no
  quoting is ever needed. This might be tab (\t), or if you get
  desperate then there are other options like NUL (\00) or newline
  (\n) -- though with these latter options you'll lose some of the
  convenience of browsing your data with simple tools like :ref:`zs
  dump`, and may have to play around a bit more with :ref:`zs make`'s
  options to construct your file in the first place.

  Alternatively, other quoting schemes (e.g., replacing ``,`` with
  ``\\,`` and ``\\`` with ``\\\\``) may not perfectly preserve
  sorting, but they do preserve prefix searches, which is often the
  important thing.

* **Beware of standard number formats.** String-wise, ``"10"`` is less
  than ``"2"``, which is a problem if you want to be able to do range
  queries on numeric data in ZS files. Some options for working around
  this include using fixed-width strings (``"10"`` and ``"02"``), or
  using some kind of big-endian binary encoding. Note that the ASCII
  space character (0x20) sorts before all printing characters,
  including digits. This means that instead of padding with zeroes
  like in ``"02"``, it will also work to pad with spaces, ``"
  2"``. Fixed width formats in general can be cumbersome to work with,
  but they do have excellent sorting properties.

  In the Google n-grams, the year field fortunately turns out to be
  fixed width (at least until Google starts scanning papyruses). And
  for the actual count fields, this formatting issue doesn't arise,
  because we have no reason to index on them.

* **Beware of little-endian Unicode and surrogate pairs.** ASCII,
  UTF-8, and UTF-32BE all have sensible sort orders (i.e.,
  ASCIIbetical sort on the encoded strings is the same as
  lexicographic sort on code points). This is definitely not true for
  UTF-16LE or UTF-32LE, and is not *quite* true for UTF-16BE, because
  of the existence of surrogate pairs (`see
  e.g. <https://ssl.icu-project.org/docs/papers/utf16_code_point_order.html>`_).

  Of course, if all you want are exact prefix searches, then these issues
  don't really matter.

  We recommend using UTF-8 unless you have a good reason not to.

  Note that the ``zs`` command-line tool has a mild bias towards
  UTF-8, in that if you pass it raw Unicode characters for
  ``--start``, ``--stop``, or ``--prefix``, then it encodes them as
  UTF-8 before doing the search.

If these issues turn out to cause enough problems, it may makes sense
at some point to define a revised version of the ZS format which has
an explicit schema for record contents, and uses a content-sensitive
sort order (e.g., one that performs numeric comparison on numeric
fields).
