.. ZS documentation master file, created by
   sphinx-quickstart on Sun Nov 24 18:21:57 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZS - compressed sorted sets
============================

ZS is a simple, read-only, binary file format designed for
distributing, querying, and archiving very large data sets (up to tens
of terabytes and beyond) -- so long as those data sets can be
represented as a set of arbitrary binary records. You can think of it
as a replacement for files stored in tab- or comma-separated format --
each line in such a file becomes a record in a ZS file. But ZS has a
number of advantages over these traditional formats.

* ZS files are **small**: ZS files (optionally) store data in
  compressed form. The 3-gram counts from the 2012 US English release
  of the `Google N-grams
  <http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_
  are distributed as a set of gzipped text files in tab-separated
  format, and take 1.3 terabytes of space. Uncompressed, this data set
  comes to more than 9 terabytes. The same data in a ZS file with the
  default bzip2 compression scheme takes just 0.9 terabytes -- more
  than 30% smaller than the current distribution format.

* ZS files are **fast**: Decompression of ZS files is parallelized
  over multiple CPUs, which is not possible with traditional
  compression formats like gzip. One of the files distributed by
  Google contains specifically the 3-grams which begin with the
  letters "th". Decompressing this file alone takes 30-40 minutes on a
  fast compuer. But with the same ZS file described above, XX

* ZS files are **really, REALLY fast**: Suppose we want to know how
  many times the phrase "this is fun" was used in a Google-scanned
  book in 1955 in the USA. ZS files have a limited indexing ability
  that lets you quickly locate any arbitrary span of records (= lines)
  that fall within a given sorted range, or share a certain textual
  prefix. This isn't as nice as a full-fledged database system that
  can query on any column, but it can be extremely useful for data
  sets where the first column (or first several columns) are usually
  used for lookup. In our example file, finding the "this is fun"
  entry takes XX disk seeks and less than XX milliseconds of CPU time
  -- call it XX ms all told. (Turns out it was used 27 times.) With a
  gzipped text file, the only way to locate an individual record, or
  span of similar records, is start decompressing the file from the
  beginning and wait until the records we want scroll by, which again
  takes anywhere up to 40 minutes. In this case, ZS is >XX times
  faster.

* ZS files contain **rich metadata**: In addition to the raw data
  records, every ZS file contains a set of structured metadata
  (specifically, an arbitrary `JSON <http://json.org>`_ document). You
  can use this to record column names, notes on data collection,
  information on which data set this is exactly, recommended citation
  information, or whatever you like, and be confident that it will
  follow your data where-ever it goes.

* ZS files are **network friendly**: Suppose you know you just want to
  look up a few individual records that are buried inside a 0.9
  terabyte file, or want a large span of records that are still much
  smaller than the full file (e.g., all 3-grams that begin "this
  is"). With ZS, you don't have to actually download the full 0.9
  terabytes of data; given a URL to the file, the ZS tools can
  efficiently locate and fetch just the parts of the file you need.

* ZS files are **ever-vigilant**: Computer hardware is simply not
  reliable, especially on scales of years and terabytes. Standard text
  files provide no way to detect data corruption; gzip provides some
  protection, but it's only guaranteed to work if you read the entire
  file from start to finish, every time, and then check the error
  code. ZS, by contrast, protects every bit of data with 64-bit CRC
  checksums, and validates these on every access (the cost of this is
  included in the times quoted above). If it matters to you whether
  your analysis gets the right answer, then ZS is a good choice.

* Relying on the ZS format creates **minimal risk**: The ZS file
  format is simple and :ref:`fully documented <format>`_; an average
  programmer with access to standard libraries could write a
  functional decompressor in a few hours. The reference implementation
  is BSD-licensed, undergoes exhaustive automated testing (~99%
  coverage) after every checkin, and includes a complete file format
  validator, so you can be sure confirm that your files match the spec
  and will be readable by any compliant implementation.

* ZS files have a name **composed entirely of sibilants**: How many
  file formats can say *that*?

This manual documents the reference implementation of the ZS file
format, which includes both a command-line ``zs`` tool for
manipulating ZS files and a fast and featureful Python API, and also
provides a complete specification of the ZS file format in enough
detail to allow independent implementations.

Contents:

.. toctree::
   :maxdepth: 2

   logistics.rst

   cmdline.rst

   library.rst

   metadata.rst

   format.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
