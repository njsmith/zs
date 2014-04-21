.. ZSS documentation master file, created by
   sphinx-quickstart on Sun Nov 24 18:21:57 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZSS - compressed sorted sets
============================

ZSS is a simple, read-only, binary file format designed for
distributing, querying, and archiving very large data sets (up to tens
of terabytes and beyond) -- so long as those data sets can be
represented as a set of arbitrary binary records. You can think of it
as a replacement for files stored in tab- or comma-separated format --
each line in such a file becomes a record in a ZSS file. But ZSS has a
number of advantages over these traditional formats.

* ZSS files are **small**: ZSS files (optionally) store data in
  compressed form. The 3-gram counts from the 2012 US English release
  of the `Google N-grams
  <http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_
  are distributed as a set of gzipped text files in tab-separated
  format, and take 1.3 terabytes of space. Uncompressed, this data set
  comes to more than 9 terabytes. The same data in a ZSS file with
  bzip2 compression takes just 0.9 terabytes -- more than 30% smaller
  than the current distribution format.

* ZSS files are **fast**: Decompression of ZSS files is parallelized
  over multiple CPUs, which is not possible with traditional
  compression formats like gzip. As distributed by Google, it takes
  30-40 minutes just to decompress just the one file containing only
  the 3-grams which begin with the letters "th". But with the same ZSS
  file described above, XX

* ZSS files are **really, really fast**: Suppose we want to know how
  many times "this is fun" was used in a Google-scanned book in 1955
  in the USA. ZSS files have a limited indexing ability that lets you
  quickly locate any arbitrary span of records (= lines) that fall
  within a given sorted range, or share a certain textual prefix.
  Finding "this is fun" takes XX disk seeks and less than XX
  milliseconds of CPU time -- call it XX ms all told. (Answer:
  27 times.) With a gzipped text file, the only way to locate an
  individual record, or span of similar records, is start
  decompressing the file from the beginning and wait until the records
  we want scroll by, which again takes anywhere up to 40 minutes. In
  this case, ZSS is >XX times faster.

* ZSS files are (potentially) **self-describing**: In addition to the
  raw data records, every ZSS file contains a set of structured
  metadata (specifically, an arbitrary `JSON <http://json.org>`_
  document). Use this to record column names, notes on data
  collection, recommended citation information, whatever you like.

* ZSS files are **network friendly**: Suppose you know you just want to
  look up a few individual records that are buried inside a 0.9
  terabyte file, or want a large span of records that are still much
  smaller than the full file (e.g., all 3-grams that begin "this
  is"). With ZSS, you don't have to actually download the full 0.9
  terabytes of data; given a URL to the file, the ZSS tools can
  efficiently locate and fetch just the parts of the file you need.

* ZSS files are **robust**: Computer hardware is simply not reliable,
  especially on scales of years and terabytes. Standard text files
  provide no way to detect data corruption; gzip provides some
  protection, but it's only guaranteed to work if you read the entire
  file from start to finish, every time, and then check the error
  code. ZSS, by contrast, protects every bit of data with 64-bit CRC
  checksums, and validates these on every access (the cost of this is
  included in the times quoted above). If it matters to you whether
  your analysis gets the right answer, then ZSS is a good choice.

* ZSS files have a name **composed entirely of sibilants**: How many
  file formats can say *that*?

This manual documents the reference implementation of the ZSS file
format, which includes both a command-line ``zss`` tool for
manipulating ZSS files and a fast and featureful Python API, and also
provides a complete specification of the ZSS file format in enough
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
