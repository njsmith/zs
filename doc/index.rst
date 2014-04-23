.. ZS documentation master file, created by
   sphinx-quickstart on Sun Nov 24 18:21:57 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZS -- storing compressed sets
=============================

ZS is a simple, read-only, binary file format designed for
distributing, querying, and archiving arbitarily large data sets (up
to tens of terabytes and beyond) -- so long as those data sets can be
represented as a set of arbitrary binary records. Of course it works
on small data sets too. You can think of it as a replacement for files
stored in tab- or comma-separated format -- each line in such a file
becomes a record in a ZS file. But ZS has a number of advantages over
these traditional formats:

* ZS files are **small**: ZS files (optionally) store data in
  compressed form. The 3-gram counts from the 2012 US English release
  of the `Google N-grams
  <http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_
  are distributed as a set of gzipped text files in tab-separated
  format, and take 1.3 terabytes of space. Uncompressed, this data set
  comes to more than 9 terabytes. The same data in a ZS file with the
  default settings takes just 0.8 terabytes -- more than 35% smaller
  than the current distribution format, and 10x smaller than the raw
  data.

* ZS files are **fast**: Decompression of ZS files can be parallelized
  over multiple CPUs, which is not possible with traditional
  compression formats like gzip. One of the files distributed by
  Google contains specifically the 3-grams which begin with the
  letters "th". Decompressing this file alone takes 30-40 minutes on a
  fast computer. But with the same ZS file described above, the speed
  is limited only by available processor power.

* ZS files are **really, REALLY fast**: Suppose we want to know how
  many times the phrase "this is fun" was used in a Google-scanned
  book published in the USA in 1955. ZS files have a limited indexing
  ability that lets you quickly locate any arbitrary span of records
  that fall within a given sorted range, or share a certain textual
  prefix. This isn't as nice as a full-fledged database system that
  can query on any column, but it can be extremely useful for data
  sets where the first column (or first several columns) are usually
  used for lookup. In our example file, finding the "this is fun"
  entry takes XX disk seeks and less than XX milliseconds of CPU time
  -- call it XX ms all told. (Turns out it was used 27 times.)  When
  this data is stored as gzipped text, then only way to locate an
  individual record, or span of similar records, is start
  decompressing the file from the beginning and wait until the records
  we want happen to scroll by, which in this case -- as noted above --
  can take anywhere up to 40 minutes. Here, ZS is >XX times faster.

* ZS files contain **rich metadata**: In addition to the raw data
  records, every ZS file contains a set of structured metadata
  (specifically, an arbitrary `JSON <http://json.org>`_ document). You
  can use this to record column names, notes on data collection or
  preprocessing steps, recommended citation information, or whatever
  you like, and be confident that it will follow your data whereever
  it goes.

* ZS files are **network friendly**: Suppose you know you just want to
  look up a few individual records that are buried inside that 0.8
  terabyte file, or want a large span of records that are still much
  smaller than the full file (e.g., all 3-grams that begin "this
  is"). With ZS, you don't have to actually download the full 0.8
  terabytes of data; given a URL to the file, the ZS tools can
  efficiently locate and fetch just the parts of the file you need.

* ZS files are **ever-vigilant**: Computer hardware is simply not
  reliable, especially on scales of years and terabytes. We've seen
  RAID cards that would occasionally flip a single bit in the data
  that was being read from disk. How confident are you that this won't
  be a bit that makes a difference in your analysis? Standard text
  files provide no mechanism for detecting data corruption; gzip
  provides some protection, but it's only guaranteed to work if you
  read the entire file from start to finish, every time, and remember
  to check the error code. ZS, by contrast, protects every bit of data
  with 64-bit CRC checksums, and the software we distribute will never
  show you any data that hasn't been double-checked for
  correctness. (Fortunately, this checking is extremely fast; its cost
  is included in all the times quoted above). If it matters to you
  whether your analysis gets the right answer, then ZS is a good
  choice.

* Relying on the ZS format creates **minimal risk**: The ZS file
  format is simple and :ref:`fully documented <format>`_; an average
  programmer with access to standard libraries could write a basic
  decompressor in a few hours. The reference implementation is
  BSD-licensed, undergoes exhaustive automated testing (~99% coverage)
  after every checkin, and includes a complete file format validator,
  so you can confirm that your files match the spec and be confident
  that they will be readable by any compliant implementation.

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
