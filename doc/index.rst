.. ZS documentation master file, created by
   sphinx-quickstart on Sun Nov 24 18:21:57 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZS: a file format for compressed sets
=====================================

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
  comes to more than 9 terabytes (and would be even more if loaded
  into a database). The same data in a ZS file with the default
  settings (bzip2 compression) takes just 0.8 terabytes -- more than
  35% smaller than the current distribution format, and 11x smaller
  than the raw data.

.. Benchmarks in next paragraph:

   On hericium.ucsd.edu:

      # raw gunzip
      njsmith@hericium:/local/scratch/njs/google_books_ngrams_v2$ gunzip -c googlebooks-eng-us-all-3gram-20120701-th.gz  | pv -rabt > /dev/null
      506GB 0:47:34 [ 181MB/s] [ 181MB/s]

      # ZS dump bz2, 32 CPUs
      njsmith@hericium:/local/scratch/njs/google_books_ngrams_v2$ zs dump google-books-eng-us-all-2012070
      1-3gram.zs --prefix="th" | pv -rabt >/dev/null
      25.6GB 0:01:25 [ 291MB/s] [ 308MB/s]]
      ^C

      # ZS dump bz2, 8 CPUs (note -j7 b/c this does not count the main thread)
      njsmith@hericium:/local/scratch/njs/google_books_ngrams_v2$ zs dump -j7 google-books-eng-us-all-201
      20701-3gram.zs --prefix="th" | pv -rabt >/dev/null
      25.9GB 0:02:11 [ 239MB/s] [ 210MB/s]
      ^C

* Nonetheless, ZS files are **fast**: ZS files allow decompression to
  be parallelized over multiple CPUs, which is not possible with
  traditional compression formats like gzip. And this is important,
  because decompression is a slow and inherently serial
  operation. Using a fast compute server for measurements, we found
  that gunzip can spit out 3-gram data at ~180 MiB/s. Google
  distributes these counts in many separate files; one of these, for
  example, contains just the 3-grams that begin with the letters "th".
  On our compute server, decompressing just this one file takes 47
  minutes.

  Bzip2 decompression on its own is quite a bit slower than gunzip,
  but with 8 CPUs and using Python's relatively crude parallelization
  facilities, the same server can decompress our smaller ZS file at
  210 MiB/s, and a careful implementation should scale nearly linearly
  with the number of CPUs. And of course, we have the option of
  choosing many different locations on the space/speed tradeoff curve:
  if we used gzip compression in our ZS file, it'd be roughly the same
  size as the current distribution format, but would decompress
  multiple times faster; or with LZMA compression (which will probably
  become the ZS default soon), decompression will be roughly twice as
  fast as for bzip2, and produce even smaller files.

.. Benchmarks in next paragraph:

   On hericium.ucsd.edu, using file with bz2 codec:

       %timeit list(zs.ZS("google-books-eng-us-all-20120701-3gram.zs", parallelism=0).search(prefix=b"this is fun\t1955\t"))
       10 loops, best of 3: 21.6 ms per loop

   "5 disk seeks" = 'zs info' says root level is 3

   Speedup:

* In fact, ZS files are **really, REALLY fast**: Suppose we want to
  know how many different Google-scanned books published in the USA in
  1955 used the phrase "this is fun". ZS files have a limited indexing
  ability that lets you quickly locate any arbitrary span of records
  that fall within a given sorted range, or share a certain textual
  prefix. This isn't as nice as a full-fledged database system that
  can query on any column, but it can be extremely useful for data
  sets where the first column (or first several columns) are usually
  used for lookup. Using our example file, finding the "this is fun"
  entry takes 5 disk seeks and ~20 milliseconds of CPU time --
  something like 80 ms all told. (And hot cache performance -- e.g.,
  when performing repeated queries in the same file -- is even
  better.) The answer, by the way, is 27 books::

      $ zs dump --prefix='this is fun\t1955\t' google-books-eng-us-all-20120701-3gram.zs
      this is fun     1955    27      27

  When this data is stored as gzipped text, then only way to locate an
  individual record, or span of similar records, is start
  decompressing the file from the beginning and wait until the records
  we want happen to scroll by, which in this case -- as noted above --
  could take more than 45 minutes. Using ZS makes this query ~35,000x
  faster.

* ZS files contain **rich metadata**: In addition to the raw data
  records, every ZS file contains a set of structured metadata in the
  form of an arbitrary `JSON <http://json.org>`_ document. You can use
  this to store information about this file's record format (e.g.,
  column names), notes on data collection or preprocessing steps,
  recommended citation information, or whatever you like, and be
  confident that it will follow your data where-ever it goes.

* ZS files are **network friendly**: Suppose you know you just want to
  look up a few individual records that are buried inside that 0.8
  terabyte file, or want a large span of records that are still much
  smaller than the full file (e.g., all 3-grams that begin "this
  is"). With ZS, you don't have to actually download the full 0.8
  terabytes of data. Given a URL to the file, the ZS tools can find
  and fetch just the parts of the file you need, using nothing but
  standard HTTP. Of course going back and forth to the server does add
  overhead; if you need to make a large number of queries then it
  might be faster (and kinder to whoever's hosting the file!) to just
  download it. But there's no point in throwing around gigabytes of
  data to answer a kilobyte question.

  Try it yourself:

  .. sneaky hack: we set the TIME variable in conf.py to get nicer
     output from the 'time' command called here

  .. command-output:: time zs dump --prefix='this is fun\t' http://bolete.ucsd.edu/njsmith/google-books-eng-us-all-20120701-3gram.zs
     :shell:
     :ellipsis: 2,-4

* ZS files are **ever-vigilant**: Computer hardware is simply not
  reliable, especially on scales of years and terabytes. I've dealt
  with RAID cards that would occasionally flip a single bit in the
  data that was being read from disk. How confident are you that this
  won't be a bit that changes your results? Standard text files
  provide no mechanism for detecting data corruption. Gzip and other
  traditional compression formats provide some protection, but it's
  only guaranteed to work if you read the entire file from start to
  finish and then remember to check the error code at the end, every
  time. ZS, by contrast, protects every bit of data with 64-bit CRC
  checksums, and the software we distribute will never show you any
  data that hasn't first been double-checked for
  correctness. (Fortunately, the cost of this checking is negligible;
  all the times quoted above include these checks). If it matters to
  you whether your analysis gets the right answer, then ZS is a good
  choice.

* Relying on the ZS format creates **minimal risk**: The ZS file
  format is simple and :ref:`fully documented <format>`; an average
  programmer with access to standard libraries could write a working
  decompressor in a few hours. The reference implementation is
  BSD-licensed, undergoes exhaustive automated testing (>98% coverage)
  after every checkin, and includes an exhaustive file format
  validator, so you can confirm that your files match the spec and be
  confident that they will be readable by any compliant
  implementation.

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

   conventions.rst

   datasets.rst

   format.rst

   changes.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
