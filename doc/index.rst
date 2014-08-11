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
on small data sets too. You can think of it as an alternative to
storing data in tab- or comma-separated files -- each line in such a file
becomes a record in a ZS file. But ZS has a number of advantages over
these traditional formats:

.. all measurements on this page use tera/giga/mega in the SI sense,
   i.e., 10^whatever, not 2^whatever.

   actual sizes:
     eng-us-all 3-gram uncompressed: 10165995927614 bytes
        as distributed: 1278862975747 bytes
        zs file with, lzma, -z 0e, 384k block size: 753997475853 bytes

* ZS files are **small**: ZS files (optionally) store data in
  compressed form. The 3-gram counts from the 2012 US English release
  of the `Google N-grams
  <http://storage.googleapis.com/books/ngrams/books/datasetsv2.html>`_
  are distributed as a set of gzipped text files in tab-separated
  format, and take 1.3 terabytes of space. Uncompressed, this data set
  comes to more than 10 terabytes (and would be even more if loaded
  into a database). The same data in a ZS file with the default
  settings (LZMA compression) takes just 0.75 terabytes -- this is
  more than 41% smaller than the current distribution format, and
  13.5x smaller than the raw data.

.. Benchmarks in next paragraph:

   On hericium.ucsd.edu:

      # raw gunzip
      njsmith@hericium:/local/scratch/njs/google_books_ngrams_v2$ gunzip -c googlebooks-eng-us-all-3gram-20120701-th.gz  | pv -rabt > /dev/null
      506GB 0:47:34 [ 181MB/s] [ 181MB/s]

      converted from mebibytes/s to megabytes/s:
        -> 190 megabytes/s

      # --prefix="sc" is a convenient way to get a large-enough sample
      # to be meaningful, small-enough to not take too long, and
      # to be representative of the kinds of actual-text-ngrams that
      # we care about most (as compared to the ngrams that are just
      # punctuation line noise, which may have different compression
      # properties)

      # ZS dump lzma, 8 CPUs (note -j7 b/c this does not count the main thread)
      njsmith@hericium:/local/corpora/google-books-v2/eng-us-all$ time zs dump google-books-eng-us-all-20120701-3gram.zs --prefix="sc" -j 7 | pv -Wrabt | wc -c
      20.1GB 0:00:55 [ 374MB/s] [ 374MB/s]
      21616070118

      real    0m55.567s
      user    5m40.845s
      sys     1m4.348s

      --> 389 megabytes/s (not mebibytes/s)

      # ZS dump lzma, 16 CPUs (which is all of the cores, but there is
      # 2x hyperthreading, so -j16 is not a no-op)
      njsmith@hericium:/local/corpora/google-books-v2/eng-us-all$ time zs dump google-books-eng-us-all-20120701-3gram.zs --prefix="sc" -j 16 | pv -Wrabt | wc -c
      20.1GB 0:00:42 [ 486MB/s] [ 486MB/s]
      21616070118

      real    0m42.971s
      user    6m54.638s
      sys     1m15.189s

      --> 503 megabytes/s (not mebibytes)

      # ZS dump lzma, 1 CPU
      njsmith@hericium:/local/corpora/google-books-v2/eng-us-all$ time zs dump google-books-eng-us-all-2
      0120701-3gram.zs --prefix="sc" -j0 | pv -Wrabt | wc -c
      20.1GB 0:07:14 [47.4MB/s] [47.4MB/s]
      21616070118

      real    7m15.289s
      user    6m35.841s
      sys     0m50.835s

      --> 49.7 megabytes/s (not mebibytes)

      With v0.9.0 of the zs tools, we get ~linear scaling until -j
      reaches something in the 8-10 range -- it looks like the main
      thread becomes the bottleneck here.

     raw disk throughput:
     njsmith@hericium:/local/corpora/google-books-v2/eng-us-all$ pv google-books-eng-us-all-20120701-5gram.zs -rabt >/dev/null
     58.7GB 0:08:18 [63.8MB/s] [ 121MB/s]
     ^C
     -> 126.9 megabytes/s
     and 3x this is = 380 MB/s.

* Nonetheless, ZS files are **fast**: Decompression is an inherently
  slow and serial operation, which means that reading compressed files
  can easily become the bottleneck in an analysis. Google distributes
  the 3-gram counts in many separate ``.gz`` files; one of these, for
  example, contains just the n-grams that begin with the letters
  "th". Using a single core on a handy compute server, we find that
  we can get decompressed data out of this ``.gz`` file at ~190
  MB/s. At this rate, reading this one file takes more than 47
  minutes -- and that's before we even begin analyzing the data inside
  it.

  The LZMA compression used in our ZS file is, on its own, slower than
  gzip. If we restrict ourselves to a single core, then we can only
  read our ZS file at ~50 MB/s. However, ZS files allow for
  multithreaded decompression. Using 8 cores, gunzip runs at... still
  ~190 MB/s, because gzip decompression cannot be parallelized. On
  those same 8 cores, our ZS file decompresses at ~390 MB/s -- a
  nearly linear speedup. This is also ~3x faster than our test server
  can read an *un*\compressed file from disk.

.. Benchmarks in next paragraph:

   On hericium.ucsd.edu, using lzma/384k:

      %timeit list(zs.ZS("google-books-eng-us-all-20120701-3gram.zs", parallelism=0).search(prefix=b"this is fun\t1955\t"))
      10 loops, best of 3: 26.6 ms per loop

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
  entry takes 5 disk seeks and ~25 milliseconds of CPU time --
  something like 85 ms all told. (And hot cache performance -- e.g.,
  when performing repeated queries in the same file -- is even
  better.) The answer, by the way, is 27 books::

      $ zs dump --prefix='this is fun\t1955\t' google-books-eng-us-all-20120701-3gram.zs
      this is fun     1955    27      27

  When this data is stored as gzipped text, then only way to locate an
  individual record, or span of similar records, is start
  decompressing the file from the beginning and wait until the records
  we want happen to scroll by, which in this case -- as noted above --
  could take more than 45 minutes. Using ZS makes this query ~33,000x
  faster.

* ZS files contain **rich metadata**: In addition to the raw data
  records, every ZS file contains a set of structured metadata in the
  form of an arbitrary `JSON <http://json.org>`_ document. You can use
  this to store information about this file's record format (e.g.,
  column names), notes on data collection or preprocessing steps,
  recommended citation information, or whatever you like, and be
  confident that it will follow your data where-ever it goes.

* ZS files are **network friendly**: Suppose you know you just want to
  look up a few individual records that are buried inside that 0.75
  terabyte file, or want a large span of records that are still much
  smaller than the full file (e.g., all 3-grams that begin "this
  is"). With ZS, you don't have to actually download the full 0.75
  terabytes of data. Given a URL to the file, the ZS tools can find
  and fetch just the parts of the file you need, using nothing but
  standard HTTP. Of course going back and forth to the server does add
  overhead; if you need to make a large number of queries then it
  might be faster (and kinder to whoever's hosting the file!) to just
  download it. But there's no point in throwing around gigabytes of
  data to answer a kilobyte question.

  If you have the ZS tools installed, you can try it right now. Here's
  a live trace of the readthedocs.org servers searching the 3-gram
  database stored at UC San Diego. Note that the computer in San
  Diego has no special software installed at all -- this is just a
  static file that's available for download over HTTP::

  .. sneaky hack: we set the TIME envvar in conf.py to get nicer
     output from the 'time' command called here

  .. command-output:: time zs dump --prefix='this is fun\t' http://bolete.ucsd.edu/njsmith/google-books-eng-us-all-20120701-3gram.zs
     :shell:
     :ellipsis: 2,-4

  ..
     If you have the ZS tools installed, you can try it right now. Here's
     a real trace of a computer in Dallas searching the 3-gram database
     stored at UC San Diego. Note that the computer in San Diego has no
     special software installed at all -- this is just a static file
     that's available for download over HTTP::

         $ time zs dump --prefix='this is fun\t' http://bolete.ucsd.edu/njsmith/google-books-eng-us-all-20120701-3gram.zs
         this is fun       1729    1       1
         this is fun       1848    1       1
         ...
         this is fun       2008    435     420
         this is fun       2009    365     352

         Real time elapsed: 1.425 seconds

* ZS files are **splittable**: If you're using a big distributed data
  processing system (e.g. Hadoop), then it's useful to split
  up your file into pieces that approximately match the underlying
  storage chunks, so each CPU can work on locally stored data. This is
  only possible, though, if your file format makes it possible to
  efficiently start reading near arbitrary positions in a file. With
  ZS files, this is possible (though because this requires multiple
  index lookups, it's not as convenient as in file formats designed
  with this as a primary consideration).

* ZS files are **ever-vigilant**: Computer hardware is simply not
  reliable, especially on scales of years and terabytes. I've dealt
  with RAID cards that would occasionally flip a single bit in the
  data that was being read from disk. How confident are you that this
  won't be a key bit that totally changes your results? Standard text files
  provide no mechanism for detecting data corruption. Gzip and other
  traditional compression formats provide some protection, but it's
  only guaranteed to work if you read the entire file from start to
  finish and then remember to check the error code at the end, every
  time. But ZS is different: it protects every bit of data with 64-bit CRC
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
  after every checkin, and just in case there are any ambiguities in
  the English spec, we also have a complete :ref:`file format
  validator <zs validate>`, so you can confirm that your files match
  the spec and be confident that they will be readable by any
  compliant implementation.

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
