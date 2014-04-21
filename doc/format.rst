.. _format:

On-disk layout of ZSS files
===========================

This page provides a complete specification of the ZSS file format,
along with rationale for specific design choices. It should be read by
anyone who plans to implement a new reader or writer for the
format, or is just interested in how things work under the covers.

Overview and general notes
--------------------------

ZSS is a read-only database format designed to store a `multiset
<https://en.wikipedia.org/wiki/Multiset>`_ of records, where each
record is an uninterpreted string of binary data. The main design
goals are:

* Locating an arbitrary record, or sorted span of records, should be
  fast.
* Doing a streaming read of a large span of records should be fast.
* Hardware is unreliable, especially on the scale of terabytes and
  years, and ZSS is designed for long-term archival of multi-terabyte
  data. Therefore it must be possible to quickly and reliably validate
  the integrity of the data returned by every operation.
* It should be reasonably efficient to access files over slow, "dumb"
  transports like HTTP.
* Files should be as small as possible while achieving the above
  goals.

The main complication influencing ZSS's design is that compression is
necessary to achieve reasonable storage sizes, but decompression is
slow, block-oriented, and inherently serial, which puts the last goal
in direct conflict with the first two. Compressing a chunk of data is
like wrapping it up into an opaque bundle. The only way to find
something inside is to first unwrap (decompress) the whole thing. This
is why it won't work to simply write our data into a large text file
and then use a standard compression program like ``gzip`` on the whole
thing. If we did this, then the only way to find any piece of data
would be to decompress the whole file, which takes ages. Instead, we
need some way to split our data up into multiple smaller bundles. Once
we've done this, reading individual records can be fast, because we
only have to unwrap a single small bundle, not a huge one. And, it
turns out, splitting up our data into multiple bundles also makes bulk
reads faster. For a large read, we have to unpack the same amount of
total data regardless of whether it's divided into small bundles or
not, so the total work is constant. But, in the multiple-bundle case,
we can easily divvy up this work across multiple CPUs, and thus finish
the job more quickly. So, small bundles are great -- but, they also
have a downside: if we make our bundles too small, then the
compression algorithm won't be able to find many redundancies to
compress out, and so our compression ratio will not be very good. In
particular, trying to compress individual records would be hopeless.

Our solution is to bundle records together into moderately-sized
blocks, and then compress each block. Then we add some framing to let
us figure out where each block starts and ends, and add an index
structure to let us quickly find which blocks contain records that
match some query, and ta-da, we have a ZSS file. The resulting
structure looks like this:

.. image:: /figures/format-overview.*
   :width: 100%

Fast lookup for arbitrary records is supported by a tree-based
indexing scheme: the header contains a pointer to the "root" index
block, which in turn refers to other index blocks, which refer to
other index blocks, until eventually the lowest-level index blocks
refer to data blocks. By following these links, we can locate any
arbitrary record in :math:`O(\log n)` time.

In addition, we require data blocks to be arranged in sorted order
within the file. This allows us to do streaming reads starting from
any point, which makes for nicely efficient disk access patterns. And
range queries are supported by combining these two access strategies:
first we traverse the index to figure out which blocks contain records
that fall into our range, and then we do a streaming read across these
blocks.

Checksumming
------------

To achieve our data integrity goals, every byte in the file that could
possibly contain undetected corruption is protected by a 64-bit
CRC. Specifically, we use the same CRC-64 calculation that the `.xz
file format <http://tukaani.org/xz/xz-file-format.txt>`_ does. The
`Rocksoft model <http://www.ross.net/crc/crcpaper.html>`_ parameters
for this CRC are: polynomial = 0x42f0e1eba9ea3693, reflect in = True,
xor in = 0xffffffffffffffff, reflect out = True, xor out =
0xffffffffffffffff, check = 0x995dc9bbdf1939fa.

.. _integer-representations:

Integer representations
-----------------------

Within the header, we make life easier for simple tools like `file
<https://en.wikipedia.org/wiki/File_%28command%29>`_ by encoding all
integers using fixed-length 64-bit little-endian format (``u64le`` for
short).

Outside of the header, integers are encoded in the *uleb128* format,
familiar from the `DWARF debugging format
<https://en.wikipedia.org/wiki/DWARF>`_. Okay, maybe not so
familiar. This is a simple variable-length encoding for unsigned
integers of arbitrary size using **u**\nsigned **l**\ittle-**e**\ndian
**b**\ase-**128**. To read a uleb128 value, you proceed from the
beginning of the string, one byte at a time. The lower 7 bits of each
byte give you the next 7 bits of your integer. This is little-endian,
so the first byte gives you the least-significant 7 bits of your
integer, then the next byte gives you bits 8 through 15, the one after
that the bits 16 through 23, etc. The 8th, most-significant bit of
each byte serves as a continuation byte. If this is 1, then you keep
going and read the next byte. If it is 0, then you are
done. Examples::

  uleb128 string  <->  integer value
  --------------       -------------
              00                0x00
              7f                0x7f
           80 01                0x80
           ff 20              0x107f
  80 80 80 80 20             2 ** 33

(This format is also used by `protocol buffers
<https://en.wikipedia.org/wiki/Protocol_Buffers>`_.) This format
allows for redundant representations by adding leading zeros, e.g. the
value 0 could also be written ``80 00``. However, doing this is
forbidden; all values must be encoded in their shortest form.

Layout details
--------------

Here's the big picture -- refer to it while reading the full details
below.

.. image:: /figures/format-details.*
   :width: 100%

ZSS files consist of a *magic number*, followed by a *header*, followed by
a sequence of *blocks*. Blocks come in two types: *data blocks*, and
*index blocks*.

.. _magic-numbers:

Magic number
''''''''''''

To make it easy to distinguish ZSS files from non-ZSS files, every
valid ZSS file begins with 8 `magic bytes
<https://en.wikipedia.org/wiki/File_format#Magic_number>`_. Specifically,
these ones (written in hex)::

  5a 53 53 1c 8e 6c 00 01    # Good magic

This is the ascii string ``ZSS``, followed by 3 random bytes,
followed by two bytes which might be used as a version identifier in
case there is ever a ZSS version 2.

Writing out a large ZSS file is a somewhat involved operation that
might take a long time. It's possible for a hardware or software
problem to occur and cause this process to be aborted before the file
is completely written, leaving behind a partial, corrupt ZSS
file. Because ZSS is designed as a reliable archival format we would
like to avoid the possibility of confusing a corrupt file with a
correct one, and because writing ZSS files can be slow, after a crash
we would like to be able to reliably determine whether the writing
operation had completed, to know whether we can trust the file left
behind. Therefore we also define a second magic number to be used
specifically for partial ZSS files::

  53 53 5a 1c 8e 6c 00 01    # Bad magic

This is the same as the regular magic value, except that the string
``ZSS`` has been replaced by ``SSZ``.

It is strongly recommended that ZSS file writers perform the following
sequence:

* Write out the ``SSZ`` magic number.
* Write out the rest of the ZSS file.
* Update the header to its final form (including, e.g., the offset of
  the root block).
* (IMPORTANT) Sync the file to disk using ``fsync()`` or equivalent.
* Replace the ``SSZ`` magic number with the correct ``ZSS`` magic
  number.

Following this procedure guarantees that, modulo disk corruption, any
file which begins with the correct ZSS magic will in fact be a
complete, valid ZSS file.

Any file which does not begin with the correct ZSS magic is not a
valid ZSS file, and should be rejected by ZSS file readers. Files with
the ``SSZ`` magic are not valid ZSS files. However, polite ZSS readers
should generally check for the ``SSZ`` magic, and if encountered,
provide a more informative error message while rejecting the file.


.. _format-header:

Header
''''''

The header contains the following fields, in order:

* Length (``u64le``): The length of the data in the header. This does
  not include either the length field itself, or the trailing CRC --
  see diagram.

* Root index offset (``u64le``): The position in the file where the
  root index block begins.

* Root index length (``u64le``): The number of bytes in the root index
  block. This *includes* the root index block's length and CRC fields;
  the idea is that doing a single read of this length, at the given
  offset, will give us the root index itself. This is an important
  optimization when IO has high-latency, as when accessing a ZSS file
  over HTTP.

* Total file length (``u64le``): The total number of bytes contained
  in this ZSS file; the same thing you'd get from ``ls -l`` or
  similar.

   .. warning:: To guarantee data integrity, it is important for
      readers to validate the file length field; our CRC checks alone
      cannot detect file truncation if it happens to coincide with a
      block boundary.

* SHA-256 of data (32 bytes): The SHA-256 hash of the stream one would
  get by extracting all data block payloads and concatenating
  them. The idea is that this value uniquely identifies the logical
  contents of a ZSS file, regardless of storage details like
  compression mode, block size, index fanout, etc.

* Codec (16 bytes): A null-padded string specifying the codec
  (compression method) used. Currently defined codecs include:

  * ``none``: Block payloads are stored in raw, uncompressed form.

  * ``deflate``: Block payloads are stored using the deflate format as
    defined in `RFC 1951 <https://tools.ietf.org/html/rfc1951>`_. Note
    that this is different from both the gzip format (RFC 1952) and
    the zlib format (RFC 1950), which use different framing and
    checksums. ZSS provides its own framing and checksum, so we just
    use raw deflate streams.

  * ``bz2``: Block payloads are compressed using `the bzip2 format
    <https://en.wikipedia.org/wiki/Bzip2>`_. Unfortunately there is no
    easy way to get a raw, unframed bzip2 stream with commonly
    available libraries, so using this method adds 10-20 bytes of
    extra framing overhead. Fortunately the improved compression
    usually more than makes up for this.

* Metadata length (``u64le``): The length of the next field:

* Metadata (UTF-8 encoded JSON): This field allows arbitrary metadata
  to be attached to a ZSS file. The only restriction is that the
  encoded value must be what JSON calls an "object" (also known as a
  dict, hash table, etc. -- basically, the outermost characters have
  to be ``{}``). But this object can contain arbitrarily complex
  values (though we recommend restricting yourself to strings for the
  keys). See :ref:`metadata-conventions`.

* <extensions> (??): Compliant readers should ignore any data
  occurring between the end of the metadata field and the end of the
  header (as defined by the header length field). This space may be
  used in the future to add backwards-compatible extensions to the ZSS
  format. (Backwards-incompatible extensions, of course, will include
  a change to the magic number.)

* CRC-64xz (``u64le``): A checksum of all the header data. This does
  not include the length field, but does include everything between it
  and the CRC. See diagram.

Blocks
''''''

Blocks themselves all have the same format:

* Length (``uleb128``): The length of the data in the block. This does
  not include either the length field itself, or the trailing CRC --
  see diagram.

* Level (``u8``): A single byte encoding the "level" of this
  block. Data blocks are level 0. Index blocks can have any level
  between 1 and 63 (inclusive). Other levels are reserved for future
  backwards-compatible extensions; compliant readers must silently
  ignore any block with its level field set to 64 or higher.

* Compressed payload (arbitrary data): The rest of the block after the
  level is a compressed representation of the payload. This should be
  decompressed according to the value of the codec field in the
  header, and then interpreted according to the rules below.

* CRC-64xz (``u64le``): CRC of the data in the block. This does not
  include the length field -- see diagram. Note that this is
  calculated directly on the raw disk representation of the block,
  compression and all.

Technically we don't need to store the length at the beginning of each
block, because every block also has its length stored either in an
index block or (for the root block) in the header. But, storing the
length directly at the beginning of each block makes it much simpler
to write simple streaming decoders, reduces seeks during streaming
reads, and adds negligible space overhead.

Data block payload
''''''''''''''''''

Data block payloads encode a list of records. Each record has the
form:

* Record length (``uleb128``): The number of bytes in this record.

* Record contents (arbitrary data): That many bytes of data, making up
  the contents of this record.

Then this is repeated as many times as you want.

Every data block must contain at least one record.

Index block payload
'''''''''''''''''''

Index block payloads encode a list of references to other index or
data blocks.

Each index payload entry has the form:

* Key length (``uleb128``): The number of bytes in the "key".

* Key value (arbitrary data): That many bytes of data, making up the
  "key" for the pointed-to block. (See below for the invariants this
  key must satisfy.)

* Block offset (``uleb128``): The file offset at which the pointed-to
  block is located.

* Block length (``uleb128``): The length of the pointed-to block. This
  *includes* the root index block's length and CRC fields; the idea is
  that doing a single read of this length, a the given offset, will
  give us the root index itself. This is an important optimization
  when IO has high-latency, as when accessing a ZSS file over HTTP.

Then this is repeated as many times as you want.

Every key block must contain at least one entry.

Key invariants
--------------

All comparisons here use ASCIIbetical order, i.e., lexicographic
comparisons on raw byte values, as returned by ``memcmp()``.

We require:

* The records in each data block payload must be listed in sorted order.

* If data block A occurs earlier in the file (at a lower offset) than
  data block B, then all records in A must be less-than-or-equal-to
  all records in B.

* Every block, except for the root block, is referenced by exactly one
  index block.

* An index block of level :math:`n` must only reference blocks of
  level :math:`n - 1`. (Data blocks are considered to have level 0.)

* The keys in each index block payload must occur in sorted order.

* To every block, we assign a span of records as follows: data blocks
  span the records they contain. Index blocks span all the records
  that are spanned by the blocks that they point to
  (recursively). Given this definition, we can state the key invariant
  for index blocks: every index key must be less-than-or-equal-to the
  *first* record which is spanned by the pointed-to block, and must be
  greater-than-or-equal-to all records which come before this record.

  .. note:: According to this definition, it is always legal to simply
     take the first record spanned by a block, and use that for its
     key. But we do not guarantee this; advanced implementations might
     take advantage of this flexibility to choose shorter keys that are
     just long enough to satisfy the invariant above. (In particular,
     there's nothing in ZSS stopping you from having large individual
     records, up into the megabyte range and beyond, and in this case
     you might well prefer not to copy the whole record into the index
     block.)

Notice that all invariants use non-strict inequalities; this is
because the same record might occur multiple times in different
blocks, making strict inequalities impossible to guarantee.

Notice also that there is no requirement about where index blocks
occur in the file, though in general each index will occur after the
blocks it points to, because unless you are very clever you can't
write an index block until you know the pointed-to blocks' disk
offsets.
