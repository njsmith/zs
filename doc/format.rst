On-disk format for ZSS files
============================

TODO: should we add

Overview and general notes
--------------------------

A ZSS file consists of a *header* followed by a sequence of
*blocks*. Blocks are further divided into data blocks and index
blocks. The basic idea is that each data block contains a bunch of key
entries, in sorted order, and the index blocks form a tree over these
data blocks allowing for O(log n) lookup.

File locations are given by *voffsets*, which count from the beginning
of the first block, immediately after the header (not the beginning of
the file itself). This is mostly a vestige of an earlier version of
this format which supported multi-file databases, but could possibly
be useful if one ever wants to change the length of a header (e.g. by
modifying the free-form metadata it contains), without having to
decompress and recompress the whole file.

Header
------

Within the header, integers are encoded using fixed-length fields, to
make life easy for simple tools like `file
<https://en.wikipedia.org/wiki/File_%28command%29>`_.

A ZSS file header contains the following fields:

* 8 bytes: Magic number. For a valid ZSS file, this is, in hex::

    5a 53 53 1c 8e 6c 00 01

  which can be broken down into: the ascii string "ZSS", 3 random
  bytes for uniqueness, and a two-byte big-endian version
  number. Currently only version 1 is defined.

  For an incomplete ZSS file that is in the process of being written,
  the header should instead be::

    53 53 5a 1c 8e 6c 00 01

  (i.e., the "ZSS" is replaced by "SSZ"). This ensures that an
  incomplete ZSS file can never be mistaken for a complete one. When
  writing a ZSS file, it is recommended that the very last steps be to
  sync the data to disk, and then rewrite the header magic.

* 4 bytes: Length of the header, as a little-endian 32-bit unsigned
  integer.

* *N* bytes, where *N* is the number of bytes in the header length
  field: The header proper, containing:

  * 8 bytes: The voffset of the *root index block*, the block which
    (directly or indirectly) contains pointers to all other
    blocks. This will generally be the last block in the file, because
    we can't know where all the other blocks are until we've written
    them.

  * 16 bytes: A `UUID
    <https://en.wikipedia.org/wiki/Universally_unique_identifier>`_ as
    specified in RFC 4122 which uniquely identifies this file. Stored
    in raw binary form.

  * 16 bytes: A null-padded string identifying the storage algorithm
    used for the blocks in this ZSS file. Currently defined values::

      zlib
      bzip2
      none+crc32c

    ``zlib`` and ``bzip2`` are self-explanatory; each block's data is
    stored using the native format of these libraries (which include
    checksums natively). The last option is used for storing
    uncompressed ZSS files. In this format, "compression" consists of
    simply returning the raw data, with a 4 byte little-endian CRC32C
    value appended.

  * 4 bytes: Length of the extensible metadata section.

  * *M* bytes: The extensible metadata section. This contains an
    arbitrary string of utf8-encoded `JSON <http://www.json.org/>`_
    data giving more information about the contents of this file. For
    example::

      {"corpus": "eng-us-all-20120701", "subset": "3gram"}

  * Generally, the *N* bytes of header will end after the *M* bytes of
    metadata, but if not then any remaining bytes should be ignored.

* 4 bytes: After the *N* header bytes, a CRC32C checksum of the *N*
  header bytes, stored in little-endian format.

Blocks
------



Outside of the header, integers are encoded in the uleb128 format,
familiar from the `DWARF debugging format
<https://en.wikipedia.org/wiki/DWARF>`_. Okay, maybe not so
familiar. This is a simple variable-length encoding for unsigned
integers of arbitrary size using **u**nsigned **l**ittle-**e**ndian
**b**ase-**128**. To read a uleb128 value, you proceed from the
beginning of the string, one byte at a time. The lower 7 bits of the
byte give you the next 7 bits of your integer. This is little-endian,
so the first byte gives you the least-significant 7 bits of your
integer, then the next byte gives you bits 8 through 15, the one after
that the bits 16 through 23, etc. The 8th, most-significant bit of
each byte serves as a continuation byte. If this is 1, then you keep
going and read the next byte. If it is 0, then you are
done. Examples::

  uleb128 string  <->  integer value
              00              0x00
              7f              0x7f
           80 01              0x80
           ff 20            0x107f
  80 80 80 80 20           2 ** 33

(This format is also used by `protocol buffers
<https://en.wikipedia.org/wiki/Protocol_Buffers>`_.) NOTE: this format
allows for redundant representations by adding leading zeros, e.g. the
value 0 could also be written ``80 00``. However, doing this is
forbidden.
