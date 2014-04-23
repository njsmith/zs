.. _metadata-conventions:

Metadata conventions
====================

The ZS format puts few constraints on the metadata included in the file.

unopinionated
most data sets are unique, so no point trying to define a universal
schema for all of them

::

   "build-info": {"user": ..., "host": ..., "time": ...}
   "record-format": {
       "type": "separated-values",
       "separator": "\t",
       "column-names": [...],
       "column-types": [...],
       }

Some other items you might want to consider including:

* Information on the preprocessing pipeline that led to this file (for
  answering questions like, "is this the version that had
  case-sensitivity enabled, or disabled?")

* Bibliographic references for papers giving details about how the
  data was collected, and that users of this data might want to cite.

* Contact information.

* Any relevant DOIs or `ORCIDs <http://orcid.org/>`_.
