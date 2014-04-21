.. _metadata-conventions:

Metadata conventions
====================

The ZSS format puts few constraints on what metadata you put in your

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

* A contact email address.

* Your `ORCID <http://orcid.org/>`_.
