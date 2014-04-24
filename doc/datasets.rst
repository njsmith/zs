Publically available ZS datasets
================================

[XX expand]

The v2 Google Books eng-us-all n-grams are here, at least temporarily:
    http://bolete.ucsd.edu/njsmith/

Note that these files use "0gram" to refer to what Google calls
"totalcounts", thus preserving the rule that n-gram counts are
normalized by (n-1)-gram counts. The simple dependency arcs that
Google calls "0grams" are not included, since they seem to have been
superseded by later data releases.

The `zscontrib <https://github.com/njsmith/zscontrib>`_ repository has
some scripts to fetch an arbitrary v2 Google Books sub-corpus and
build it into a set of .zs files.
