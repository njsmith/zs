import requests
import lxml.html
import re
import sys
import multiprocessing
import os
import os.path
import cPickle
import time
import json

OVERVIEW_URL = "http://books.google.com/ngrams/datasets"
# Version 2
VERSION = "20120701"

# Given the relevant bit of a url, returns the normalized name (as a string!)
# of the order of ngrams contained in that url.
def _norm_order_tag(tag):
    # Google, in their infinite wisdom, put the 0gram counts in the
    # "totalcounts" file, and put the dependency arcs in the "0gram"
    # files. We normalize it so that 0-5 grams actually are 0-5 grams, and the
    # arcs go into their own "dependency" category.
    if tag == "totalcounts":
        return "0gram"
    else:
        assert tag.endswith("gram")
        if tag == "0gram":
            return "dependency"
        else:
            return tag

def urls_by_corpus():
    url_re_code = ("googlebooks-"
                   "(?P<corpus>.*)-"
                   "(?P<order_tag>(totalcounts)|([0-5]gram))-"
                   "%s.*"
                   % (re.escape(VERSION),))
    url_re = re.compile(url_re_code)
    urls = {}
    sys.stderr.write("Fetching %s\n" % (OVERVIEW_URL,))
    overview = lxml.html.parse(OVERVIEW_URL).getroot()
    sys.stderr.write("Extracting data file URLs\n")
    overview.make_links_absolute()
    for _, attr, url, _ in overview.iterlinks():
        if attr == "href":
            match = url_re.search(url)
            if match:
                corpus = match.group("corpus")
                order = _norm_order_tag(match.group("order_tag"))
                urls.setdefault(corpus, {}).setdefault(order, set()).add(url)
    # Sanity check in case of partial downloads etc. As of 2013-07-22, "spa"
    # is the last corpus on the page.
    check_corpora = ["spa-all", "rus-all", "ita-all", "fre-all", "eng-gb-all",
                     "eng-all"]
    for check_corpus in check_corpora:
        if check_corpus not in urls:
            raise RuntimeError("Failed to fetch URL list properly? %r missing"
                               % (check_corpus,))
        expected_orders = ["0gram", "1gram", "2gram", "3gram", "4gram",
                           "5gram", "dependency"]
        if set(urls[check_corpus].keys()) != set(expected_orders):
            raise RuntimeError("Failed to fetch URL list properly? "
                               "%r orders: %r"
                               % (check_corpus, urls[check_corpus].keys()))
    return urls

def get_size(url, _session=None, _backoff=0.5):
    if _session is None:
        _session = requests.Session()
    try:
        return int(_session.head(url).headers["Content-length"])
    except requests.ConnectionError, e:
        sys.stderr.write("Problem with HEAD %s: sleeping %s s\n"
                         % (url, _backoff))
        time.sleep(_backoff)
        return get_size(url, _session=_session, _backoff=2 * _backoff)

def get_sizes(url_list, workers=30):
    if workers > 0:
        return _get_sizes_parallel(url_list, workers)
    else:
        return _get_sizes_serial(url_list)

def _get_sizes_serial(url_list):
    session = requests.Session()
    for url in url_list:
        yield (url, get_size(url, _session=session))

def _get_sizes_parallel_init():
    global _REQUEST_SESSION
    _REQUEST_SESSION = requests.Session()

def _get_sizes_parallel_worker(url):
    global _REQUEST_SESSION
    return (url, get_size(url, _session=_REQUEST_SESSION))

def _get_sizes_parallel(url_list, workers):
    try:
        pool = multiprocessing.Pool(workers, _get_sizes_parallel_init)
        for result in pool.imap_unordered(_get_sizes_parallel_worker,
                                          url_list,
                                          # Not based on anything, just a guess
                                          chunksize=5):
            yield result
    finally:
        pool.terminate()

################################################################

# The "totalcounts" files in the v2 Google books corpora are truly
# bizarre. Their format is:
#
# 1 space character
# then a tab character
# then a set of blocks each of which is:
#   year
#   comma
#   word tokens
#   comma
#   pages
#   comma
#   books
#   tab character
#
# This is radically unlike the other files, all of which have the form:
#   ngram
#   tab
#   year
#   tab
#   ngram tokens
#   tab
#   books
#   newline
#
# This script converts the former into the latter, with the convention that
# these are zero-grams, so the ngram field in the output is an empty
# string. It throws away the page information.

def _fix_totalcounts(args):
    data = sys.stdin.read()
    assert data[:2] == " \t"
    data = data[2:]
    blocks = data.split("\t")
    assert blocks[-1] == ""
    blocks.pop(-1)
    for block in blocks:
        numbers = block.split(",")
        assert len(numbers) == 4
        year, words, pages, books = numbers
        sys.stdout.write("\t%s\t%s\t%s\n" % (year, words, books))

################################################################

MAKEFILE_TEMPLATE = """
ZSS_OPTS :=
SHELL := /bin/bash -e -o errexit -o pipefail
PYTHON := {{ python }}
TMPDIR := .
TMP_COMPRESS := lzop
CURL := curl -sS
SORT = env LC_ALL=C sort -T $(TMPDIR) --compress-program=$(TMP_COMPRESS)

# "pipe viewer" is nice for viewing throughput, but not necessary
ifndef PV
  HAVE_PV := $(shell pv --version 2>/dev/null)
  ifdef HAVE_PV
    PV := pv
  endif
endif
ifdef PV
  PIPE_PV = | $(PV) -rabtp
  ZSS_OPTS += --no-spinner
endif

# You can override stuff here:
-include override.make
-include override.{{ corpus_fullname }}.make

.DELETE_ON_ERROR:

.PHONY: all
all: {% for subset in subsets | sort -%}
{{ corpus_fullname }}-{{ subset }}.zss {% endfor %}

{{ corpus_fullname }}:
\tmkdir {{ corpus_fullname }}

{{ corpus_fullname }}/corpus-sizes.pickle: | {{ corpus_fullname }}
\t${PYTHON} -m zss.zngram.gbooks2 _pickle-sizes "{{ corpus }}" "$@"

{% for subset in subsets | sort %}
### Makefile portion for subset: {{ subset }}
.PHONY: {{ subset }}
{{ subset }}: {{ corpus_fullname }}-{{ subset }}.zss

{{ corpus_fullname }}/{{ subset }}: | {{ corpus_fullname }}
\tmkdir -p "$@"

.PHONY: sorted-{{ subset }} size-check-{{ subset }}

{% set metadata = json_encode({"corpus": corpus_fullname, "subset": subset}) %}

{% if subset != "0gram" %}
# Standard sub-corpus (not zero-grams):

{{ corpus_fullname }}-{{ subset }}.zss: sorted-{{ subset }} size-check-{{ subset }}
\ttime $(PYTHON) -m zss.util.merge_sorted {% for url in urls[subset] | sort -%}
{{ corpus_fullname }}/{{ subset }}/sorted-{{ basename(url) }} {% endfor -%}
$(PIPE_PV) \
| $(PYTHON) -m zss.util.from_file $(ZSS_OPTS) /dev/fd/0 "$@" \
--metadata='{{ metadata}}'

sorted-{{ subset }}: {% for url in urls[subset] | sort -%}
{{ corpus_fullname }}/{{ subset }}/sorted-{{ basename(url) }} {% endfor %}

size-check-{{ subset }}: {{ corpus_fullname }}/corpus-sizes.pickle sorted-{{ subset }}
\t$(PYTHON) -m zss.zngram.gbooks2 _size-check "{{ corpus_fullname }}/{{ subset }}" "{{ subset }}" "{{ corpus_fullname }}/corpus-sizes.pickle"

{% for url in urls[subset] | sort %}
{{ corpus_fullname }}/{{ subset }}/sorted-{{ basename(url) }}: | {{ corpus_fullname }}/{{ subset }}
\ttime $(CURL) "{{ url }}" | gunzip -c | $(SORT) | gzip -1 -c > "$@"
{% endfor %}

{% else %}
# Zero-grams need special handling:

{% set totalcounts_url = list(urls[subset])[0] %}
{% set totalcountsgz
    = "%s/%s/sorted-%s.gz" % (corpus_fullname, subset, basename(totalcounts_url)) %}
sorted-{{ subset }}: {{ totalcountsgz }}

{{ corpus_fullname }}-{{ subset }}.zss: sorted-{{ subset }} size-check-{{ subset }}
\ttime gunzip -c {{ totalcountsgz }} \
| $(PYTHON) -m zss.util.from_file --metadata='{{ metadata}}' $(ZSS_OPTS) /dev/fd/0 "$@"

size-check-{{ subset }}: {{ corpus_fullname }}/corpus-sizes.pickle sorted-{{ subset }}
\ttest $$(gunzip -c "{{ totalcountsgz }}" | wc -c) -ge 1000

{{ totalcountsgz }}: | {{ corpus_fullname }}/{{ subset }}
\ttime $(CURL) "{{ totalcounts_url }}" | $(PYTHON) -m zss.zngram.gbooks2 _fix-totalcounts | gzip -1 -c > "$@"

{% endif %}

### End of Makefile portion for subset: {{ subset }}
{% endfor %}
"""

def write_makefile(corpus):
    from jinja2 import Template
    template = Template(MAKEFILE_TEMPLATE)
    urls = urls_by_corpus()[corpus]
    template.environment.filters["basename"] = os.path.basename
    corpus_fullname = "%s-%s" % (corpus, VERSION)
    args = {
        "python": sys.executable,
        "corpus": corpus,
        "corpus_fullname": corpus_fullname,
        "subsets": urls.keys(),
        "urls": urls,
        "basename": os.path.basename,
        "list": list,
        "json_encode": json.dumps,
        }
    text = template.render(**args)
    open("%s.make" % (corpus_fullname), "wb").write(text)

def _pickle_sizes(args):
    corpus, out_path = args
    order_to_urls = urls_by_corpus()[corpus]
    all_urls = set()
    for urls in order_to_urls.values():
        all_urls.update(urls)
    sys.stderr.write("Getting sizes for %s urls.\n" % (len(all_urls,)))
    url_to_size = dict()
    for (url, size) in get_sizes(all_urls):
        sys.stderr.write(".")
        url_to_size[url] = size
    sys.stderr.write("\nGot sizes.\n")
    cPickle.dump((order_to_urls, url_to_size),
                 open(out_path, "wb"),
                 protocol=2)

# This is a sanity check -- mostly designed to catch cases where some sort of
# crash or failure leaves us with an empty "sorted" file. (If we wanted to be
# really careful we should fetch the URLs twice, once just to check the
# uncompressed sizes, and then check that that matches the uncompressed sizes
# of our sorted files... but that sounds like more work than I'm willing to do
# right now.)
def _size_check(args):
    sorted_dir, order, pickle_path = args
    order_to_urls, url_to_size = cPickle.load(open(pickle_path, "r"))
    bad = False
    min_expansion = 1000
    min_expansion_path = None
    for url in order_to_urls[order]:
        disk_path = "%s/sorted-%s" % (sorted_dir, os.path.basename(url))
        disk_size = os.stat(disk_path).st_size
        url_size = url_to_size[url]
        expansion = disk_size * 1.0 / url_size
        if expansion < min_expansion:
            min_expansion = expansion
            min_expansion_path = disk_path
        # For eng-us-all, 3gram-ww actually is actually 0.95 times smaller
        # when sorted and gzip -1'd than as distributed.
        if expansion < 0.9:
            bad = True
            sys.stderr.write("%s : disk size (%s) is %0.2f times the size of %s\n"
                             % (disk_path, disk_size, expansion, url))
    sys.stderr.write("Note: smallest disk size/url size (which is (sorted gzip1)/(sorted gzip6)) is: %s (for %s)\n"
                     % (min_expansion, min_expansion_path))

    sys.stderr.write("Too-small values here could indicate partial downloads and missing data\n")
    if bad:
        sys.stderr.write("Some file shrunk by more than 10%! Refusing to continue.\n")
        sys.exit(1)

def _make_makefile(args):
    corpus, = args
    write_makefile(corpus)

# Invocation as python -mzss.zngram.gbooks2
# Note that in this case this code may be executed twice, once in the __main__
# namespace and once in the 'zss.zngram.gbooks2' namespace.
if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.stderr.write("Need at least one argument!")
    elif sys.argv[1] == "_fix-totalcounts":
        _fix_totalcounts(sys.argv[2:])
    elif sys.argv[1] == "_size-check":
        _size_check(sys.argv[2:])
    elif sys.argv[1] == "_pickle-sizes":
        _pickle_sizes(sys.argv[2:])
    elif sys.argv[1] == "make-makefile":
        _make_makefile(sys.argv[2:])
    else:
        sys.stderr.write("Unknown argument: %r\n" % (sys.argv[1],))
        sys.exit(2)
