from setuptools import setup, Extension, find_packages
import sys
import os.path

if os.path.exists(".this_is_a_checkout"):
    USE_CYTHON = True
else:
    # Don't depend on Cython in builds-from-sdist
    USE_CYTHON = False

DESC = """ZS is a compressed, read-only file format for efficiently
distributing, querying, and archiving arbitrarily large record-oriented
datasets."""

LONG_DESC = open("README.rst").read()

if USE_CYTHON:
    cython_ext = "pyx"
else:
    cython_ext = "c"
ext_modules = [
    Extension("zs._zs", ["zs/_zs.%s" % (cython_ext,)])
]
if USE_CYTHON:
    from Cython.Build import cythonize
    #import pdb; pdb.set_trace()
    ext_modules = cythonize(ext_modules)

extra_requires = []
# Remove this temporarily so the RTD build will work
# if sys.version_info[0] < 3:
#     extra_requires += ["backports.lzma"]

# defines __version__
exec(open("zs/version.py").read())

setup(
    name="zs",
    version=__version__,
    description=DESC,
    long_description=LONG_DESC,
    author="Nathaniel J. Smith",
    author_email="njs@pobox.com",
    url="https://github.com/njsmith/zs",
    license="2-clause BSD",
    classifiers =
      [ "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        ],
    packages=find_packages(),
    # This means, just install *everything* you see under zs/, even if it
    # doesn't look like a source file, so long as it appears in MANIFEST.in:
    include_package_data=True,
    # This lets us list some specific things we don't want installed, the
    # previous line notwithstanding:
    exclude_package_data={"": ["*.c", "*.pyx", "*.h", "README"],
                          # WTF this is ridiculous. We have to 'exclude' each
                          # data directory here so setuptools doesn't think we
                          # want to copy it, because you can't copy
                          # directories!  However everything *inside* the
                          # directories will still be copied. Which implicitly
                          # creates the directory. So basically this is how
                          # you say "yes please copy this directory (while
                          # pretending not to)".  This may get fixed at some
                          # point:
                          #   http://bugs.python.org/issue19286
                          "zs.tests": ["data",
                                        "data/broken-files",
                                        "data/http-test",
                                    ]},
    entry_points={
        "console_scripts": [
            "zs = zs.cmdline.main:entrypoint",
            ],
    },
    install_requires=["six", "requests", "docopt"] + extra_requires,
    ext_modules=ext_modules,
)
