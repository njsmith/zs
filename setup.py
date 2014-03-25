from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize

DESC = """Compressed sorted sets -- a space-efficient, static database."""

LONG_DESC = (DESC + "\n"
             "Tools for creating and using the ``.zss`` file format,\n"
             "which allows for the storage of large, compressible\n"
             "data sets in a way that allows for efficient random access,\n"
             "range queries, and decompression. (Original use case:\n"
             "working with the multi-terabyte Google n-gram releases.)")

setup(
    name="zss",
    version="0.0.0+dev",
    description=DESC,
    long_description=LONG_DESC,
    author="Nathaniel J. Smith",
    author_email="njs@pobox.com",
    packages=find_packages(),
    # This means, just install *everything* you see under zss/, even if it
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
                          "zss.tests": ["data",
                                        "data/broken-files",
                                        "data/http-test",
                                    ]},
    url="https://github.com/njsmith/zss",
    install_requires=["six", "requests"],
    classifiers =
      [ "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 2",
        ],
    ext_modules=cythonize([
        Extension("zss._zss",
                  ["zss/_zss.pyx"],
              )
    ]),
)
