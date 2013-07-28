from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

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
    packages=["zss"],
    url="https://github.com/njsmith/zss",
    install_requires=[],
    classifiers =
      [ "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Programming Language :: Python :: 2",
        ],
    cmdclass={"build_ext": build_ext},
    ext_modules=[
        Extension("zss._zss",
                  ["zss/_zss.pyx"],
                  )
        ],
)
