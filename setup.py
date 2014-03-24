import setuptools
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
    install_requires=["six", "requests"],
    classifiers =
      [ "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 2",
        ],
    cmdclass={"build_ext": build_ext},
    package_data={"zss": ["test-data/*.zss"]},
    ext_modules=[
        Extension("zss._zss",
                  ["zss/_zss.pyx"],
                  )
        ],
)
