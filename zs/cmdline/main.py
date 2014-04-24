# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# Quirky things about how the command system works here:
# - we use docopt: http://docopt.org
# - we store the --help text for each command as the docstring for the
#   relevant function.
# - the top-level 'main' does some prevalidation on arguments commonly used

import sys
import codecs

import six
from docopt import docopt, DocoptExit

import zs

from .util import optfail

# Docopt's exit conditions:
#   https://github.com/docopt/docopt/issues/106#issuecomment-20569331
#   https://github.com/docopt/docopt/issues/44
# Exiting with status 0 on --help is fine
# But we really should have status 2 if user provided invalid arguments.
def fixed_docopt(*args, **kwargs):
    try:
        return docopt(*args, **kwargs)
    except DocoptExit as e:
        sys.stderr.write(str(e))
        sys.stderr.write("\n")
        sys.exit(2)

# Called on things like --start, --stop, --prefix, --terminator
def binaryize(arg):
    if arg is None:
        return None
    if six.PY3:
        # arg is unicode. It might have been originally unicode (e.g. on
        # windows), or originally binary (e.g. on POSIX). If it was originally
        # unicode then we can definitely convert it to a utf-8 bytestring. If
        # it was originally bytes, then this is the magic incantation that
        # gives us back the original bytestring, no matter what it
        # was, assuming that the user is in a utf-8 locale.
        # (See http://legacy.python.org/dev/peps/pep-0383/)
        arg = arg.encode("utf-8", "surrogateescape")
    # now arg is a bytestring. we interpret \ escapes in a py2-and-py3
    # compatible way.
    return codecs.escape_decode(arg)[0]

def transopt(opt):
    return "__%s__" % (opt.strip("-"),)

subcommands = {}

from .dump import command_dump
subcommands["dump"] = command_dump

from .validate import command_validate
subcommands["validate"] = command_validate

from .info import command_info
subcommands["info"] = command_info

from .make import command_make
subcommands["make"] = command_make

# args = argv[1:]
def main(args):
    """ZS: a space-efficient file format format for distributing, archiving,
and querying large data sets.

Usage:
  zs <subcommand> [<args>...]
  zs --version
  zs --help

Available subcommands:
  zs dump      Get contents of a .zs file.
  zs info      Get general metadata about a .zs file.
  zs validate  Check a .zs file for validity.
  zs make      Create a new .zs file with specified contents.

For details, use 'zs <subcommand> --help'.
"""

    opts = fixed_docopt(main.__doc__, argv=args, version=zs.__version__,
                        options_first=True)
    # docopt handles --help and --version for us
    subcommand = opts["<subcommand>"]
    if subcommand not in subcommands:
        optfail("Unrecognized subcommand %r; try --help for info"
                % (subcommand,))
    subcommand_fn = subcommands[subcommand]
    subopts = fixed_docopt(subcommand_fn.__doc__, argv=args)

    # Generic option handling

    # options specifying binary values
    for opt in ["--terminator", "--start", "--stop", "--prefix"]:
        if opt in subopts:
            subopts[transopt(opt)] = binaryize(subopts[opt])

    # options specifying integers
    for opt in ["--branching-factor", "--approx-block-size"]:
        if opt in subopts and subopts[opt] is not None:
            try:
                subopts[transopt(opt)] = int(subopts[opt])
            except ValueError:
                optfail("%s wants an integer, but got %r"
                        % (opt, subopts[opt]))

    # special opts
    if "-j" in subopts:
        if subopts["-j"] == "guess":
            subopts["__j__"] = "guess"
        else:
            try:
                subopts["__j__"] = int(subopts["-j"])
            except ValueError:
                optfail("-j should be an integer but got %r"
                        % (subopts["-j"],))

    if subopts.get("--length-prefixed") is not None:
        legal = ["u64le", "uleb128"]
        if subopts["--length-prefixed"] not in legal:
            optfail("Invalid --length-prefixed value %r: must be "
                    % (subopts["--length-prefixed"],)
                    + " or ".join(legal))

    return subcommand_fn(subopts)

def entrypoint():
    return main(sys.argv[1:])
