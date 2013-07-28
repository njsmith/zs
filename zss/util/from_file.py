import zss
import argparse
import multiprocessing
import json

DESC = """
Convert a file containing sorted, separated records into a structured ZSS
file. (The most common case is where the input is a text file, with each
newline-terminated line as a single ZSS record.)
"""

def main(argv):
    argv[0] = "%s -m zss.util.from_file" % (sys.executable,)
    parser = argparse.ArgumentParser(argv[0], description=DESC)
    parser.add_argument("input")
    parser.add_argument("output_zss")
    parser.add_argument("--separator", default="\\n")
    parser.add_argument("--branching-factor", default=1024, type="int")
    parser.add_argument("--approx-blocks-size", default=262144, type="int")
    parser.add_argument("-j", "--parallelism",
                        default=multiprocessing.cpu_count(),
                        type="int")
    parser.add_argument("--codec", default="bz2")
    parser.add_argument("--compress-level", type="int")
    parser.add_argument("--uuid")
    parser.add_argument("--metadata", metavar="JSON")

    args = parser.parse_args(argv)
    sep = args.separator.decode("string_escape")
    metadata = json.loads(args.metadata)
    compress_kwargs = {}
    if args.compress_level is not None:
        compress_kwargs["compress_level"] = args.compress_level
    writer = zss.Writer(args.output_zss, metadata,
                        args.branching_factor, args.approx_block_size,
                        args.parallelism,
                        compression=args.compression,
                        compress_kwargs=compress_kwargs,
                        uuid=args.uuid)
    writer.from_file(open(args.input, "rb"), sep=sep)

if __name__ == "__main__":
    main(sys.argv)
