# A little test of how many bytes/sec python can push
import sys

# Testing on polypore.
# ~50-60 MiB/s:
# for line in sys.stdin:
#     print line

# ~ 100 MiB/s on polypore,
# ~ 120 MiB/s on ged
# BUFSIZE = 2 ** 20
# leftover = ""
# while True:
#     buf = sys.stdin.read(BUFSIZE)
#     if not buf:
#         break
#     lines = buf.split("\n")
#     lines[0] = leftover + lines[0]
#     leftover = lines.pop()
#     sys.stdout.write("\n".join(lines))

# ~ 69 MiB/s on ged (~55 for w(line); w("\n"))
# BUFSIZE = 2 ** 20
# leftover = ""
# w = sys.stdout.write
# while True:
#     buf = sys.stdin.read(BUFSIZE)
#     if not buf:
#         break
#     lines = buf.split("\n")
#     lines[0] = leftover + lines[0]
#     leftover = lines.pop()
#     for line in lines:
#         w(line + "\n")

# # ~ 52 MiB/s on ged
# from zss._zss import to_uleb128
# BUFSIZE = 2 ** 20
# leftover = ""
# w = sys.stdout.write
# while True:
#     buf = sys.stdin.read(BUFSIZE)
#     if not buf:
#         break
#     lines = buf.split("\n")
#     lines[0] = leftover + lines[0]
#     leftover = lines.pop()
#     for line in lines:
#         w(to_uleb128(len(line)) + line)

# ~ 80 MiB/s on ged -- local variables are faster!
# BUFSIZE = 2 ** 20
# def doit():
#     leftover = ""
#     w = sys.stdout.write
#     while True:
#         buf = sys.stdin.read(BUFSIZE)
#         if not buf:
#             break
#         lines = buf.split("\n")
#         lines[0] = leftover + lines[0]
#         leftover = lines.pop()
#         for line in lines:
#             w(line + "\n")
# doit()

# ~ 48 MiB/s on ged
# BUFSIZE = 2 ** 20
# from zss._zss import to_uleb128
# def doit():
#     leftover = ""
#     w = sys.stdout.write
#     while True:
#         buf = sys.stdin.read(BUFSIZE)
#         if not buf:
#             break
#         lines = buf.split("\n")
#         lines[0] = leftover + lines[0]
#         leftover = lines.pop()
#         last_line = ""
#         for line in lines:
#             w(to_uleb128(len(line)) + line)
#             assert last_line <= line
#             last_line = line
# doit()

# ~72 MiB/s on ged
#BUFSIZE = 2 ** 20
# ~ 120 MiB/s on ged (!)
#   pretty similar from 16-200 at least
# ~ 155 MiB/s on polypore
#BUFSIZE = 16 * 2 ** 10

# polypore:
#   16384: 156 MiB/s
#   32768: 152 MiB/s
#   65536: 155 MiB/s
BUFSIZE = int(sys.argv[1])
from zss._zss import pack_data_records
def doit():
    leftover = ""
    w = sys.stdout.write
    while True:
        buf = sys.stdin.read(BUFSIZE)
        if not buf:
            break
        lines = buf.split("\n")
        lines[0] = leftover + lines[0]
        leftover = lines.pop()
        w(pack_data_records(lines, 2* BUFSIZE))
doit()
