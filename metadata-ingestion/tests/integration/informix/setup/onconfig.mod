# Applied on top of the image's built-in "small" profile (informix_config.small)
# by the entrypoint's `Modify ONCONFIG` step. These exist to get first-boot disk
# initialization under the 60s budget that informix_init.sh hardcodes for creating
# the sysadmin database; overrunning it makes the entrypoint give up and kill the
# container.
#
# DIRECT_IO: the image defaults to 1, but the data directory lives on the
# container's overlay filesystem, where O_DIRECT is unreliable and slow. Buffered
# I/O is both faster and portable across CI runners.
# ROOTSIZE: 350000 (350MB) is the image default; a catalog-only test database needs
# a fraction of that, and laying down the smaller chunk file is the biggest single
# win on a 2-vCPU runner.
#
# LOGFILES/LOGSIZE MUST be shrunk alongside ROOTSIZE and are not optional tuning:
# the logical logs live in the root dbspace, and the image's default LOGFILES 10 is
# sized for a 350MB rootdbs. Leaving it at 10 against a 100MB rootdbs makes disk
# initialization fail outright ("Informix stopped" in the container log, server
# never reaches On-Line). 6 x 2000KB = 12MB fits comfortably.
#
# This does log one cosmetic warning -- "Logical log layout may cause Dynamic Server
# to get into a locked state", because Informix wants LOGSIZE >= 256x max concurrent
# user threads. Irrelevant for a catalog scrape that opens one connection.
DIRECT_IO 0
ROOTSIZE 100000
LOGFILES 6
LOGSIZE 2000
