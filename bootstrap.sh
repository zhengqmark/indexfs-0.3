#!/bin/sh
#
#-----------------------------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.
#
OPT="-O2 -DNDEBUG"       # (A) Production use (optimized mode)
# OPT="-g2 -Wall"        # (B) Debug mode, w/ full line-level debugging symbols
# OPT="-g2 -DNDEBUG"     # (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------------------------
#
#-----------------------------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to enable PVFS2 or HDFS binding.
#
FS_OPT=""                    # (A) Build IndexFS upon POSIX
# FS_OPT="--with-hadoop"     # (B) Build IndexFS upon HDFS
# FS_OPT="--with-pvfs2"      # (C) Build IndexFS upon PVFS2
#-----------------------------------------------------------------

mkdir -p build && cd build || exit 1
../configure CFLAGS="${OPT}" CXXFLAGS="${OPT}" ${FS_OPT} || exit 1
make -j3 --no-print-directory || exit 1

exit 0
