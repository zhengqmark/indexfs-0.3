# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS =
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =

# common headers
noinst_HEADERS += common.h
noinst_HEADERS += config.h
noinst_HEADERS += dentcache.h
noinst_HEADERS += dircache.h
noinst_HEADERS += dirhandle.h
noinst_HEADERS += dmapcache.h
noinst_HEADERS += logging.h
noinst_HEADERS += scanner.h
noinst_HEADERS += network.h

# legacy headers
noinst_HEADERS += debugging.h
noinst_HEADERS += giga_index.h
noinst_HEADERS += options.h
noinst_HEADERS += sha.h
noinst_HEADERS += murmurhash3.h

# helper headers
noinst_HEADERS += bitmap.h
noinst_HEADERS += counter.h
noinst_HEADERS += ../util/str_hash.h
noinst_HEADERS += ../util/measurement.h
noinst_HEADERS += ../util/monitor_thread.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libcommon_idxfs.la

libcommon_idxfs_la_SOURCES =
libcommon_idxfs_la_SOURCES += sha.c
libcommon_idxfs_la_SOURCES += murmurhash3.cc
libcommon_idxfs_la_SOURCES += giga_index.c
libcommon_idxfs_la_SOURCES += debugging.c
libcommon_idxfs_la_SOURCES += config.cc
libcommon_idxfs_la_SOURCES += config_hdfs.cc
libcommon_idxfs_la_SOURCES += logging.cc
libcommon_idxfs_la_SOURCES += dircache.cc
libcommon_idxfs_la_SOURCES += dirhandle.cc
libcommon_idxfs_la_SOURCES += dmapcache.cc
libcommon_idxfs_la_SOURCES += scanner.cc
libcommon_idxfs_la_SOURCES += ../util/str_hash.cc
libcommon_idxfs_la_SOURCES += ../util/monitor_thread.cc

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =
nobase_bin_PROGRAMS += network_test

network_test_SOURCES = network_test.cc
network_test_LDADD = $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
