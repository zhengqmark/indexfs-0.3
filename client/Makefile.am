# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS =
COMM_FLAGS += -DNBULK_INSERT
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =
noinst_HEADERS += client.h
noinst_HEADERS += metadata_client.h
noinst_HEADERS += libclient.h
noinst_HEADERS += libclient_helper.h
noinst_HEADERS += fuse_helper.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES =
noinst_LTLIBRARIES += libclient_idxfs.la

libclient_idxfs_la_SOURCES =
libclient_idxfs_la_SOURCES += client.cc
libclient_idxfs_la_SOURCES += metadata_client.cc

noinst_LTLIBRARIES += libclient_c_idxfs.la

libclient_c_idxfs_la_SOURCES =
libclient_c_idxfs_la_SOURCES += libclient.cc

## -------------------------------------------------------------------------
## Fuse Client
## -------------------------------------------------------------------------

if BUILD_FUSE
nobase_bin_PROGRAMS = fuse_main
endif

fuse_main_SOURCES = fuse_main.cc

fuse_main_LDADD =
fuse_main_LDADD += libclient_idxfs.la
fuse_main_LDADD += $(top_builddir)/backends/libbackends_idxfs.la
fuse_main_LDADD += $(top_builddir)/communication/librpc_idxfs.la
fuse_main_LDADD += $(top_builddir)/common/libcommon_idxfs.la
fuse_main_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
fuse_main_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

fuse_main_LDFLAGS = $(FUSE_LIBS)
fuse_main_CPPFLAGS = $(FUSE_FLAGS)

## -------------------------------------------------------------------------
## IndexFS Client
## -------------------------------------------------------------------------

nobase_lib_LTLIBRARIES = libindexfs.la

libindexfs_la_SOURCES =
libindexfs_la_SOURCES += client.cc
libindexfs_la_SOURCES += metadata_client.cc
libindexfs_la_SOURCES += libclient_mt.cc
libindexfs_la_SOURCES += libclient_facade.cc

libindexfs_la_LIBADD =
libindexfs_la_LIBADD += $(top_builddir)/backends/libbackends_idxfs.la
libindexfs_la_LIBADD += $(top_builddir)/communication/librpc_idxfs.la
libindexfs_la_LIBADD += $(top_builddir)/common/libcommon_idxfs.la
libindexfs_la_LIBADD += $(top_builddir)/thrift/libthrift_idxfs.la
libindexfs_la_LIBADD += $(top_builddir)/lib/leveldb/libleveldb.la

libindexfs_la_LDFLAGS = -release $(INDEXFS_VERSION)

## -------------------------------------------------------------------------
