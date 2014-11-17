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
noinst_HEADERS += split_thread.h
noinst_HEADERS += metadata_server.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS = metadata_server

metadata_server_SOURCES =
metadata_server_SOURCES += metadata_server.cc
metadata_server_SOURCES += coordinated_ops.cc
metadata_server_SOURCES += split_thread.cc
metadata_server_SOURCES += server_main.cc

metadata_server_LDADD =
metadata_server_LDADD += $(top_builddir)/backends/libbackends_idxfs.la
metadata_server_LDADD += $(top_builddir)/communication/librpc_idxfs.la
metadata_server_LDADD += $(top_builddir)/common/libcommon_idxfs.la
metadata_server_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
metadata_server_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
