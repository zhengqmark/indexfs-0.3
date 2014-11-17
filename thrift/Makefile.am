# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

AM_CFLAGS = $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

nodist_noinst_HEADERS =
nodist_noinst_HEADERS += indexfs_types.h
nodist_noinst_HEADERS += indexfs_constants.h
nodist_noinst_HEADERS += MetadataService.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libthrift_idxfs.la

nodist_libthrift_idxfs_la_SOURCES =
nodist_libthrift_idxfs_la_SOURCES += indexfs_types.cpp
nodist_libthrift_idxfs_la_SOURCES += indexfs_constants.cpp
nodist_libthrift_idxfs_la_SOURCES += MetadataService.cpp

## -------------------------------------------------------------------------

EXTRA_DIST = indexfs.thrift

BUILT_SOURCES =
BUILT_SOURCES += indexfs_types.cpp
BUILT_SOURCES += indexfs_constants.cpp
BUILT_SOURCES += MetadataService.cpp

indexfs_types.cpp: indexfs.thrift
	$(THRIFT) -gen cpp -out ./ $(top_srcdir)/thrift/indexfs.thrift && sleep 1

indexfs_constants.cpp: indexfs.thrift
	$(THRIFT) -gen cpp -out ./ $(top_srcdir)/thrift/indexfs.thrift && sleep 1

MetadataService.cpp: indexfs.thrift
	$(THRIFT) -gen cpp -out ./ $(top_srcdir)/thrift/indexfs.thrift && sleep 1

CLEANFILES =
CLEANFILES += indexfs_types.h
CLEANFILES += indexfs_constants.h
CLEANFILES += MetadataService.h
CLEANFILES += indexfs_types.cpp
CLEANFILES += indexfs_constants.cpp
CLEANFILES += MetadataService.cpp
CLEANFILES += MetadataService_server.skeleton.cpp

## -------------------------------------------------------------------------
