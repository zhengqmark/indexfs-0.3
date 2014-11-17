## Copyright (c) 2014 The IndexFS Authors. All rights reserved.
## Use of this source code is governed by a BSD-style license that can be
## found in the LICENSE file. See the AUTHORS file for names of contributors.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

## -------------------------------------------------------------------------
## IndexFS Modules
## -------------------------------------------------------------------------

SUBDIRS =

# --------------------------------
# Main Modules
# --------------------------------

SUBDIRS += lib/leveldb
SUBDIRS += thrift
SUBDIRS += communication
SUBDIRS += common
SUBDIRS += backends
SUBDIRS += client
SUBDIRS += server

# --------------------------------
# Performance Testing
# --------------------------------

if BUILD_IOTESTS
  SUBDIRS += io_test
endif

if BUILD_MDTESTS
  SUBDIRS += md_test
endif

# --------------------------------
# Client Toolkit
# --------------------------------

SUBDIRS += bin

## -------------------------------------------------------------------------
## Other Artifacts
## -------------------------------------------------------------------------

EXTRA_DIST =

EXTRA_DIST += include
EXTRA_DIST += bootstrap.sh
EXTRA_DIST += AUTHORS LICENSE README.md
EXTRA_DIST += sbin/idxfs.sh sbin/hdfs.sh
EXTRA_DIST += sbin/start-all.sh sbin/stop-all.sh
EXTRA_DIST += sbin/start-idxfs.sh sbin/stop-idxfs.sh
EXTRA_DIST += sbin/tree-test.sh sbin/mount-fuse.sh
EXTRA_DIST += etc/indexfs-standalone etc/indexfs-distributed
EXTRA_DIST += etc/hdfs-standalone

## -------------------------------------------------------------------------
## Additional Makefile Actions
## -------------------------------------------------------------------------

all-local:
	ln -fs $(top_builddir)/server/metadata_server $(top_builddir)/
	ln -fs $(top_builddir)/io_test/io_driver $(top_builddir)/
	ln -fs $(top_builddir)/client/fuse_main $(top_builddir)/
	ln -fs $(top_builddir)/client/.libs/libindexfs-$(INDEXFS_VERSION).so $(top_builddir)/
	ln -fs $(top_builddir)/libindexfs-$(INDEXFS_VERSION).so $(top_builddir)/libindexfs.so

clean-local:
	rm -f $(top_builddir)/metadata_server
	rm -f $(top_builddir)/io_driver
	rm -f $(top_builddir)/fuse_main
	rm -f $(top_builddir)/libindexfs*.so

## -------------------------------------------------------------------------
