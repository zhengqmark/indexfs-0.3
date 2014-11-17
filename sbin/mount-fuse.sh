#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/mount-fuse.sh
#
# Use this script to mount IndexFS via FUSE.
# Root privilege is neither required nor recommended to run this script.
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-standalone"}

# check the location of the build directory
INDEXFS_BASE=$INDEXFS_HOME

if test -d $INDEXFS_HOME/build
then
  INDEXFS_BASE=$INDEXFS_HOME/build
fi

# check whether our binaries are there
if test ! -e $INDEXFS_BASE/client/fuse_main
then
  echo "Cannot find indexfs fuse client -- oops"
  exit 1
fi

INDEXFS_RUN=${INDEXFS_RUN:-"/tmp/indexfs"}
INDEXFS_MNT=$INDEXFS_RUN/fuse-mnt

# check server config servers
if test ! -e $INDEXFS_CONF_DIR/server_list
then
  echo "Cannot found our server list file -- oops"
  exit 1
fi

if test ! -e $INDEXFS_CONF_DIR/indexfs_conf
then
  echo "Cannot found our indexfs config file -- oops"
  exit 1
fi

mkdir -p $INDEXFS_MNT
fusermount -u $INDEXFS_MNT &>/dev/null

# start-fuse client
$INDEXFS_BASE/client/fuse_main --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
  --srvlstfn=$INDEXFS_CONF_DIR/server_list -- -s $INDEXFS_MNT || exit 1

echo "IndexFS appears to be mounted at $INDEXFS_MNT"

exit 0
