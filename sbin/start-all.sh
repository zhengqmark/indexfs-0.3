#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/start-all.sh
#
# Use this script to start an indexfs server cluster over the network.
# Root privilege is neither required nor recommended to run this scripts.
#
# Note that this script makes use of the 'sbin/start-idxfs.sh' to start
# each indexfs server on each participanting server node. Currently,
# we recommend to run 1 single indexfs server on 1 single machine.
#
# Note also that this script uses SSH to launch jobs on remote servers.
# Before using this script, please prepare your server list file at
# etc/indexfs-distributed/server_list, and make sure your "control node" can
# SSH to these servers without providing a password.
#
# Please also make sure that all servers have access to the same indexfs distribution
# and can access that with the same file system path. It is recommended to place
# the indexfs distribution on a shared file system, such as NFS.
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_ROOT=${INDEXFS_ROOT:-"/tmp/indexfs"}
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-distributed"}

# make ssh a bit more admin-friendly
SSH='ssh -o ConnectTimeout=5 -o ConnectionAttempts=1 -o StrictHostKeyChecking=no'

# check if we have the required server list file
if test ! -e "$INDEXFS_CONF_DIR/server_list"
then
  echo "Cannot find our server list file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_DIR/server_list"
  exit 1
fi

# remove old indexfs data
rm -rf $INDEXFS_ROOT/*
mkdir -p $INDEXFS_ROOT

report_error() {
  echo "Fail to start indexfs server at $1"
  echo "Abort!"
  exit 1
}

# ask all member server nodes to start a new indexfs server instance
for srv_node in \
  $(cat $INDEXFS_CONF_DIR/server_list | cut -d':' -f1)
do
  INDEXFS_ID=$((${INDEXFS_ID:-"-1"} + 1))
  INDEXFS_RUN=$INDEXFS_ROOT/run/server-$INDEXFS_ID
  $SSH $srv_node "env INDEXFS_ID=$INDEXFS_ID INDEXFS_CONF_DIR=$INDEXFS_CONF_DIR \
    INDEXFS_RUN=$INDEXFS_RUN $INDEXFS_HOME/sbin/start-idxfs.sh" || report_error $srv_node
done

exit 0
