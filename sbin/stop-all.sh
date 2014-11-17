#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/stop-all.sh
#
# Use this script to shutdown an indexfs server cluster over the network.
# Root privilege is neither required nor recommended to run this scripts.
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

report_error() {
  echo "Fail to kill indexfs server at $1"
}

# ask all member server nodes to shutdown their local indexfs servers
for srv_node in \
  $(cat $INDEXFS_CONF_DIR/server_list | cut -d':' -f1)
do
  INDEXFS_ID=$((${INDEXFS_ID:-"-1"} + 1))
  INDEXFS_RUN=$INDEXFS_ROOT/run/server-$INDEXFS_ID
  $SSH $srv_node "env INDEXFS_ID=$INDEXFS_ID INDEXFS_CONF_DIR=$INDEXFS_CONF_DIR \
    INDEXFS_RUN=$INDEXFS_RUN $INDEXFS_HOME/sbin/stop-idxfs.sh" || report_error $srv_node
done

exit 0
