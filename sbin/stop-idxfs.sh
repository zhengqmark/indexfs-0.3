#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/stop-idxfs.sh
#
# Use this script to shutdown the indexfs server running at the local machine.
# If indexfs has been built with HDFS as the backend, then the local HDFS
# service will also been stopped.
# Root privilege is neither required nor recommended to run this script.  
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)

INDEXFS_ID=${INDEXFS_ID:-"0"}
INDEXFS_RUN=${INDEXFS_RUN:-"/tmp/indexfs"}
INDEXFS_LOGS=$INDEXFS_RUN/logs
INDEXFS_PID_FILE=$INDEXFS_RUN/metadata_server.pid.$INDEXFS_ID

# check running instances
if test -e $INDEXFS_PID_FILE
then
  echo "Stopping indexfs server at `hostname -s` ... "
  pid=$(cat $INDEXFS_PID_FILE)
  kill -9 $pid &>/dev/null; rm -f $INDEXFS_PID_FILE
else
  killall -9 metadata_server &>/dev/null
  echo "No running indexfs server instance found at `hostname -s`"
fi

# stop HDFS if necessary
if test x"`$INDEXFS_HOME/sbin/idxfs.sh backend`" = x"__HDFS__"
then
  $INDEXFS_HOME/sbin/hdfs.sh stop
fi

exit 0
