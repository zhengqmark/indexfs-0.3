#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/start-idxfs.sh
#
# This starts a local indexfs server instance on the local machine. If indexfs
# has been built with HDFS as the backend, then a local HDFS service will
# also be started with 1 HDFS name server instance and 1 HDFS data server instance.
# Root privilege is neither required nor recommended to run this script.
#
# Before using this script, please prepare your indexfs config file at
# etc/indexfs-standalone/indexfs_conf, your server list file at
# etc/indexfs-standalone/server_list, as well as your indexfs hdfs config file at
# etc/indexfs-standalone/hdfs_conf. These three files are distributed along
# with the indexfs source code tarball with default settings.
# 
# Note that please use only IPv4 addresses in the server list file, as we
# currently do not support hostname or IPv6 addresses.
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-standalone"}

# check indexfs backend type
INDEXFS_BACKEND=$($INDEXFS_HOME/sbin/idxfs.sh backend)
case $INDEXFS_BACKEND in
  __NFS__)
    echo "Using NFS as the storage backend"
    ;;
  __HDFS__)
    echo "Using HDFS as the storage backend"
    ;;
  *)
    echo "Unexpected backend '$INDEXFS_BACKEND'"
    exit 1
    ;;
esac

# check if we have the required configuration files
if test ! -e "$INDEXFS_CONF_DIR/server_list"
then
  echo "Cannot find our server list file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_DIR/server_list"
  exit 1
fi

if test ! -e "$INDEXFS_CONF_DIR/indexfs_conf"
then
  echo "Cannot find our indexfs config file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_DIR/indexfs_conf"
  exit 1
fi

if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  if test ! -e "$INDEXFS_CONF_DIR/hdfs_conf"
  then
    echo "Cannot find our indexfs hdfs config file -- oops"
    echo "It is supposed to be found at $INDEXFS_CONF_DIR/hdfs_conf"
    exit 1
  fi
fi

# check the location of the build directory
INDEXFS_BASE=$INDEXFS_HOME

if test -d $INDEXFS_HOME/build
then
  INDEXFS_BASE=$INDEXFS_HOME/build
fi

# check the existence of our indexfs server binary
if test ! -e $INDEXFS_BASE/metadata_server
then
  echo "Cannot find the metadata server binary -- oops"
  echo "It is supposed to be found at $INDEXFS_BASE/metadata_server"
  exit 1
fi

INDEXFS_ID=${INDEXFS_ID:-"0"}
INDEXFS_RUN=${INDEXFS_RUN:-"/tmp/indexfs"}
INDEXFS_LOGS=$INDEXFS_RUN/logs
INDEXFS_PID_FILE=$INDEXFS_RUN/metadata_server.pid.$INDEXFS_ID

# check running instances
if test -e $INDEXFS_PID_FILE
then
  echo "Found 1 running indexfs server -- kill it first"
  pid=$(cat $INDEXFS_PID_FILE)
  kill -9 $pid && sleep 1; rm -f $INDEXFS_PID_FILE
fi

# prepare server directories
rm -rf $INDEXFS_RUN/*
mkdir -p $INDEXFS_RUN $INDEXFS_LOGS

if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  # check hdfs status
  echo "Cheking hdfs ..."
  $INDEXFS_HOME/sbin/hdfs.sh check || exit 1

  # bootstrap hdfs first
  echo "Starting hdfs at `hostname -s` ..."
  $INDEXFS_HOME/sbin/hdfs.sh stop &>/dev/null
  $INDEXFS_HOME/sbin/hdfs.sh start || exit 1
  sleep 5 # wait for hdfs to open business

  echo "Setup hdfs ..."
  $INDEXFS_HOME/sbin/hdfs.sh mkdir $INDEXFS_RUN || exit 1

  # prepare indexfs-hdfs runtime
  LD_PATH=`$INDEXFS_HOME/sbin/hdfs.sh ldpath`
  export LD_LIBRARY_PATH=$LD_PATH
  export LIBHDFS_OPTS="-Djava.library.path=$LD_PATH"
  export CLASSPATH=`$INDEXFS_HOME/sbin/hdfs.sh classpath`
fi

# time to kick-out our indexfs server
echo "Starting indexfs server at `hostname -s`, logging to $INDEXFS_LOGS ..."

if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  nohup $INDEXFS_BASE/metadata_server --srvid="$INDEXFS_ID" --log_dir="$INDEXFS_LOGS" \
    --hconfigfn="$INDEXFS_CONF_DIR/hdfs_conf" \
    --configfn="$INDEXFS_CONF_DIR/indexfs_conf" --srvlstfn="$INDEXFS_CONF_DIR/server_list" \
    1>$INDEXFS_LOGS/metadata_server.OUT.$INDEXFS_ID 2>$INDEXFS_LOGS/metadata_server.ERR.$INDEXFS_ID </dev/null &
else
  nohup $INDEXFS_BASE/metadata_server --srvid="$INDEXFS_ID" --log_dir="$INDEXFS_LOGS" \
    --configfn="$INDEXFS_CONF_DIR/indexfs_conf" --srvlstfn="$INDEXFS_CONF_DIR/server_list" \
    1>$INDEXFS_LOGS/metadata_server.OUT.$INDEXFS_ID 2>$INDEXFS_LOGS/metadata_server.ERR.$INDEXFS_ID </dev/null &
fi

echo "$!" | tee $INDEXFS_PID_FILE &>/dev/null

exit 0
