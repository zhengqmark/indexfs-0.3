#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/tree-test.sh
#
# This launches an MPI-based performance test on an indexfs setup.
# Root privilege is neither required nor recommended to run this script.
#
# MPI is required to run this script:
#   > mpirun -version
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=$INDEXFS_HOME/etc/indexfs-standalone

# check the location of the build directory
INDEXFS_BASE=$INDEXFS_HOME

if test -d $INDEXFS_HOME/build
then
  INDEXFS_BASE=$INDEXFS_HOME/build
fi

# check our test binary
if test ! -e $INDEXFS_BASE/io_test/io_driver
then
  echo "Cannot find the test binary -- oops"
  echo "It is supposed to be found at $INDEXFS_BASE/io_test/io_driver"
  exit 1
fi

INDEXFS_RUN=/tmp/indexfs
INDEXFS_OUTPUT=$INDEXFS_RUN/iotest-output
INDEXFS_BACKEND=$($INDEXFS_HOME/sbin/idxfs.sh backend)

# prepare test directories
rm -rf $INDEXFS_OUTPUT
mkdir -p $INDEXFS_OUTPUT

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

if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  if test ! -e $INDEXFS_CONF_DIR/hdfs_conf
  then
    echo "Cannot found our indexfs hdfs config file -- oops"
    exit 1
  fi
fi

# prepare indexfs-hdfs runtime if necessary
if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  LD_PATH=`$INDEXFS_HOME/sbin/hdfs.sh ldpath`
  export LD_LIBRARY_PATH=$LD_PATH
  export LIBHDFS_OPTS="-Djava.library.path=$LD_PATH"
  export CLASSPATH=`$INDEXFS_HOME/sbin/hdfs.sh classpath`
fi

# run tree test
if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  mpiexec -np 2 $INDEXFS_BASE/io_test/io_driver \
    --prefix=$(date +'%s') \
    --task=tree --dirs=1 --files=1600 --share_dirs \
    --hconfigfn=$INDEXFS_CONF_DIR/hdfs_conf \
    --configfn=$INDEXFS_CONF_DIR/indexfs_conf --srvlstfn=$INDEXFS_CONF_DIR/server_list \
    --log_dir=$INDEXFS_OUTPUT --log_file=$INDEXFS_OUTPUT/latency_histogram
else
  mpiexec -np 2 $INDEXFS_BASE/io_test/io_driver \
    --prefix=$(date +'%s') \
    --task=tree --dirs=1 --files=1600 --share_dirs \
    --configfn=$INDEXFS_CONF_DIR/indexfs_conf --srvlstfn=$INDEXFS_CONF_DIR/server_list \
    --log_dir=$INDEXFS_OUTPUT --log_file=$INDEXFS_OUTPUT/latency_histogram
fi

exit 0
