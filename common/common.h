// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_INCLUDE_COMMON_H_
#define _INDEXFS_INCLUDE_COMMON_H_

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/util/mutexlock.h"

#include "port/port.h"
#include "thrift/indexfs_types.h"

namespace indexfs {

using leveldb::Env;
using leveldb::Slice;
using leveldb::Status;
using leveldb::Cache;
using leveldb::MutexLock;
using leveldb::port::CondVar;
using leveldb::port::Mutex;
using leveldb::Options;
using leveldb::WritableFile;
using leveldb::RandomAccessFile;

typedef uint64_t TINumber;
typedef const std::string Path;

} // namespace indexfs

#endif /* _INDEXFS_INCLUDE_COMMON_H_ */
