// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_LOGGING_H_
#define _INDEXFS_COMMON_LOGGING_H_

extern "C" {
#include "common/debugging.h" // legacy header
}

#include <string>
#include <sstream>
#include <glog/logging.h>

namespace indexfs {

extern void FlushFSLog();

extern void CloseFSLog();

extern void OpenServerLog(const std::string &log_name);

extern void OpenClientLog(const std::string &log_name);

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_LOGGING_H_ */
