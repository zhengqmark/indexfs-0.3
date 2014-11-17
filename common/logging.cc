// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>

#include "common/logging.h"

namespace indexfs {

static inline
void OpenFSLog(const char* log_name) {
#ifndef NDEBUG
  FLAGS_minloglevel = 0;
  giga_logopen(LOG_DEBUG);
#else
  FLAGS_minloglevel = 2;
  giga_logopen(LOG_ERR);
#endif
  google::InitGoogleLogging(log_name);
}

void FlushFSLog() {
  google::FlushLogFiles(google::INFO);
}

void CloseFSLog() {
  giga_logclose();
  google::ShutdownGoogleLogging();
}

void OpenServerLog(const std::string &log_name) {
  OpenFSLog(log_name.c_str());
}

void OpenClientLog(const std::string &log_name) {
  OpenFSLog(log_name.c_str());
}

} /* namespace indexfs */
