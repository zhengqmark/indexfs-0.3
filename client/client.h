// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_CLIENT_INTERFACE_H_
#define _INDEXFS_CLIENT_INTERFACE_H_

#include <sys/stat.h>

#include "common/config.h"
#include "common/common.h"
#include "thrift/indexfs_types.h"

namespace indexfs {

class ClientFactory;

/* -----------------------------------------------------------------
 * Main Client Interface
 * -----------------------------------------------------------------
 */

class Client {
 protected:

  // No public creates
  explicit Client() { }
  friend class ClientFactory;

 public:

  virtual ~Client() { }

  //-------------------------------------------------------
  /* Client Life-Cycle Management */
  //-------------------------------------------------------

  virtual Status Init() = 0;

  virtual Status Dispose() = 0;

  //-------------------------------------------------------
  /* File System Metadata Management */
  //-------------------------------------------------------

  virtual Status Getattr(Path &path, StatInfo* info) = 0;

  virtual Status Mknod(Path &path, int16_t permission) = 0;

  virtual Status Mkdir(Path &path, int16_t permission) = 0;

  virtual Status Chmod(Path &path, int16_t permission) = 0;

  virtual Status Remove(Path &path) = 0;

  virtual Status Rename(Path &source, Path &target) = 0;

  //-------------------------------------------------------
  /* File System Directory Management */
  //-------------------------------------------------------

  virtual Status Readdir(Path &path,
                         std::vector<std::string>* result) {
    return Status::OK();
  }

  virtual Status ReaddirPlus(Path &path,
                             std::vector<std::string>* names,
                             std::vector<StatInfo>* entries) {
    return Status::OK();
  }

  virtual Status Fsyncdir(Path &path) { return Status::OK(); }

  virtual Status AccessDir(Path &path) { return Status::OK(); };

  //-------------------------------------------------------
  /* File System I/O Operations */
  //-------------------------------------------------------

  virtual Status Read(
      int fd, size_t offset, size_t size, char *buf, int *ret_size) = 0;

  virtual Status Write(
      int fd, size_t offset, size_t size, const char* buf) = 0;

  virtual Status Close(int fd) = 0;

  virtual Status Open(Path &path, int16_t mode, int *fd) = 0;

  //-------------------------------------------------------
  /* Miscellaneous Performance Benchmarking Utilities */
  //-------------------------------------------------------

  virtual void Noop() = 0;

  virtual void PrintMeasurements(FILE* output) = 0;
};

/* -----------------------------------------------------------------
 * Client Factory Interface
 * -----------------------------------------------------------------
 */

class ClientFactory {
 public:
  virtual ~ClientFactory() { }
  virtual Client* GetClient(Config* config) = 0;
};

extern ClientFactory* GetDefaultClientFactory();

} /* namespace indexfs */

#endif /* _INDEXFS_CLIENT_INTERFACE_H_ */
