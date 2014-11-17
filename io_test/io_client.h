// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_MPI_IO_CLIENT_H_
#define _INDEXFS_MPI_IO_CLIENT_H_

#include "common/common.h"
#include "leveldb/util/histogram.h"

#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

// Abstract FS Interface
class IOClient;

DECLARE_bool(bulk_insert); // Is bulk_insert enabled? -- for IndexFS only
DECLARE_bool(print_ops); // Print op trace to stdout? -- useful for debugging

//////////////////////////////////////////////////////////////////////////////////
// IO CLIENT FACTORY
//

struct IOClientFactory {
  // Using IndexFS Interface
  static IOClient* GetIndexFSClient(int rank, const std::string &id);
  // Using Standard POSIX API (or FUSE)
  static IOClient* GetLocalFSClient(int rank, const std::string &id);
  // Using OrangeFS (a.k.a. PVFS2) Interface
  static IOClient* GetOrangeFSClient(int rank, const std::string &id);
};

//////////////////////////////////////////////////////////////////////////////////
// IO CLIENT INTERFACE
//

class IOClient {

  // To be called by IO factories only
  static IOClient* NewIndexFSClient();
  static IOClient* NewLocalFSClient();
  static IOClient* NewOrangeFSClient();

 public:
  IOClient() {}
  virtual ~IOClient();

  // Life-cycle Ctrl //
  virtual Status Init             ()                                 = 0;
  virtual Status Dispose          ()                                 = 0;

  // Integer-based Interface //
  virtual Status NewFile          (int dno, int fno, const std::string &prefix);
  virtual Status GetAttr          (int dno, int fno, const std::string &prefix);
  virtual Status MakeDirectory    (int dno, const std::string &prefix);
  virtual Status SyncDirectory    (int dno, const std::string &prefix);

  // Core FS Interface //
  virtual Status NewFile          (Path &path)                       = 0;
  virtual Status MakeDirectory    (Path &path)                       = 0;
  virtual Status MakeDirectories  (Path &path)                       = 0;
  virtual Status SyncDirectory    (Path &path)                       = 0;
  virtual Status ResetMode        (Path &path)                       = 0;
  virtual Status GetAttr          (Path &path)                       = 0;
  virtual Status ListDirectory    (Path &path)                       = 0;
  virtual Status Remove           (Path &path)                       = 0;
  virtual Status Rename           (Path &source, Path &destination)  = 0;

  // Other Interface //
  virtual void Noop() {}
  virtual void PrintMeasurements(FILE* output) {}

 private:
  friend class IOClientFactory;
  // No copying allowed
  IOClient(const IOClient&);
  IOClient& operator=(const IOClient&);
};

//////////////////////////////////////////////////////////////////////////////////
// IO MEASUREMENT CONTROL INTERFACE
//

DECLARE_string(tsdb_ip); // TSDB's IP address
DECLARE_int32(tsdb_port); // TSDB's UDP port

struct IOMeasurements {
  static void EnableMonitoring(IOClient* cli, bool enable);
  static void Reset(IOClient* cli);
  static void PrintMeasurements(IOClient* cli, FILE* output);
};

} /* namespace mpi */ } /* namespace indexfs */

#endif /* _INDEXFS_MPI_IO_CLIENT_H_ */
