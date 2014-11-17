// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADATA_CLIENT_H_
#define _INDEXFS_METADATA_CLIENT_H_

#include "common/config.h"
#include "common/bitmap.h"
#include "common/logging.h"
#include "common/dentcache.h"
#include "common/dmapcache.h"
#include "common/dircache.h"
#include "common/dirhandle.h"

#include "client.h"
#include "communication/rpc.h"
#include "util/measurement.h"

namespace indexfs {

struct FileDescriptor;
static const int MAX_NUM_FILEDESCRIPTORS = 128;

class MetadataClient : public Client {
 public:

  MetadataClient(Config* cfg);

  virtual ~MetadataClient();

  virtual Status Init() { return rpc_->Init(); }

  virtual Status AccessDir(Path &path);

  virtual Status Getattr(Path &path, StatInfo* info);

  virtual Status Mknod(Path &path, int16_t permission);

  virtual Status Mkdir(Path &path, int16_t permission);

  virtual Status Chmod(Path &path, int16_t permission);

  virtual Status Remove(Path &path);

  virtual Status Rename(Path &src, Path &dst);

  virtual Status Readdir(Path &path, std::vector<std::string>* result);

  virtual Status ReaddirPlus(Path &path,
                             std::vector<std::string>* names,
                             std::vector<StatInfo>* entries);

  virtual Status Fsyncdir(Path &path) { return Status::OK(); }

  virtual Status Open(Path &path, int16_t mode, int *fd);

  virtual Status Read(int fd, size_t offset, size_t size, char *buf,
                      int *ret_size);

  virtual Status Write(int fd, size_t offset, size_t size, const char*buf);

  virtual Status Close(int fd);

  virtual Status Dispose() { return rpc_->Shutdown(); }

  virtual void Noop();

  virtual void PrintMeasurements(FILE* output);

 protected:

  int SelectServer(DirHandle &handle, Path &entry);

  void UpdateBitmap(DirHandle &handle, GigaBitmap &bitmap);

  bool IsEntryExpired(DirEntryValue& value, int depth);

  // Basic namespace utilities
  //

  DirHandle FetchDir
    (TINumber dir_id, int zeroth_server);

  Status AddCacheEntry
    (TINumber parent, Path &dir, DirEntryValue* value);

  Status GetCacheEntry
    (TINumber parent, Path &dir, DirEntryValue* value);

  Status Lookup
    (int zeroth_server, TINumber directory, Path &entry, AccessInfo* info,
     int lease_time);

  Status ResolvePath
    (Path &path, TINumber* parent, int* zeroth_server, std::string* entry,
     int* depth = NULL);

  // Internal interfaces for core file system operations.
  //

  virtual Status RPC_Getattr
    (TINumber parent, Path &entry, StatInfo* info, DirHandle &handle,
     int lease_time=0);

  virtual Status RPC_Mknod
    (TINumber parent, Path &entry, int16_t permission, DirHandle &handle);

  virtual Status RPC_Mkdir(TINumber parent, Path &entry, int16_t permission,
                           int16_t hint_server, DirHandle &handle);

  virtual Status RPC_Chmod
    (TINumber parent, Path &entry, int16_t permission, DirHandle &handle);

  virtual Status RPC_Remove
    (TINumber parent, Path &entry, DirHandle &handle);

  virtual Status RPC_Create(TINumber parent, Path &entry, int server,
                            const StatInfo &info, const std::string &link,
                            const std::string &data, DirHandle &handle);

  virtual Status RPC_Open(int parent, Path &entry, int mode, DirHandle &handle,
                          OpenResult &ret);

  virtual Status Internal_ResolvePath
    (Path &path, TINumber* parent, int* zeroth_server, std::string* entry, int*);

  int LeaseTime(int depth);

  RPC* rpc_;
  DirCache* dir_cache_;
  DirEntryCache<DirEntryValue>* dent_cache_;
  DirMappingCache* dmap_cache_;

  enum MetadataServerOps {
    oGetattr, oMknod, oMkdir, oCreateEntry, oChmod, oRemove, oRename,
    oReaddir, oReadBitmap, oOpen, oRead, oWrite, oClose, oLookup,
    NumMetadataOps
  };

  Measurement* measure_;

 private:
  // No copy allowed
  MetadataClient(const MetadataClient&);
  MetadataClient& operator=(const MetadataClient&);

  Config* cfg_;
  Env* env_;

  int AllocateFD();
  int fd_count_;
  FileDescriptor* fd_[MAX_NUM_FILEDESCRIPTORS];
};

} /* namespace indexfs */

#endif /* _INDEXFS_METADATA_CLIENT_H_ */
