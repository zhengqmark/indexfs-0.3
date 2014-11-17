// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADATA_SERVER_H_
#define _INDEXFS_METADATA_SERVER_H_

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include "thrift/MetadataService.h"
#include <glog/logging.h>

#include "backends/metadb.h"
#include "common/dircache.h"
#include "common/dentcache.h"
#include "common/dmapcache.h"
#include "common/dirhandle.h"
extern "C" {
  #include "common/options.h"
}
#include "util/measurement.h"
#include "client/metadata_client.h"

namespace indexfs {

class DirHandle;
class SplitThread;

class MetadataServer : virtual public MetadataServiceIf {
public:
  MetadataServer() {
  }

  ~MetadataServer() {
  }

  static void Init(Config* options,
                   MetadataBackend* mdb,
                   Env* env,
                   DirEntryCache<ServerDirEntryValue>* dent_cache,
                   DirMappingCache* dmap_cache,
                   DirCache* dir_cache,
                   Measurement* measure,
                   SplitThread* split_thread);

  static void GetInstrumentPoints(std::vector<std::string> &points);

  bool InitRPC();

  void Getattr(StatInfo& _return, const TInodeID dir_id,
               const std::string& path, int lease_time);

  void IGetattr(StatInfo& _return, const std::string& path);

  void Access(AccessInfo& _return, const TInodeID dir_id,
              const std::string& path, int lease_time);

  void Mknod(const TInodeID dir_id, const std::string& path,
             const int16_t permission);

  void IMknod(const std::string& path, const int16_t permission);

  void Mkdir(const TInodeID dir_id, const std::string& path,
             const int16_t permission, const int16_t hint_server);

  void IMkdir(const std::string& path, const int16_t permission);

  void CreateEntry(const TInodeID dir_id, const std::string& path,
                   const StatInfo& info, const std::string& link,
                   const std::string& data);

  void CreateNamespace(LeaseInfo& _return, const TInodeID dir_id,
                       const std::string& path, const int16_t permission);

  void CloseNamespace(const TInodeID dir_id);

  void CreateZeroth(const TInodeID dir_id);

  void Chmod(const TInodeID dir_id, const std::string& path,
             const int16_t permission);

  void IChmod(const std::string& path, const int16_t permission);

  void IChfmod(const std::string& path, const int16_t permission);

  void Remove(const TInodeID dir_id,
              const std::string& path);

  void IRemove(const std::string& path);

  void Rename(const TInodeID src_id, const std::string& src_path,
              const TInodeID dst_id, const std::string& dst_path);

  void IRename(const std::string& src_path, const std::string& dst_path);

  void Readdir(ScanResult& _return,
              const TInodeID dir_id, const int64_t partition,
              const std::string& start_key, const int16_t max_num_entries);


  void ReaddirPlus(ScanPlusResult& _return,
                   const TInodeID dir_id, const int64_t partition,
                   const std::string& start_key, const int16_t max_num_entries);

  void ReadBitmap(GigaBitmap& _return, const TInodeID dir_id);

  void UpdateBitmap(const TInodeID dir_id, const GigaBitmap& mapping);

  void OpenFile(OpenResult& _return, const TInodeID dir_id, const std::string& path,
                const int16_t mode, const int16_t auth);

  void Read(ReadResult& _return, const TInodeID dir_id, const std::string& path,
            const int32_t offset, const int32_t size);

  void Write(WriteResult& _return, const TInodeID dir_id, const std::string& path,
             const std::string& data, const int32_t offset);

  void CloseFile(const TInodeID dir_id, const std::string& path,
                 const int16_t mode);

  void InsertSplit(const TInodeID dir_id,
                   const int16_t parent_index, const int16_t child_index,
                   const std::string& path_split_files,
                   const GigaBitmap& bitmap,
                   const int64_t min_seq, const int64_t max_seq,
                   const int64_t num_entries);

  static MetadataBackend* mdb_;
  static DirEntryCache<ServerDirEntryValue>* dent_cache_;
  static DirMappingCache* dmap_cache_;
  static DirCache* dir_cache_;
  static Config* options_;
  static Mutex split_mtx_;
  static int split_flag;
  static Mutex insert_mtx_;
  static SplitThread* split_thread_;
  static Env* env_;
  static bool no_overwrite_;
  static MetadataClient* proxy_;

  enum MetadataServerOps {
    oGetattr, oMknod, oMkdir, oCreateEntry, oCreateZeroth, oChmod,
    oRemove, oRename, oReaddir, oReadBitmap, oUpdateBitmap, oInsertSplit,
    oOpen, oRead, oWrite, oClose, oSplit, oAccess, NumMetadataOps
  };
  static Measurement* measure_;

private:

  bool CheckSplit(const DirHandle &hdir, int index);

  void ScheduleSplit(const TInodeID dir_id,
                     const int partition,
                     DirHandle &hdir);

  void Split(const TInodeID dir_id,
             const int partition,
             DirHandle& hdir);

  void InsertSplitRemote(const TInodeID dir_id,
                         const int child_server,
                         const int16_t parent_index,
                         const int16_t child_index,
                         const std::string &path_split_files,
                         const giga_mapping_t *bitmap,
                         const int64_t min_seq,
                         const int64_t max_seq,
                         const int64_t num_entries);

  int CheckAddressing(giga_mapping_t *mapping,
                      const std::string &path);

  int AssignServerForNewInode();

  DirHandle FetchDir(const TInodeID dir_id);

  bool CreateZerothRemote(int zeroth_server, const TInodeID dir_id);

  bool UpdateBitmapRemote(int zeroth_server, int dir_id, DirHandle &hdir);

  void InsertShadow(const TInodeID dir_id);

  void GenerateFilePath(const TInodeID dir_id,
                        const std::string& objname,
                        std::string* file_path,
                        std::string* dir_path=0);

  void WriteLockDirEntry(const TInodeID dir_id, const std::string& objname,
                         DirHandle &hdir, Cache::Handle **handle);

  void UnlockDirEntry(DirHandle &hdir, Cache::Handle* handle);

  struct PathInfo {
    int depth_;
    TInodeID parent_;
    std::string entry_;
  };

  TInodeID NextDirectoryID();

  void ResolvePath(PathInfo* _return,
                   const std::string &path, bool create_if_necessary);

  Status Lookup(TInodeID* _return,
                TInodeID dir_id, const std::string &entry, bool create_if_necessary);

  friend class SplitThread;
  friend class DirEntryLockHandler;
};

} // namespace indexfs

#endif
