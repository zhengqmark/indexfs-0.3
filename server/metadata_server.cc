// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sstream>
#include <algorithm>
#include <fcntl.h>
#include <cmath>
#include "common/config.h"
#include "metadata_server.h"
#include "split_thread.h"

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

namespace indexfs {

MetadataBackend* MetadataServer::mdb_ = NULL;
DirCache* MetadataServer::dir_cache_ = NULL;
DirMappingCache* MetadataServer::dmap_cache_ = NULL;
DirEntryCache<ServerDirEntryValue>* MetadataServer::dent_cache_ = NULL;
Config* MetadataServer::options_ = NULL;
Measurement* MetadataServer::measure_ = NULL;
SplitThread* MetadataServer::split_thread_ = NULL;
MetadataClient* MetadataServer::proxy_= NULL;
Env* MetadataServer::env_ = NULL;
Mutex MetadataServer::split_mtx_;
int MetadataServer::split_flag = 0;
Mutex MetadataServer::insert_mtx_;

static const bool kNoOverwrite = true; // FIXME: false for POSIX_ENV
static const int kNumInstrumentPoints = 18;
static const char* kMetadataServerOpsName[kNumInstrumentPoints] = {
    "getattr", "mknod", "mkdir", "createentry", "createzeroth", "chmod",
    "remove", "rename", "readdir", "readbitmap", "updatebitmap", "insertsplit",
    "open", "read", "write", "close", "split", "access"
};
static const int kTimeEpsilon = 10000;

void MetadataServer::Init(Config* options,
                          MetadataBackend* mdb,
                          Env* env,
                          DirEntryCache<ServerDirEntryValue>* dent_cache,
                          DirMappingCache* dmap_cache,
                          DirCache* dir_cache,
                          Measurement* measure,
                          SplitThread* split_thread) {
  options_ = options;
  mdb_ = mdb;
  env_ = env;
  dent_cache_ = dent_cache;
  dmap_cache_ = dmap_cache;
  dir_cache_ = dir_cache;
  DirHandle::dmap_cache_ = dmap_cache;
  DirHandle::dir_cache_ = dir_cache;
  measure_ = measure;
  split_thread_ = split_thread;
}

void MetadataServer::GetInstrumentPoints(std::vector<std::string> &points) {
  for (int i = 0; i < kNumInstrumentPoints; ++i)
    points.push_back(std::string(kMetadataServerOpsName[i]));
}

template <class TException>
inline void SanityCheck(const bool condition, const TException &e) {
  if (condition) throw e;
}

bool MetadataServer::InitRPC() {
  return true;
}

DirHandle MetadataServer::FetchDir(const TInodeID dir_id) {
  Directory* dir;
  dir_cache_->Get(dir_id, &dir);

  Cache::Handle* handle = dmap_cache_->Get(dir_id);
  if (handle == NULL) {
    MutexLock l(&dir->partition_mtx);
    handle = dmap_cache_->Get(dir_id);
    if (handle == NULL) {
      giga_mapping_t mapping;
      if (mdb_->ReadBitmap(dir_id, &mapping) != 0) {
        LOG(ERROR) << "Error: Directory (" << dir_id << ") cannot be found";
        return DirHandle(NULL, NULL);
      }
      handle = dmap_cache_->Put(dir_id, mapping);
    }
  }

  return DirHandle(dir, handle);
}

int MetadataServer::CheckAddressing(giga_mapping_t *mapping,
                                    const std::string &path) {
  int index, server = 0;
  index = giga_get_index_for_file(mapping, path.c_str());
  server = giga_get_server_for_index(mapping, index);
  if (server != options_->GetSrvID()) {
    index = -1;
  }
  return index;
}

inline GigaBitmap CopyGigaMap(const giga_mapping_t *mapping) {
  GigaBitmap bitmap;
  bitmap.id = mapping->id;
  bitmap.bitmap =
    std::string((char *) mapping->bitmap, sizeof(mapping->bitmap));
  bitmap.curr_radix = mapping->curr_radix;
  bitmap.zeroth_server = mapping->zeroth_server;
  bitmap.num_servers = mapping->server_count;
  return bitmap;
}

inline giga_mapping_t CopyMapping(const GigaBitmap &mapping) {
  giga_mapping_t new_mapping;
  new_mapping.id = mapping.id;
  new_mapping.curr_radix = mapping.curr_radix;
  new_mapping.server_count = mapping.num_servers;
  new_mapping.zeroth_server = mapping.zeroth_server;
  memcpy(new_mapping.bitmap, mapping.bitmap.data(),
         sizeof(new_mapping.bitmap));
  return new_mapping;
}

class DirEntryLockHandler {
  public:
    explicit DirEntryLockHandler(MetadataServer* server,
                                 const TInodeID dir_id,
                                 const std::string& objname,
                                 DirHandle &hdir) : server_(server),
                                 hdir_(hdir), handle_(NULL) {
      server_->WriteLockDirEntry(dir_id, objname, hdir_, &handle_);
    }

    ~DirEntryLockHandler() {
      server_->UnlockDirEntry(hdir_, handle_);
    }

  private:
    MetadataServer* server_;
    DirHandle& hdir_;
    Cache::Handle *handle_;

    DirEntryLockHandler(const DirEntryLockHandler&);
    void operator=(const DirEntryLockHandler&);
};

void MetadataServer::WriteLockDirEntry(const TInodeID dir_id,
                                       const std::string& objname,
                                       DirHandle &hdir,
                                       Cache::Handle **handle) {
  Status s = dent_cache_->GetHandle(dir_id, objname, handle);
  uint64_t now = env_->NowMicros();
  if (s.ok()) {
    ServerDirEntryValue* value = reinterpret_cast<ServerDirEntryValue*>(
                                          dent_cache_->Value(*handle));
    value->write_rate.AddRequest(now);
    while (value->status == LEASE_WRITE_STATUS) {
      hdir.dir->partition_cv.Wait();
    }
    if (now < value->expire_time + kTimeEpsilon) {
      value->status = LEASE_WRITE_STATUS;
      uint64_t micros = value->expire_time - now + kTimeEpsilon;
      hdir.dir->partition_mtx.Unlock();
      env_->SleepForMicroseconds(micros);
      hdir.dir->partition_mtx.Lock();
    }
  } else {
    ServerDirEntryValue* value = new ServerDirEntryValue();
    value->status = LEASE_WRITE_STATUS;
    value->write_rate.AddRequest(now);
    value->inode_id = -1;
    value->zeroth_server = -1; // use an invalid number as a place holder
    *handle = dent_cache_->Insert(dir_id, objname, value);
  }
}

void MetadataServer::UnlockDirEntry(DirHandle &hdir,
                                    Cache::Handle* handle) {
  if (handle != NULL) {
    ServerDirEntryValue* value = reinterpret_cast<ServerDirEntryValue*>(
                                          dent_cache_->Value(handle));
    value->status = LEASE_READ_STATUS;
    dent_cache_->ReleaseHandle(handle);
  }
  hdir.dir->partition_cv.SignalAll();
}

void MetadataServer::Getattr(StatInfo& _return, const TInodeID dir_id,
                             const std::string& objname, int lease_time) {
  MeasurementHelper helper(oGetattr, measure_);

  DirHandle hdir = FetchDir(dir_id);

  if (hdir.mapping == NULL) {
    DLOG(WARNING) << "No such directory found under ID: " << dir_id;
    throw FileNotFoundException();
  }

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  /*
  ServerDirEntryValue value;
  Status s = dent_cache_->Get(dir_id, objname, &value);
  if (s.ok()) {
    if (value.status == LEASE_WRITE_STATUS) {
      hdir.dir->partition_cv.Wait();
    }
  }
  */

  if (mdb_->Getattr(dir_id, index, objname, &_return) != 0) {
    throw FileNotFoundException();
  }

  /*
  time_t new_expire_time = env_->NowMicros() + lease_time;
  if (new_expire_time > value.expire_time) {
    value.expire_time = env_->NowMicros() + lease_time;
  }
  value.status = LEASE_READ_STATUS;
  dent_cache_->Put(dir_id, objname, value);
  */
}

uint64_t GetLeaseTime(const ServerDirEntryValue *value, const int depth_time) {
  /*
  uint64_t sum = value->write_rate.GetCount() + value->read_rate.GetCount();
  if (sum <= 1) {
    return std::min(depth_time, 400*1000);
  } else {
    return value->read_rate.GetCount() * 800000 / sum;
  }
  */
  /*
  uint64_t lease_time = sqrt(value->write_rate.GetInterval() *
      (2000000 / std::max(value->read_rate.GetInterval(), (uint64_t) 1000)));
  return lease_time;
  */
  /*
  return std::min(std::max(value->read_rate.GetInterval() * 1000000 /
    std::max(value->read_rate.GetInterval() + value->write_rate.GetInterval(),
     (uint64_t) 1), (uint64_t) 200000), (uint64_t) 1000000);
  */
  return 1000 * 1000;
  //return depth_time;
}

void MetadataServer::Access(AccessInfo& _return, const TInodeID dir_id,
                            const std::string& objname, int lease_time) {
  MeasurementHelper helper(oAccess, measure_);

  DirHandle hdir = FetchDir(dir_id);

  if (hdir.mapping == NULL) {
    DLOG(WARNING) << "No such directory found under ID: " << dir_id;
    throw FileNotFoundException();
  }

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  Cache::Handle* dent_handle;
  ServerDirEntryValue* value;
  Status s = dent_cache_->GetHandle(dir_id, objname, &dent_handle);

  if (s.ok()) {
    value = reinterpret_cast<ServerDirEntryValue*>(
                dent_cache_->Value(dent_handle));
    while (value->status == LEASE_WRITE_STATUS) {
      uint64_t now = env_->NowMicros();
      if (now + kTimeEpsilon > value->expire_time) {
        hdir.dir->partition_cv.Wait();
      } else break;
    }
    if (value->inode_id == -1 || value->zeroth_server == -1) {
      StatInfo stat;
      if (mdb_->Getattr(dir_id, index, objname, &stat) != 0)
         throw FileNotFoundException();
      value->inode_id = stat.id;
      value->zeroth_server = stat.zeroth_server;
    }
    _return.id = value->inode_id;
    _return.zeroth_server = value->zeroth_server;
  } else {
    StatInfo stat;
    if (mdb_->Getattr(dir_id, index, objname, &stat) != 0)
      throw FileNotFoundException();
    if (!S_ISDIR(stat.mode)) throw NotDirectoryException();
    value = new ServerDirEntryValue();
    _return.id = value->inode_id = stat.id;
    _return.zeroth_server = value->zeroth_server = stat.zeroth_server;
    dent_handle = dent_cache_->Insert(dir_id, objname, value);
  }

  uint64_t now = env_->NowMicros();
  value->read_rate.AddRequest(now);

  uint64_t srv_lease_time;
  if (value->status == LEASE_WRITE_STATUS) {
    srv_lease_time = value->expire_time - now;
  } else {
    srv_lease_time = GetLeaseTime(value, lease_time);
    //uint64_t srv_lease_time = lease_time;
  }
  uint64_t new_expire_time = now + srv_lease_time;
  if (new_expire_time > value->expire_time) {
    value->expire_time = new_expire_time;
  }
  _return.lease_time = new_expire_time;
  value->status = LEASE_READ_STATUS;
  dent_cache_->ReleaseHandle(dent_handle);
  hdir.dir->partition_cv.Signal();
}

void MetadataServer::Mknod(const TInodeID dir_id, const std::string& objname,
                           const int16_t permission) {
  MeasurementHelper helper(oMknod, measure_);

  DirHandle hdir = FetchDir(dir_id);

  if (hdir.mapping == NULL) {
    DLOG(WARNING) << "No such directory found under ID: " << dir_id;
    throw FileNotFoundException();
  }

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  SanityCheck((mdb_->Create(dir_id, index, objname, "") != 0),
              FileAlreadyExistException());

  ScheduleSplit(dir_id, index, hdir);
}

int MetadataServer::AssignServerForNewInode() {
  return rand() % options_->GetSrvNum();
}

bool MetadataServer::CreateZerothRemote(int zeroth_server,
                                        const TInodeID dir_id) {
  boost::shared_ptr<TSocket>
    socket(new TSocket(options_->GetSrvIP(zeroth_server),
                       options_->GetSrvPort(zeroth_server)));
  boost::shared_ptr<TBufferedTransport>
    transport(new TBufferedTransport(socket));
  boost::shared_ptr<TBinaryProtocol>
    protocol(new TBinaryProtocol(transport));
  MetadataServiceClient client(protocol);

  try {
    transport->open();
    client.CreateZeroth(dir_id);
    transport->close();
  } catch (TException &tx) {
    LOG(ERROR) << "ERROR: " << tx.what() << std::endl;
    return false;
  }
  return true;
}

void MetadataServer::Mkdir(const TInodeID dir_id, const std::string& objname,
                       const int16_t permission, const int16_t hint_server) {
  MeasurementHelper helper(oMkdir, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  TInodeID object_id = mdb_->NewInodeNumber();
  int zeroth_server = hint_server;
  SanityCheck(mdb_->Mkdir(dir_id, index, objname, object_id,
                          zeroth_server, options_->GetSrvNum())!=0,
              FileAlreadyExistException());
  if (zeroth_server == options_->GetSrvID()) {
    CreateZeroth(object_id);
  } else {
    SanityCheck(CreateZerothRemote(zeroth_server, object_id)!=true,
                FileAlreadyExistException());
  }

  ScheduleSplit(dir_id, index, hdir);
}

void MetadataServer::CreateEntry(const TInodeID dir_id,
                                 const std::string& objname,
                                 const StatInfo& info,
                                 const std::string& link,
                                 const std::string& data) {
  MeasurementHelper helper(oCreateEntry, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  SanityCheck((mdb_->CreateEntry(dir_id, index, objname, info, link, data)!=0),
              FileAlreadyExistException());

  ScheduleSplit(dir_id, index, hdir);
}

void MetadataServer::CreateNamespace(LeaseInfo& _return, const TInodeID dir_id,
                       const std::string& objname, const int16_t permission) {
  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  TInodeID object_id = mdb_->NewInodeNumber();
  int zeroth_server = AssignServerForNewInode();
  SanityCheck(mdb_->Mkdir(dir_id, index, objname, object_id,
                         zeroth_server, options_->GetSrvNum())!=0,
             FileAlreadyExistException());

  // do not create zeroth server
  //

  int bulk_size = options_->GetDirBulkSize();
  _return.timeout = 0;
  _return.max_dirs = bulk_size;
  _return.next_inode = mdb_->NewInodeBatch(bulk_size);
  _return.next_zeroth_server = AssignServerForNewInode();

  ScheduleSplit(dir_id, index, hdir);
}

void MetadataServer::CloseNamespace(const TInodeID dir_id) {
  CreateZeroth(dir_id);
  Mknod(dir_id, ".BULK_DIRECTORY", 0644);
}

void MetadataServer::CreateZeroth(const TInodeID dir_id) {
  MeasurementHelper helper(oCreateZeroth, measure_);

  Directory* dir;
  dir_cache_->Get(dir_id, &dir);

  MutexLock(&(dir->partition_mtx));
  if (mdb_->Mkdir(dir_id, -1, "", dir_id, options_->GetSrvID(),
                  options_->GetSrvNum()) != 0) {
    dir_cache_->Release(dir_id, dir);
    throw FileAlreadyExistException();
  }
  dir_cache_->Release(dir_id, dir);
}

void MetadataServer::Chmod(const TInodeID dir_id, const std::string& objname,
                           const int16_t permission) {
  MeasurementHelper helper(oChmod, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  StatInfo stat;
  if (mdb_->Getattr(dir_id, index, objname, &stat) != 0)
      throw FileNotFoundException();
  if (S_ISDIR(stat.mode)) {
    DirEntryLockHandler dent_lock(this, dir_id, objname, hdir);
    SanityCheck(mdb_->Chmod(dir_id, index, objname, permission)!=0,
                FileNotFoundException());
  } else {
    SanityCheck(mdb_->Chmod(dir_id, index, objname, permission)!=0,
                FileNotFoundException());
  }
  // TODO: wait for expiration? revoke handler from clients?
}

void MetadataServer::Remove(const TInodeID dir_id,
                            const std::string& objname) {
  MeasurementHelper helper(oRemove, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  StatInfo stat;
  if (mdb_->Getattr(dir_id, index, objname, &stat) != 0)
      throw FileNotFoundException();
  if (S_ISDIR(stat.mode)) {
    DirEntryLockHandler dent_lock(this, dir_id, objname, hdir);
    //TODO: how to delete a directory? how to check if the directory is empty?
    SanityCheck(mdb_->Remove(dir_id, index, objname)!=0,
                FileNotFoundException());
  } else {
    SanityCheck(mdb_->Remove(dir_id, index, objname)!=0,
                FileNotFoundException());
  }
  //TODO: clean up the entry in the directory ent cache
}

void MetadataServer::Rename(const TInodeID src_id, const std::string& src_path,
                            const TInodeID dst_id, const std::string& dst_path)
{
  MeasurementHelper helper(oRename, measure_);

  SanityCheck(dst_id == src_id, FileNotInSameServer());

  DirHandle sdir = FetchDir(src_id);
  SanityCheck(sdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(sdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(sdir.mapping, src_path)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(sdir.mapping);
     throw se;
  }

  StatInfo info;
  SanityCheck(mdb_->Getattr(src_id, index, src_path, &info)!=0,
              FileNotFoundException());

  if (S_ISDIR(info.mode)) {
    DirEntryLockHandler dent_lock(this, src_id, src_path, sdir);
    int dst_index = CheckAddressing(sdir.mapping, dst_path);
    SanityCheck(dst_index >= 0, FileNotInSameServer());
    SanityCheck((mdb_->CreateEntry(dst_id, dst_index, dst_path, info,"", "")!=0),
                FileAlreadyExistException());
    SanityCheck(mdb_->Remove(src_id, index, src_path)!=0,
                FileNotFoundException());
  } else {
    int dst_index = CheckAddressing(sdir.mapping, dst_path);
    SanityCheck(dst_index >= 0, FileNotInSameServer());
    SanityCheck((mdb_->CreateEntry(dst_id, dst_index, dst_path, info,"", "")!=0),
                FileAlreadyExistException());
    SanityCheck(mdb_->Remove(src_id, index, src_path)!=0,
                FileNotFoundException());
  }
}

void MetadataServer::Readdir(ScanResult& _return,
                             const TInodeID dir_id,
                             const int64_t partition,
                             const std::string& start_key,
                             const int16_t max_num_entries) {
  MeasurementHelper helper(oReaddir, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());
  _return.mapping = CopyGigaMap(hdir.mapping);

  unsigned char more_entries;
  SanityCheck(mdb_->Readdir(dir_id, partition, start_key, max_num_entries,
                           &(_return.entries), &(_return.end_key), &more_entries) !=0,
              FileNotFoundException());
  _return.more_entries = more_entries;
}

void MetadataServer::ReaddirPlus(ScanPlusResult& _return,
                                 const TInodeID dir_id,
                                 const int64_t partition,
                                 const std::string& start_key,
                                 const int16_t max_num_entries) {

  MeasurementHelper helper(oReaddir, measure_);
  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());
  _return.mapping = CopyGigaMap(hdir.mapping);

  unsigned char more_entries;
  SanityCheck(mdb_->ReaddirPlus(dir_id, partition, start_key, max_num_entries,
                                &(_return.names), &(_return.entries),
                                &(_return.end_key), &more_entries) !=0,
              FileNotFoundException());
  _return.more_entries = more_entries;
}


void MetadataServer::ReadBitmap(GigaBitmap& _return, const TInodeID dir_id) {
  MeasurementHelper helper(oReadBitmap, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());
  _return = CopyGigaMap(hdir.mapping);
}

void MetadataServer::UpdateBitmap(const TInodeID dir_id,
                                  const GigaBitmap &mapping) {
  MeasurementHelper helper(oUpdateBitmap, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());
  giga_mapping_t update_mapping = CopyMapping(mapping);
  giga_update_cache(hdir.mapping, &update_mapping);
  mdb_->UpdateBitmap(dir_id, *hdir.mapping);
}

bool MetadataServer::CheckSplit(const DirHandle &hdir, int index) {
  hdir.dir->partition_size ++;
  return (hdir.dir->partition_size >= options_->GetSplitThreshold() &&
          giga_is_splittable(hdir.mapping, index) == 1 &&
          hdir.dir->split_flag == 0);
}

void MetadataServer::ScheduleSplit(const TInodeID dir_id,
                                   const int partition,
                                   DirHandle &hdir) {
  /*
  (void) dir_id;
  (void) hdir;
  (void) partition;
  */
  if (CheckSplit(hdir, partition) == true) {
    //Split(dir_id, partition, hdir);
    hdir.dir->split_flag = 1;
    split_thread_->AddSplitTask(dir_id, partition);
  }
}

bool MetadataServer::UpdateBitmapRemote(int zeroth_server,
                                        int dir_id,
                                        DirHandle &hdir) {
  boost::shared_ptr<TSocket>
    socket(new TSocket(options_->GetSrvIP(zeroth_server),
                       options_->GetSrvPort(zeroth_server)));
  boost::shared_ptr<TBufferedTransport>
    transport(new TBufferedTransport(socket));
  boost::shared_ptr<TBinaryProtocol>
    protocol(new TBinaryProtocol(transport));
  MetadataServiceClient client(protocol);

  try {
    transport->open();
    GigaBitmap mapping = CopyGigaMap(hdir.mapping);
    client.UpdateBitmap(dir_id, mapping);
    transport->close();
  } catch (TException &tx) {
    LOG(ERROR) << "ERROR: " << tx.what() << std::endl;
    return false;
  }
  return true;
}

void MetadataServer::Split(const TInodeID dir_id,
                           const int parent,
                           DirHandle &hdir) {
  //TODO: fault tolerance order?
  MeasurementHelper helper(oSplit, measure_);

  MutexLock split_mtx_lock(&split_mtx_);
  MutexLock mutexlock(&hdir.dir->partition_mtx);

  int parent_srv = options_->GetSrvID();
  int child = giga_index_for_splitting(hdir.mapping, parent);
  int child_srv = giga_get_server_for_index(hdir.mapping, child);

  LOG(INFO) << "split[" << dir_id << "]: p" << parent << "s" << parent_srv //
            << "--> p" << child << "s" << child_srv;

  int ret = 0;
  if (parent_srv != child_srv) {
    char split_dir_path_buf[PATH_MAX] = {0};
    snprintf(split_dir_path_buf, sizeof(split_dir_path_buf),
             "%ssst-d%d-p%dp%d-s%ds%d",
             options_->GetSplitDir().c_str(), (int) dir_id,
             parent, child, parent_srv, child_srv);
    std::string split_dir_path(split_dir_path_buf);

    uint64_t min_seq, max_seq;
    ret = mdb_->Extract(dir_id, parent, child, split_dir_path,
                        &min_seq, &max_seq);

    if (ret > 0)
       InsertSplitRemote(dir_id, child_srv, parent, child,
                         split_dir_path, hdir.mapping,
                         min_seq, max_seq, ret);
  }

  if (ret >= 0) {
    giga_update_mapping(hdir.mapping, child);
    hdir.dir->partition_size -= ret;
    if (mdb_->UpdateBitmap(dir_id, *hdir.mapping) < 0) {
      LOG(ERROR) << "ERROR: failed to write bitmap (" << dir_id << ")\n";
    }
    if (parent_srv != child_srv) {
      UpdateBitmapRemote(hdir.mapping->zeroth_server, dir_id, hdir);
      mdb_->ExtractClean();
    }
  }

  hdir.dir->split_flag = 0;
}

void MetadataServer::InsertSplitRemote(const TInodeID dir_id,
                                       const int child_server,
                                       const int16_t parent_index,
                                       const int16_t child_index,
                                       const std::string &path_split_files,
                                       const giga_mapping_t *bitmap,
                                       const int64_t min_seq,
                                       const int64_t max_seq,
                                       const int64_t num_entries) {
  boost::shared_ptr<TSocket>
    socket(new TSocket(options_->GetSrvIP(child_server),
                       options_->GetSrvPort(child_server)));
  boost::shared_ptr<TBufferedTransport>
    transport(new TBufferedTransport(socket));
  boost::shared_ptr<TBinaryProtocol>
    protocol(new TBinaryProtocol(transport));
  MetadataServiceClient client(protocol);

  try {
    transport->open();
    client.InsertSplit(dir_id, parent_index, child_index, path_split_files,
                       CopyGigaMap(bitmap), min_seq, max_seq, num_entries);
    transport->close();
  } catch (TException &tx) {
    LOG(ERROR) << "ERROR (InsertSplitRemote): " << tx.what() << std::endl;
  }
}

void MetadataServer::InsertSplit(const TInodeID dir_id,
                                 const int16_t parent_index,
                                 const int16_t child_index,
                                 const std::string& path_split_files,
                                 const GigaBitmap& bitmap,
                                 const int64_t min_seq,
                                 const int64_t max_seq,
                                 const int64_t num_entries) {
  MeasurementHelper helper(oInsertSplit, measure_);

  LOG(INFO) << "InsertSplit[" << dir_id << "]: " << path_split_files;

  mdb_->BulkInsert(path_split_files, min_seq, max_seq); // handle failure
  DirHandle hdir = FetchDir(dir_id);
  if (hdir.mapping == 0) {
    giga_mapping_t mapping = CopyMapping(bitmap);
    giga_update_mapping(&mapping, child_index);
    if (mdb_->CreateBitmap(dir_id, mapping, options_->GetSrvID()) == 0)
      dmap_cache_->Insert(dir_id, mapping);
    Directory* dir;
    dir_cache_->Get(dir_id, &dir);
    dir->partition_size += num_entries;
    dir_cache_->Release(dir_id, dir);
  } else {
    giga_update_mapping(hdir.mapping, child_index);
    mdb_->UpdateBitmap(dir_id, *hdir.mapping);
    hdir.dir->partition_size += num_entries;
  }
}

void MetadataServer::InsertShadow(const TInodeID dir_id) {
  std::stringstream ss;
  ss << options_->GetLevelDBDir();
  ss << "s" << options_->GetSrvID()<< "_" << dir_id;
  std::string path = ss.str();
  std::string temp = path + ".temp";

  uint64_t min_seq, max_seq;
  ReadonlyMetadataBackend shadow;

  if (shadow.Init(path,
      options_->GetHDFSIP(), options_->GetHDFSPort(), options_->GetSrvID()) != 0) {
    LOG(ERROR) << "cannot init shadow db";
    return;
  }

  int ret = shadow.Extract(dir_id, 0, 0, temp, &min_seq, &max_seq);

  if (ret < 0) {
    LOG(ERROR) << "cannot extract from shadow db";
    return;
  }

  if (mdb_->BulkInsert(temp, min_seq, max_seq) != 0) {
    LOG(ERROR) << "cannot insert shadow db into main db";
    return;
  }

  shadow.ExtractClean();
  shadow.Close();
}

void MetadataServer::GenerateFilePath(const TInodeID dir_id,
                                      const std::string& objname,
                                      std::string* file_path,
                                      std::string* dir_path) {
  char fpath[PATH_MAX] = {0};
  sprintf(fpath, "%s/files/%llu/%s.dat",
          options_->GetFileDir().c_str(), static_cast<unsigned long long>(dir_id),
          objname.c_str());
  file_path->assign(fpath);
  if (dir_path != 0) {
    fpath[file_path->size() - objname.size() - 4] = '\0';
    dir_path->assign(fpath);
  }
}

void MetadataServer::OpenFile(OpenResult& _return, const TInodeID dir_id,
                              const std::string& objname, const int16_t mode,
                              const int16_t auth) {
  MeasurementHelper helper(oOpen, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  char data[FILE_THRESHOLD];
  int data_len;
  bool is_embedded;

  if (mdb_->OpenFile(dir_id, index, objname, &is_embedded,
                     &data_len, &(data[0]))==0) {
    _return.is_embedded = is_embedded;
    //TODO: handle different open modes
    if (is_embedded) {
      if ((mode & O_RDONLY) > 0 || (mode & O_RDWR) > 0) {
        _return.data.assign(data, data_len);
      } else {
        //TODO: truncate the file
      }
    } else {
      GenerateFilePath(dir_id, objname, &(_return.data));
    }
    _return.data.clear();
  } else {
     throw FileNotFoundException();
  }
}

void MetadataServer::Read(ReadResult& _return, const TInodeID dir_id,
                          const std::string& objname, const int32_t offset,
                          const int32_t size) {
  MeasurementHelper helper(oRead, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  char data[FILE_THRESHOLD];
  int data_len;
  bool is_embedded;
  if (mdb_->OpenFile(dir_id, index, objname, &is_embedded,
                     &data_len, &(data[0]))==0) {
    _return.is_embedded = is_embedded;
    if (is_embedded) {
      if (offset < data_len) {
        _return.data.assign(data+offset, std::min(data_len-offset, size));
      } else {
        _return.data.clear();
      }
    } else {
      GenerateFilePath(dir_id, objname, &(_return.data));
    }
  } else {
     throw FileNotFoundException();
  }
}

void MetadataServer::Write(WriteResult& _return,
                           const TInodeID dir_id, const std::string& objname,
                           const std::string& data, const int32_t offset) {
  MeasurementHelper helper(oWrite, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  char buf[FILE_THRESHOLD];
  int buf_len;
  bool is_embedded;
  if (mdb_->OpenFile(dir_id, index, objname, &is_embedded,
                    &buf_len, &(buf[0]))==0) {
    _return.is_embedded = is_embedded;
    if (is_embedded) {
      if (offset + data.size() <= FILE_THRESHOLD) {
        mdb_->WriteFile(dir_id, index, objname,
                        offset, data.size(), data.data());
      } else {
        _return.is_embedded = false;
        Status status;
        std::string fpath;
        std::string fdir;
        GenerateFilePath(dir_id, objname, &fpath, &fdir);

        status = env_->CreateDir(fdir);
        /* migrate file to underlying storage */
        if (kNoOverwrite) {
          _return.data.assign(buf, buf_len);
        } else {
          WritableFile *file;
          status = env_->NewWritableFile(fpath, &file);
          if (!status.ok()) throw IOError();
          status = file->Append(Slice(buf, buf_len));
          if (!status.ok()) throw IOError();
          status = file->Close();
          if (!status.ok()) throw IOError();
        }
        mdb_->WriteLink(dir_id, index, objname, fpath);
        _return.link.assign(fpath);
      }
    } else {
      _return.link.assign(buf, buf_len);
    }
  } else {
     throw FileNotFoundException();
  }
}

void MetadataServer::CloseFile(const TInodeID dir_id, const std::string& objname,
                               const int16_t mode) {
  MeasurementHelper helper(oClose, measure_);

  DirHandle hdir = FetchDir(dir_id);
  SanityCheck(hdir.mapping == NULL, FileNotFoundException());

  MutexLock l(&(hdir.dir->partition_mtx));

  int index = 0;
  if ((index = CheckAddressing(hdir.mapping, objname)) < 0) {
     ServerRedirectionException se;
     se.redirect = CopyGigaMap(hdir.mapping);
     throw se;
  }

  //reset attributes
  StatInfo info;
  SanityCheck(mdb_->Getattr(dir_id, index, objname, &info) != 0,
              FileNotFoundException());
  if (!info.is_embedded) {
    std::string fpath;
    GenerateFilePath(dir_id, objname, &fpath);
    env_->GetFileSize(fpath, (uint64_t *) &info.size);
  }
  info.mtime = time(NULL);
  SanityCheck(mdb_->Setattr(dir_id, index, objname, info) != 0, IOError());
}

} // namespace indexfs
