// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>
#include <fcntl.h>

#include "util/str_hash.h"
#include "metadata_client.h"

namespace indexfs {

// The max number of redirections the client will follow if the
// servers keep sending new bitmaps.
//
static const size_t kNumRedirect = 10;
static const int kNumInstrumentPoints = 14;
static const unsigned int kMaxNumScanEntries = 256;
static const char* kMetadataClientOpsName[kNumInstrumentPoints] = {
    "getattr", "mknod", "mkdir", "createentry", "chmod", "remove",
    "rename", "readdir", "readbitmap", "open",
    "read", "write", "close", "lookup"};

static
std::string ToString(std::vector<int> &v) {
  std::stringstream ss;
  ss << "[";
  for (size_t i = 0; i < v.size(); i++) {
    ss << v[i] << ", ";
  }
  ss << "...]";
  return ss.str();
}

template <class T>
inline void SanityClean(T* ptr) {
  if (ptr != NULL) delete ptr;
}

struct FileDescriptor {
  int parent_dir_id;
  int zth_server;
  int16_t mode;
  std::string objname;
  WritableFile* wf;
  RandomAccessFile* rf;

  FileDescriptor(int id, int srv, int16_t mo, const std::string & name) :
    parent_dir_id(id), zth_server(srv), mode(mo), objname(name), wf(0), rf(0) {}

  FileDescriptor() : parent_dir_id(0), zth_server(0), mode(0), wf(0), rf(0) {}
};

MetadataClient::MetadataClient(Config* conf)
  : cfg_(conf)
  , dir_cache_(new DirCache(conf->GetDirCacheSize()))
  , dent_cache_(new DirEntryCache<DirEntryValue>(conf->GetDirEntryCacheSize()))
  , dmap_cache_(new DirMappingCache(conf->GetDirMappingCacheSize()))
  , rpc_(RPC::CreateRPC(conf)), fd_count_(0) {
  DirHandle::dmap_cache_ = dmap_cache_;
  DirHandle::dir_cache_ = dir_cache_;
#if defined(OS_LINUX) && defined(HDFS)
  int hdfs_port = cfg_->GetHDFSPort();
  const char* hdfs_ip = cfg_->GetHDFSIP();
  env_ = hdfs_ip == NULL ? Env::Default() : Env::HDFSEnv(hdfs_ip, hdfs_port);
#else
  env_ = Env::Default();
#endif
  memset(fd_, 0, sizeof(fd_));

  std::vector<std::string> points;
  for (int i = 0; i < kNumInstrumentPoints; ++i)
    points.push_back(std::string(kMetadataClientOpsName[i]));
  measure_ = new Measurement(points, 0, 0);
}

MetadataClient::~MetadataClient() {
  delete rpc_;
  delete measure_;

  SanityClean(dent_cache_);
  SanityClean(dmap_cache_);
  SanityClean(dir_cache_);

  DirHandle::dmap_cache_ = NULL;
  DirHandle::dir_cache_ = NULL;
}

void MetadataClient::PrintMeasurements(FILE* output) {
  measure_->Print(output);
}

int MetadataClient::LeaseTime(int depth) {
  int limit = 6400000 / (depth + 1);
  if (limit < 100000) limit = 100000;
  //int limit = 1000000;
  return limit;
}

bool MetadataClient::IsEntryExpired
  (DirEntryValue &value, int depth) {
  return env_->NowMicros() > value.expire_time;
}

int MetadataClient::SelectServer
  (DirHandle &handle, Path &entry) {
  index_t index =
      giga_get_index_for_file(handle.mapping, entry.c_str());
  int server =
      giga_get_server_for_index(handle.mapping, index);

  DLOG_ASSERT(server >= 0);
  DLOG_ASSERT(server < cfg_->GetSrvNum());
  DLOG(INFO) << "Routing entry " << entry << " to server " << server;

  return server;
}

void MetadataClient::UpdateBitmap
  (DirHandle &dirhandle, GigaBitmap &bitmap) {
  giga_mapping_t mapping = ToLegacyMapping(bitmap);
  MutexLock l(&(dirhandle.dir->partition_mtx));
  giga_update_cache(dirhandle.mapping, &mapping);
}

Status MetadataClient::AddCacheEntry
  (TINumber parent, Path &dir, DirEntryValue* value) {
  return dent_cache_->Put(parent, dir, (*value));
}

Status MetadataClient::GetCacheEntry
  (TINumber parent, Path &dir, DirEntryValue* value) {
  return dent_cache_->Get(parent, dir, value);
}

// Create a handle for the specified directory, which is identified by its
// unique inode number. The handle returned will contain the corresponding
// directory control block and a bitmap mapping directory partitions to their
// servers.
//
// Usually, the bitmap can be easily retrieved from the local cache. Otherwise,
// it will have to be fetched directly from the directory's home server, which is,
// of course, the special zeroth server of the directory in question.
//
// Should we fail to get the bitmap from the home server, an empty handle object
// is returned as a mark of failure. On the other hand, when the bitmap is
// conveniently satisfied by our local cache, it will be subject to staleness.
// So use the bitmap returned with caution. It may bite you.
//
DirHandle MetadataClient::FetchDir
  (TINumber dir_id, int zeroth_server) {
  DLOG_ASSERT(zeroth_server >= 0);
  DLOG_ASSERT(zeroth_server < cfg_->GetSrvNum());

  Directory* dir;
  dir_cache_->Get(dir_id, &dir);
  Cache::Handle* handle = dmap_cache_->Get(dir_id);
  if (handle == NULL) {
    MeasurementHelper helper(oReadBitmap, measure_);

    MutexLock l(&dir->partition_mtx);
    handle = dmap_cache_->Get(dir_id);
    if (handle == NULL) {
      try {
        GigaBitmap mapping;
        rpc_->GetClient(zeroth_server)->ReadBitmap(mapping, dir_id);
        handle = dmap_cache_->Put(dir_id, ToLegacyMapping(mapping));
      }
      catch (FileNotFoundException &tx) {
        LOG(ERROR) << "Fail to fetch bitmap from server "
          << zeroth_server << " for directory under ID: " << dir_id;
        return DirHandle(); // which indicates an error to the caller
      }
    }
  } /* end double-checking */
  return DirHandle(dir, handle);
}

Status MetadataClient::ResolvePath(Path &path, TINumber* parent,
                  int* zeroth_server, std::string* entry, int* depth) {
  if (path.empty())
    return Status::InvalidArgument("Empty path");

  if (path.substr(0,1) != "/")
    return Status::InvalidArgument("Invalid path", path);

  if (path.size() == 1) {
    *parent = ROOT_DIR_ID;
    *zeroth_server = 0;
    *entry = "/";
    if (depth != NULL) *depth = 0;
    return Status::OK();
  }

  if (path.substr(path.size() -1 , 1) == "/") {
    std::string new_path = path.substr(0, path.size() - 1);
    return ResolvePath(new_path, parent, zeroth_server, entry, depth);
  }

  return Internal_ResolvePath(path, parent, zeroth_server, entry, depth);
}

Status MetadataClient::Internal_ResolvePath(Path &path, TINumber* parent,
                 int* zeroth_server, std::string* entry, int* path_depth) {
  int depth = 0;
  int pdir_id = 0;
  int pzeroth_server = 0;

  std::string name;
  size_t now = 0, last = 0, end = path.rfind("/");

  while (last < end) {
    now = path.find("/", last + 1);
    if (now - last > 1) {
      depth++;
      name = path.substr(last + 1, now - last - 1);
      DirEntryValue value;
      Status s = GetCacheEntry(pdir_id, name, &value);
      if (!s.ok() || IsEntryExpired(value, depth)) {
        AccessInfo info;
        s = Lookup(pzeroth_server, pdir_id, name, &info, LeaseTime(depth));
        if (!s.ok()) return s;
        value.inode_id = info.id;
        value.zeroth_server = info.zeroth_server;
        value.expire_time = info.lease_time;
        AddCacheEntry(pdir_id, name, &value);
      }
      pdir_id = value.inode_id;
      pzeroth_server = value.zeroth_server;
    }
    last = now;
  }

  *parent = pdir_id;
  *zeroth_server = pzeroth_server;
  *entry = path.substr(end + 1);
  if (path_depth != NULL) *path_depth = depth;
  return Status::OK();
}

Status MetadataClient::Lookup(int zeroth_server, TINumber parent,
                             Path &entry, AccessInfo* info, int lease_time) {
  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  MeasurementHelper helper(oLookup, measure_);

  size_t num_retries = 0;
  while (num_retries < kNumRedirect) {
    int server = SelectServer(handle, entry);
    ++num_retries;
    try {
      rpc_->GetClient(server)->Access((*info), parent, entry, lease_time);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      continue; // Retry again!
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("No such file or diectory");
    } catch (NotDirectoryException &dx) {
      return Status::IOError("Not a directory");
    }
    return Status::OK();
  }
  return Status::Corruption("Too many redirections");
}

Status MetadataClient::Getattr
  (Path &path, StatInfo *info) {
  if (path == "/") {
    info->uid = info->gid = 0;
    info->mtime = info->ctime = 0;
    info->mode = S_IFDIR | S_IRWXO | S_IRWXG | S_IXOTH | S_IROTH;
    return Status::OK();
  }

  TINumber parent;
  int zeroth_server;
  int depth;

  std::string entry;
  Status s = ResolvePath(path, &parent, &zeroth_server, &entry, &depth);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  s = RPC_Getattr(parent, entry, info, handle, LeaseTime(depth));
  if (s.IsCorruption()) {
    LOG(ERROR) << "Error[getattr]: (" << path << ")" << s.ToString();
  }
  return s;
}

Status MetadataClient::AccessDir(Path &path) {
  TINumber parent;
  int zeroth_server;
  int depth;

  std::string entry;
  Status s = ResolvePath(path+"/a", &parent, &zeroth_server, &entry, &depth);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::NotFound("No Such Entry");
  return Status::OK();
}


Status MetadataClient::RPC_Getattr(TINumber parent, Path &entry, StatInfo* info,
                                   DirHandle &handle, int lease_time) {
  MeasurementHelper helper(oGetattr, measure_);

  std::vector<int> srvs;
  while (srvs.size() < kNumRedirect) {
    int server = SelectServer(handle, entry);
    srvs.push_back(server);
    try {
      rpc_->GetClient(server)->Getattr((*info), parent, entry, lease_time);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      continue; // Retry again!
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("No Such Entry");
    }
    return Status::OK();
  }
  return Status::Corruption("Too Many Redirection");
}

Status MetadataClient::Mknod
  (Path &path, int16_t permission) {
  TINumber parent;
  int zeroth_server;

  std::string entry;
  Status s = ResolvePath(path, &parent, &zeroth_server, &entry);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  s = RPC_Mknod(parent, entry, permission, handle);

  if (s.IsCorruption()) {
    LOG(ERROR) << "Error[mknod]: (" << path << ")" << s.ToString();
  }
  return s;
}

Status MetadataClient::RPC_Mknod
  (TINumber parent, Path &entry, int16_t permission, DirHandle &handle) {
  MeasurementHelper helper(oMknod, measure_);

  std::vector<int> srvs;
  while (srvs.size() < kNumRedirect) {
    int server = SelectServer(handle, entry);
    srvs.push_back(server);
    try {
      rpc_->GetClient(server)->Mknod(parent, entry, permission);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      continue; // Retry again!
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("No Such Entry");
    } catch (FileAlreadyExistException &ex) {
      return Status::IOError("File Already Exists");
    }
    return Status::OK();
  }
  return Status::Corruption("Too Many Redirection");
}

Status MetadataClient::Mkdir
  (Path &path, int16_t permission) {
  TINumber parent;
  int zeroth_server;

  std::string entry;
  Status s = ResolvePath(path, &parent, &zeroth_server, &entry);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  int16_t hint_server = GetStrHash(path.data(), path.size(), 0) %
                        cfg_->GetSrvNum();
  return RPC_Mkdir(parent, entry, permission, hint_server, handle);
}

Status MetadataClient::RPC_Mkdir
  (TINumber parent, Path &entry, int16_t permission,
   int16_t hint_server, DirHandle &handle) {
  MeasurementHelper helper(oMkdir, measure_);

  std::vector<int> srvs;
  while (srvs.size() < kNumRedirect) {
    int server = SelectServer(handle, entry);
    srvs.push_back(server);
    try {
      rpc_->GetClient(server)->Mkdir(parent, entry, permission, hint_server);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      continue; //Retry again!
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("No Such Entry");
    } catch (FileAlreadyExistException &ex) {
      return Status::IOError("Dir Already Exists");
    }
    return Status::OK();
  }
  LOG(ERROR) << "fail to perform mkdir, too many redirections: "
             << ToString(srvs);
  return Status::Corruption("Too Many Redirection");
}

Status MetadataClient::Chmod
  (Path &path, int16_t permission) {
  TINumber parent;
  int zeroth_server;

  std::string entry;
  Status s = ResolvePath(path, &parent, &zeroth_server, &entry);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  return RPC_Chmod(parent, entry, permission, handle);
}

Status MetadataClient::RPC_Chmod
  (TINumber parent, Path &entry, int16_t permission, DirHandle &handle) {
  MeasurementHelper helper(oChmod, measure_);

  int server;
  server = SelectServer(handle, entry);
  try {
    rpc_->GetClient(server)->Chmod(parent, entry, permission);
  } catch (ServerRedirectionException &sx) {
    UpdateBitmap(handle, sx.redirect);
    return RPC_Chmod(parent, entry, permission, handle);
  } catch (FileNotFoundException &fx) {
    return Status::NotFound("Cannot find the entry");
  }
  return Status::OK();
}

Status MetadataClient::Remove
  (Path &path) {
  TINumber parent;
  int zeroth_server;

  std::string entry;
  Status s = ResolvePath(path, &parent, &zeroth_server, &entry);
  if (s.IsNotFound()) return Status::OK();
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zeroth_server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  return RPC_Remove(parent, entry, handle);
}

Status MetadataClient::RPC_Remove
  (TINumber parent, Path &entry, DirHandle &handle) {
  MeasurementHelper helper(oRemove, measure_);

  int server;
  server = SelectServer(handle, entry);
  try {
    rpc_->GetClient(server)->Remove(parent, entry);
  } catch (ServerRedirectionException &sx) {
    UpdateBitmap(handle, sx.redirect);
    return RPC_Remove(parent, entry, handle);
  } catch (FileNotFoundException &fx) {
    return Status::NotFound("Cannot find the entry");
  }
  return Status::OK();
}

Status MetadataClient::RPC_Create(TINumber parent, Path &entry, int server,
                                  const StatInfo &info, const std::string &link,
                                  const std::string &data, DirHandle &handle) {
  MeasurementHelper helper(oCreateEntry, measure_);

  while (true) {
    try {
      rpc_->GetClient(server)->CreateEntry(parent, entry, info, link, data);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      server = SelectServer(handle, entry);
      continue;
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("No Such Entry");
    } catch (FileAlreadyExistException &ex) {
      return Status::IOError("Entry Already Exists");
    }
    break;
  }
  return Status::OK();
}

Status MetadataClient::Rename(Path &src, Path &dst) {
  //Not fault tolerant rename
  TINumber src_parent;
  int src_server;
  std::string src_entry;
  Status s = ResolvePath(src, &src_parent, &src_server, &src_entry);
  if (!s.ok()) return s;
  DirHandle src_handle = FetchDir(src_parent, src_server);
  if (src_handle.dir == NULL || src_handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  TINumber dst_parent;
  int dst_server, depth;
  std::string dst_entry;
  s = ResolvePath(dst, &dst_parent, &dst_server, &dst_entry, &depth);
  if (!s.ok()) return s;
  DirHandle dst_handle = FetchDir(dst_parent, dst_server);
  if (dst_handle.dir == NULL || dst_handle.mapping == NULL)
    return Status::Corruption("Fail to fetch dir handle");

  StatInfo info;
  s = RPC_Getattr(src_parent, src_entry, &info, src_handle, LeaseTime(depth));
  if (!s.ok()) return s;
  s = RPC_Create(dst_parent, dst_entry, dst_server, info, "", "", dst_handle);
  if (!s.ok()) return s;
  return RPC_Remove(src_parent, src_entry, src_handle);
}

static uint8_t reverse_bits(unsigned int b, unsigned int n) {
  return ((b * 0x80200802ULL) & 0x0884422110ULL) * 0x0101010101ULL >> (32+sizeof(uint8_t)-n);
}

Status MetadataClient::Readdir(Path &path, std::vector<std::string>* result) {
  TINumber dir_id;
  int server;
  int depth;
  std::string entry;

  Status s = ResolvePath(path+"/test", &dir_id, &server, &entry, &depth);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(dir_id, server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::IOError("Not a directory");

  uint8_t curr_idx = 0;
  unsigned int curr_radix = handle.mapping->curr_radix;
  std::string start_key;
  start_key.clear();
  while (curr_idx < (1 << curr_radix)) {
    uint8_t curr_partition = reverse_bits(curr_idx, curr_radix);
    if (get_bit_status(handle.mapping->bitmap, curr_partition) > 0) {
      server = giga_get_server_for_index(handle.mapping, curr_partition);
      ScanResult scan_result;
      do {
        try {
          rpc_->GetClient(server)->Readdir(scan_result, dir_id,
                                           curr_partition,
                                           start_key, kMaxNumScanEntries);
        } catch (ServerRedirectionException &sx) {
          break;
        } catch (FileNotFoundException &fx) {
          break;
        } catch (NotDirectoryException &dx) {
          return Status::IOError("Not a directory");
        }
        UpdateBitmap(handle, scan_result.mapping);
        start_key = scan_result.end_key;
        result->insert(result->end(),
                       scan_result.entries.begin(), scan_result.entries.end());
      } while (scan_result.more_entries > 0);
    }
    curr_radix = handle.mapping->curr_radix;
    curr_idx ++;
  }

  return Status::OK();
}

Status MetadataClient::ReaddirPlus(Path &path,
                                   std::vector<std::string>* names,
                                   std::vector<StatInfo>* entries) {
  TINumber dir_id;
  int server;
  int depth;
  std::string entry;

  Status s = ResolvePath(path+"/test", &dir_id, &server, &entry, &depth);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(dir_id, server);
  if (handle.dir == NULL || handle.mapping == NULL)
    return Status::IOError("Not a directory");

  uint8_t curr_idx = 0;
  unsigned int curr_radix = handle.mapping->curr_radix;
  std::string start_key;
  start_key.clear();
  while (curr_idx < (1 << curr_radix)) {
    uint8_t curr_partition = reverse_bits(curr_idx, curr_radix);
    if (get_bit_status(handle.mapping->bitmap, curr_partition) > 0) {
      server = giga_get_server_for_index(handle.mapping, curr_partition);
      ScanPlusResult scan_result;
      do {
        try {
          rpc_->GetClient(server)->ReaddirPlus(scan_result, dir_id,
                                               curr_partition, start_key,
                                               kMaxNumScanEntries);
        } catch (ServerRedirectionException &sx) {
          break;
        } catch (FileNotFoundException &fx) {
          break;
        } catch (NotDirectoryException &dx) {
          return Status::IOError("Not a directory");
        }
        UpdateBitmap(handle, scan_result.mapping);
        start_key = scan_result.end_key;
        names->insert(names->end(),
                      scan_result.names.begin(), scan_result.names.end());
        entries->insert(entries->end(),
                        scan_result.entries.begin(), scan_result.entries.end());
      } while (scan_result.more_entries > 0);
    }
    curr_radix = handle.mapping->curr_radix;
    curr_idx ++;
  }

  return Status::OK();
}


Status MetadataClient::RPC_Open(int parent, Path &entry,
                                int mode, DirHandle &handle,
                                OpenResult &ret) {
  MeasurementHelper helper(oOpen, measure_);

  int server = SelectServer(handle, entry);
  while (true) {
    try {
      rpc_->GetClient(server)->OpenFile(ret, parent, entry, mode, 0);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      server = SelectServer(handle, entry);
      continue;
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("Cannot find the entry");
    } catch (IOError &iox) {
      return Status::IOError("Has an IO error");
    }
    break;
  }
  return Status::OK();
}

int MetadataClient::AllocateFD() {
  //TODO: Find spare fd
  for (int i = 0; i < MAX_NUM_FILEDESCRIPTORS; ++i)
    if (fd_[i] == 0)
      return i;
  return -1;
}

Status MetadataClient::Open(Path &path, int16_t mode, int *fd) {
  TINumber parent;
  int zth_server;
  std::string entry;
  Status s = ResolvePath(path, &parent, &zth_server, &entry);
  if (!s.ok()) return s;

  DirHandle handle = FetchDir(parent, zth_server);
  OpenResult ret;
  s = RPC_Open(parent, entry, mode, handle, ret);
  if (s.ok()) {
    *fd = AllocateFD();
    fd_[*fd] = new FileDescriptor(parent, zth_server, mode, entry);
    if (!ret.is_embedded) {
      if ((mode & O_RDONLY) > 0) {
        s = env_->NewRandomAccessFile(ret.data, &fd_[*fd]->rf);
      }
      if ((mode & O_WRONLY) > 0) {
        s = env_->NewWritableFile(ret.data, &fd_[*fd]->wf);
      }
    }
  }
  return s;
}

Status MetadataClient::Read(int fd, size_t offset, size_t size, char *buf,
                            int *ret_size) {
  if (fd_[fd] == 0) return Status::IOError("No such file descriptor");
  if (fd_[fd]->rf == 0) {
    MeasurementHelper helper(oRead, measure_);

    DirHandle handle = FetchDir(fd_[fd]->parent_dir_id,
                              fd_[fd]->zth_server);
    int server = SelectServer(handle, fd_[fd]->objname);
    ReadResult ret;
    while (true) {
      try {
        rpc_->GetClient(server)->Read(ret, fd_[fd]->parent_dir_id,
                                      fd_[fd]->objname, offset, size);
      } catch (ServerRedirectionException &sx) {
        UpdateBitmap(handle, sx.redirect);
        server = SelectServer(handle, fd_[fd]->objname);
      continue;
      } catch (FileNotFoundException &fx) {
        return Status::NotFound("Cannot find the entry");
      } catch (IOError &iox) {
        return Status::IOError("Has an IO error");
      }
      break;
    }
    if (ret.is_embedded) {
      *ret_size = ret.data.size();
      memcpy(buf, ret.data.data(), *ret_size);
      return Status::OK();
    } else {
      env_->NewRandomAccessFile(ret.data, &fd_[fd]->rf);
    }
  }
  Slice result;
  Status s = fd_[fd]->rf->Read(offset, size, &result, buf);
  *ret_size = result.size();
  return s;
}

Status MetadataClient::Write(int fd, size_t offset, size_t size,
                             const char *buf) {
  if (fd_[fd] == 0) return Status::IOError("No such file descriptor");
  if (fd_[fd]->wf == 0) {
    MeasurementHelper helper(oWrite, measure_);

    DirHandle handle = FetchDir(fd_[fd]->parent_dir_id,
                                fd_[fd]->zth_server);
    int server = SelectServer(handle, fd_[fd]->objname);
    WriteResult ret;
    while (true) {
      try {
        rpc_->GetClient(server)->Write(ret, fd_[fd]->parent_dir_id,
                      fd_[fd]->objname, std::string(buf, size), offset);
      } catch (ServerRedirectionException &sx) {
        UpdateBitmap(handle, sx.redirect);
        server = SelectServer(handle, fd_[fd]->objname);
      continue;
      } catch (FileNotFoundException &fx) {
        return Status::NotFound("Cannot find the entry");
      } catch (IOError &iox) {
        return Status::IOError("Has an IO error");
      }
      break;
    }
    if (!ret.is_embedded) {
      Status s = env_->NewWritableFile(ret.link, &(fd_[fd]->wf));
      if (!s.ok()) return s;
      fd_[fd]->wf->Append(ret.data);
    } else return Status::OK();
  }
  Status s = fd_[fd]->wf->Append(Slice(buf, size));
  return s;
}

Status MetadataClient::Close(int fd) {
  if (fd_[fd] == 0) return Status::IOError("No such file descriptor");
  if (fd_[fd]->wf != 0) {
    fd_[fd]->wf->Close();
    delete fd_[fd]->wf;
  }
  if (fd_[fd]->rf != 0) {
    delete fd_[fd]->rf;
  }
  MeasurementHelper helper(oClose, measure_);

  DirHandle handle = FetchDir(fd_[fd]->parent_dir_id,
                              fd_[fd]->zth_server);
  int server = SelectServer(handle, fd_[fd]->objname);
  while (true) {
    try {
      rpc_->GetClient(server)->CloseFile(fd_[fd]->parent_dir_id,
                                         fd_[fd]->objname, fd_[fd]->mode);
    } catch (ServerRedirectionException &sx) {
      UpdateBitmap(handle, sx.redirect);
      server = SelectServer(handle, fd_[fd]->objname);
      continue;
    } catch (FileNotFoundException &fx) {
      return Status::NotFound("Cannot find the entry");
    } catch (IOError &iox) {
      return Status::IOError("Has an IO error");
    }
    break;
  }
  delete fd_[fd];
  fd_[fd] = 0;
  return Status::OK();
}

void MetadataClient::Noop() {
  rpc_->GetClient(0)->InitRPC(); // Assuming single server
}

} /* namespace indexfs */
