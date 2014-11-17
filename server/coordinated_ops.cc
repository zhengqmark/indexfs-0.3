// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>

#include "common/config.h"
#include "metadata_server.h"

namespace indexfs {

static Mutex inode_lock;

TInodeID MetadataServer::NextDirectoryID() {
  MutexLock l(&inode_lock);
  return mdb_->NewInodeNumber();
}

void MetadataServer::ResolvePath(PathInfo* _return,
                                 const std::string &path,
                                 bool create_if_necessary) {
  if (path.empty()) {
    IllegalPath bad_path;
    bad_path.message = "Empty path";
    throw bad_path;
  }

  if (path.substr(0, 1) != "/") {
    IllegalPath bad_path;
    bad_path.message = "Path must be absolute";
    throw bad_path;
  }

  if (path.size() == 1) {
    _return->depth_ = -1;
    _return->parent_ = -1;
    return;
  }

  if (path.substr(path.size() - 1, 1) == "/") {
    IllegalPath bad_path;
    bad_path.message = "Path ends with a slash";
    throw bad_path;
  }

  int depth = 0;
  TInodeID parent_id = ROOT_DIR_ID;

  std::string entry;
  size_t now = 0, last = 0, end = path.rfind('/');

  while (last < end) {
    now = path.find('/', last + 1);
    if (now - last > 1) {
      depth++;
      entry = path.substr(last + 1, now - last - 1);
      TInodeID dir_id;
      Status s = Lookup(&dir_id, parent_id, entry, create_if_necessary);
      if (s.IsCorruption()) {
        NotADirectory not_a_dir;
        not_a_dir.path = path.substr(0, now);
        throw not_a_dir;
      }
      if (s.IsNotFound()) {
        ParentPathNotFound no_parent;
        no_parent.path = path.substr(0, now);
        throw no_parent;
      }
      DLOG_ASSERT(s.ok());
      parent_id = dir_id;
    }
    last = now;
  }

  _return->depth_ = depth;
  _return->parent_ = parent_id;
  _return->entry_ = path.substr(end + 1);
}

Status MetadataServer::Lookup(TInodeID* _return,
                              TInodeID dir_id, const std::string &entry,
                              bool create_if_necessary) {
  DirHandle dhandle = FetchDir(dir_id);
  if (dhandle.mapping == NULL) {
    return Status::Corruption("No such directory", "" + dir_id);
  }
  MutexLock l(&(dhandle.dir->partition_mtx));
  DLOG_ASSERT(CheckAddressing(dhandle.mapping, entry) == 0);

  Cache::Handle* dent_handle;
  ServerDirEntryValue* value;
  Status s = dent_cache_->GetHandle(dir_id, entry, &dent_handle);

  if (s.ok()) {
    value = reinterpret_cast<ServerDirEntryValue*>(
        dent_cache_->Value(dent_handle));
    *_return = value->inode_id;
  } else {
    StatInfo stat;
    if (mdb_->Getattr(dir_id, 0, entry, &stat) != 0) {
      if (!create_if_necessary) {
        return Status::NotFound("No such file or directory", entry);
      }
      TInodeID id = NextDirectoryID();
      if (mdb_->Mkdir(dir_id, 0, entry, id, 0, 1) != 0) {
        IOError io_error;
        io_error.message = "Cannot create directory";
        throw io_error;
      }
      Directory* dir;
      dir_cache_->Get(id, &dir);
      MutexLock(&(dir->partition_mtx));
      if (mdb_->Mkdir(id, -1, "", id, 0, 1) != 0) {
        dir_cache_->Release(id, dir);
        IOError io_error;
        io_error.message = "Cannot insert directory manifest data";
        throw io_error;
      }
      dir_cache_->Release(id, dir);
      stat.id = id;
      stat.mode = S_IFDIR;
      stat.zeroth_server = 0;
    }
    if (!S_ISDIR(stat.mode)) {
      return Status::Corruption("Not a directory", entry);
    }
    value = new ServerDirEntryValue();
    *_return = value->inode_id = stat.id;
    DLOG_ASSERT(stat.zeroth_server == 0);
    dent_handle = dent_cache_->Insert(dir_id, entry, value);
  }

  dent_cache_->ReleaseHandle(dent_handle);

  return Status::OK();
}

void MetadataServer::IGetattr(StatInfo& _return,
                              const std::string& path) {
  MeasurementHelper helper(oGetattr, measure_);
  if (options_->GetSrvNum() == 1) {
    PathInfo path_info;
    ResolvePath(&path_info, path, false);
    if (path_info.depth_ == -1) {
      _return.id = 0;
      _return.size = 0;
      _return.mode = S_IFDIR;
      return;
    }
    DirHandle dhandle = FetchDir(path_info.parent_);
    DLOG_ASSERT(dhandle.mapping != NULL);
    MutexLock l(&(dhandle.dir->partition_mtx));
    DLOG_ASSERT(CheckAddressing(dhandle.mapping, path_info.entry_) == 0);
    if (mdb_->Getattr(path_info.parent_, 0, path_info.entry_, &_return) != 0) {
      NoSuchFileOrDirectory not_found;
      throw not_found;
    }
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IMknod(const std::string& path,
                            const int16_t permission) {
  MeasurementHelper helper(oMknod, measure_);
  if (options_->GetSrvNum() == 1) {
    PathInfo path_info;
    ResolvePath(&path_info, path, false);
    if (path_info.depth_ == -1) {
      IOError io_error;
      io_error.message = "Cannot re-create root";
      throw io_error;
    }
    DirHandle dhandle = FetchDir(path_info.parent_);
    DLOG_ASSERT(dhandle.mapping != NULL);
    MutexLock l(&(dhandle.dir->partition_mtx));
    DLOG_ASSERT(CheckAddressing(dhandle.mapping, path_info.entry_) == 0);
    if (mdb_->Create(path_info.parent_, 0, path_info.entry_, "") != 0) {
      FileAlreadyExists file_exists;
      throw file_exists;
    }
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IMkdir(const std::string& path,
                            const int16_t permission) {
  MeasurementHelper helper(oMkdir, measure_);
  if (options_->GetSrvNum() == 1) {
    PathInfo path_info;
    ResolvePath(&path_info, path, false);
    if (path_info.depth_ == -1) {
      IOError io_error;
      io_error.message = "Cannot re-create root";
      throw io_error;
    }
    DirHandle dhandle = FetchDir(path_info.parent_);
    DLOG_ASSERT(dhandle.mapping != NULL);
    MutexLock l(&(dhandle.dir->partition_mtx));
    TInodeID id = NextDirectoryID();
    DLOG_ASSERT(CheckAddressing(dhandle.mapping, path_info.entry_) == 0);
    if (mdb_->Mkdir(path_info.parent_, 0, path_info.entry_, id, 0, 1) != 0) {
      FileAlreadyExists file_exists;
      throw file_exists;
    }
    Directory* dir;
    dir_cache_->Get(id, &dir);
    MutexLock(&(dir->partition_mtx));
    if (mdb_->Mkdir(id, -1, "", id, 0, 1) != 0) {
      dir_cache_->Release(id, dir);
      IOError io_error;
      io_error.message = "Cannot insert directory manifest data";
      throw io_error;
    }
    dir_cache_->Release(id, dir);
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IChmod(const std::string& path,
                            const int16_t permission) {
  MeasurementHelper helper(oChmod, measure_);
  if (options_->GetSrvNum() == 1) {
    PathInfo path_info;
    ResolvePath(&path_info, path, false);
    if (path_info.depth_ == -1) {
      IOError io_error;
      io_error.message = "Cannot update root";
      throw io_error;
    }
    DirHandle dhandle = FetchDir(path_info.parent_);
    DLOG_ASSERT(dhandle.mapping != NULL);
    MutexLock l(&(dhandle.dir->partition_mtx));
    DLOG_ASSERT(CheckAddressing(dhandle.mapping, path_info.entry_) == 0);
    StatInfo stat;
    if (mdb_->Getattr(path_info.parent_, 0, path_info.entry_, &stat) != 0) {
      NoSuchFileOrDirectory not_found;
      throw not_found;
    }
    if (mdb_->Chmod(path_info.parent_, 0, path_info.entry_, permission) != 0) {
      NoSuchFileOrDirectory not_found;
      throw not_found;
    }
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IChfmod(const std::string& path,
                            const int16_t permission) {
  MeasurementHelper helper(oChmod, measure_);
  if (options_->GetSrvNum() == 1) {
    PathInfo path_info;
    ResolvePath(&path_info, path, false);
    if (path_info.depth_ == -1) {
      IOError io_error;
      io_error.message = "Cannot update root";
      throw io_error;
    }
    DirHandle dhandle = FetchDir(path_info.parent_);
    DLOG_ASSERT(dhandle.mapping != NULL);
    MutexLock l(&(dhandle.dir->partition_mtx));
    DLOG_ASSERT(CheckAddressing(dhandle.mapping, path_info.entry_) == 0);
    StatInfo stat;
    if (mdb_->Getattr(path_info.parent_, 0, path_info.entry_, &stat) != 0) {
      NoSuchFileOrDirectory not_found;
      throw not_found;
    }
    if (S_ISDIR(stat.mode)) {
      NotAFile not_a_file;
      throw not_a_file;
    }
    if (mdb_->Chmod(path_info.parent_, 0, path_info.entry_, permission) != 0) {
      NoSuchFileOrDirectory not_found;
      throw not_found;
    }
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IRemove(const std::string& path) {
  if (options_->GetSrvNum() == 1) {
    ServerInternalError srv_error;
    srv_error.message = "Not implemented";
    throw srv_error;
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

void MetadataServer::IRename(const std::string& src_path,
                             const std::string& dst_path) {
  if (options_->GetSrvNum() == 1) {
    ServerInternalError srv_error;
    srv_error.message = "Not implemented";
    throw srv_error;
  }
  LOG_ASSERT(options_->GetSrvNum() == 1);
}

} /* namespace indexfs */
