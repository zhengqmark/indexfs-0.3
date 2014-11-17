// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_DIRECTORY_ENTRY_CACHE_H_
#define _INDEXFS_DIRECTORY_ENTRY_CACHE_H_

#include "common.h"
#include "counter.h"
#include "include/leveldb/util/coding.h"

namespace indexfs {

struct DirEntryValue {
  TINumber inode_id;
  int zeroth_server;
  uint64_t expire_time;
};

enum LeaseStatus {
  LEASE_READ_STATUS,
  LEASE_WRITE_STATUS,
};

struct ServerDirEntryValue {
  ServerDirEntryValue() : inode_id(0), zeroth_server(0),
              expire_time(0), status(LEASE_READ_STATUS),
              write_rate(100000), read_rate(1000){}

  TINumber inode_id;
  int zeroth_server;
  uint64_t expire_time;
  LeaseStatus status;
  RateCounter write_rate;
  RateCounter read_rate;
};

template <class TEntry>
static void DeleteEntry(const Slice& key, void* value) {
  TEntry* dv = reinterpret_cast<TEntry*>(value);
  if (dv != NULL)
    delete dv;
}

template <class TEntry>
class DirEntryCache {
 private:
  Cache* cache_;

 public:

   DirEntryCache(int entries) : cache_(leveldb::NewLRUCache(entries)) {
   }

   ~DirEntryCache() {
     if (cache_ != NULL)
       delete cache_;
   }

  Status Get(const TINumber dir_id, const std::string &objname, TEntry* value) {
    Status s;
    Cache::Handle* handle = NULL;
    std::string key = objname;
    leveldb::PutFixed64(&key, dir_id);
    handle = cache_->Lookup(key);
    if (handle != NULL) {
      TEntry* v = reinterpret_cast<TEntry*>(cache_->Value(handle));
      *value = *v;
      cache_->Release(handle);
    } else {
      s = Status::NotFound("Dir entry is not in the cache");
    }
    return s;
  }

  Status GetHandle(const TINumber dir_id, const std::string &objname,
                   Cache::Handle** value) {
    Status s;
    std::string key = objname;
    leveldb::PutFixed64(&key, dir_id);
    *value = cache_->Lookup(key);
    if (*value == NULL) {
      s = Status::NotFound("Dir entry is not in the cache");
    }
    return s;
  }

  void ReleaseHandle(Cache::Handle* handle) {
    if (handle != NULL) {
      cache_->Release(handle);
    }
  }

  void* Value(Cache::Handle* handle) {
    if (handle != NULL) {
      return cache_->Value(handle);
    }
    return NULL;
  }

  Status Put(const TINumber dir_id, const std::string &objname,
             const TEntry& value) {
    Status s;
    Cache::Handle* handle = NULL;
    std::string key = objname;
    leveldb::PutFixed64(&key, dir_id);
    TEntry* copy = new TEntry(value);
    handle = cache_->Insert(key, copy, 1, &DeleteEntry<TEntry>);
    if (handle != NULL)
      cache_->Release(handle);
    return s;
  }

  Cache::Handle* Insert(const TINumber dir_id, const std::string &objname,
                        TEntry* value) {
    Status s;
    std::string key = objname;
    leveldb::PutFixed64(&key, dir_id);
    return cache_->Insert(key, value, 1, &DeleteEntry<TEntry>);
  }

  void Evict(const TINumber dir_id, const std::string &objname) {
    Status s;
    std::string key = objname;
    leveldb::PutFixed64(&key, dir_id);
    cache_->Erase(key);
  }
};

} // namespace indexfs

#endif
