// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "dircache.h"

namespace indexfs {

static const int kNumShards = 16;

DirCache::DirCache(int entries)
  : capacity_(entries) {
    mutexs_ = new Mutex[kNumShards];
    dirs_ = new std::map<TINumber, Directory*>[kNumShards];
    (void) capacity_;
}

DirCache::~DirCache() {
  std::map<TINumber, Directory*>::iterator it;
  for (int i = 0; i < kNumShards; ++i) {
    for (it = dirs_[i].begin(); it != dirs_[i].end(); it++)
      delete it->second;
  }
  delete [] dirs_;
  delete [] mutexs_;
}

void DirCache::Get(const TINumber dir_id,
                     Directory* *directory) {
  int shard = dir_id & (kNumShards - 1);
  MutexLock l(&mutexs_[shard]);
  std::map<TINumber, Directory*>::iterator it;
  it = dirs_[shard].find(dir_id);

  if (it != dirs_[shard].end()) {
    *directory = it->second;
  } else {
    *directory = new Directory();
    dirs_[shard].insert(
        std::pair<TINumber, Directory*>(dir_id, *directory));
  }
  (*directory)->refcount++;
}

void DirCache::Release(const TINumber dir_id,
                       Directory* directory) {
  int shard = dir_id & (kNumShards - 1);
  MutexLock l(&mutexs_[shard]);
  directory->refcount--;
  if (directory->refcount == 0) {
    dirs_[shard].erase(dir_id);
    delete directory;
  }
}

void DirCache::Evict(const TINumber dir_id) {
  int shard = dir_id & (kNumShards - 1);
  MutexLock l(&mutexs_[shard]);
  std::map<TINumber, Directory*>::iterator it = dirs_[shard].find(dir_id);
  it->second->refcount--;
  if (it->second->refcount == 0) {
    delete it->second;
    dirs_[shard].erase(it);
  }
}

} // namespace indexfs
