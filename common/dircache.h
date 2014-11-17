// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_DIRECTORY_CACHE_H_
#define _INDEXFS_DIRECTORY_CACHE_H_

#include <map>
#include "common.h"

namespace indexfs {

struct Directory {
  int partition_size;
  short refcount;
  short split_flag;

  Mutex partition_mtx;
  CondVar partition_cv;

  Directory() : partition_size(0), refcount(1), split_flag(0),
                partition_cv(&partition_mtx) {
  }
};

class DirCache {
 public:

  DirCache(int entries);

  ~DirCache();

  void Get(const TINumber dir_id,
           Directory* *directory);

  void Release(const TINumber dir_id,
               Directory* directory);

  void Evict(const TINumber dir_id);

 private:

  Mutex *mutexs_;
  std::map<TINumber, Directory*> *dirs_;
  int capacity_;
};

} // namespace indexfs

#endif
