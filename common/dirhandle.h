// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DIRHANDLE_H_
#define DIRHANDLE_H_

#include "common/dmapcache.h"
#include "common/dircache.h"

namespace indexfs {

class DirHandle {
 public:
  DirHandle() : mapping(NULL), dir(NULL), handle_(NULL) {
  }

  DirHandle(Directory* newdir, Cache::Handle* newhandle)
    : dir(newdir), handle_(newhandle) {
    mapping = (handle_ != NULL) ? dmap_cache_->Value(handle_) : NULL;
  }

  ~DirHandle() {
    if (dir != NULL && mapping != NULL) {
      TInodeID dir_id = mapping->id;
      dmap_cache_->Release(handle_);
      dir_cache_->Release(dir_id, dir);
    }
  }

  void Setup(Directory* newdir, Cache::Handle* newhandle) {
    if (newdir != NULL && newhandle != NULL) {
      dir = newdir;
      handle_ = newhandle;
      mapping = dmap_cache_->Value(handle_);
    } else {
      dir = NULL;
      handle_ = NULL;
      mapping = NULL;
    }
  }

  giga_mapping_t* mapping;
  Directory* dir;
  Cache::Handle* handle_;

  static DirMappingCache* dmap_cache_;
  static DirCache* dir_cache_;
};

} // namespace indexfs

#endif /* DIRHANDLE_H_ */
