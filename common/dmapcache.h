// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_DIRECTORY_MAP_CACHE_H_
#define _INDEXFS_DIRECTORY_MAP_CACHE_H_

#include "common.h"
extern "C" {
  #include "giga_index.h"
}

namespace indexfs {

class DirMappingCache {

 public:

  DirMappingCache(int entries);

  ~DirMappingCache();

  Cache::Handle* Get(const TINumber dir_id);

  Cache::Handle* Put(const TINumber dir_id,
                     const giga_mapping_t &mapping);

  void Insert(const TINumber dir_id,
              const giga_mapping_t &mapping);

  giga_mapping_t* Value(Cache::Handle* handle);

  void Release(Cache::Handle* handle);

  void Evict(const TINumber dir_id);

 private:
  Cache* cache_;
};

} // namespace indexfs

#endif
