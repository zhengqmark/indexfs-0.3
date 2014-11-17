// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "dmapcache.h"
#include "leveldb/util/coding.h"

namespace indexfs {

static void DeleteEntry(const Slice& key, void* value) {
  giga_mapping_t* dv = reinterpret_cast<giga_mapping_t*>(value);
  if (dv != NULL)
    delete dv;
}

DirMappingCache::DirMappingCache(int entries)
  : cache_(leveldb::NewLRUCache(entries)) {
}

DirMappingCache::~DirMappingCache() {
  if (cache_ != NULL)
    delete cache_;
}

Cache::Handle* DirMappingCache::Get(const TINumber dir_id) {
  char buf[sizeof(TINumber)];
  leveldb::EncodeFixed64(buf, dir_id);
  Slice key(buf, sizeof(buf));
  return cache_->Lookup(key);
}

Cache::Handle* DirMappingCache::Put(const TINumber dir_id,
                                    const giga_mapping_t &mapping) {
  char buf[sizeof(TINumber)];
  leveldb::EncodeFixed64(buf, dir_id);
  Slice key(buf, sizeof(buf));
  giga_mapping_t* value = new giga_mapping_t(mapping);
  return cache_->Insert(key, value, 1, &DeleteEntry);
}

void DirMappingCache::Insert(const TINumber dir_id,
                                  const giga_mapping_t &mapping) {
  cache_->Release(Put(dir_id, mapping));
}

giga_mapping_t* DirMappingCache::Value(Cache::Handle* handle) {
  return reinterpret_cast<giga_mapping_t*>(cache_->Value(handle));
}

void DirMappingCache::Release(Cache::Handle* handle) {
  cache_->Release(handle);
}

void DirMappingCache::Evict(const TINumber dir_id) {
  char buf[sizeof(TINumber)];
  leveldb::EncodeFixed64(buf, dir_id);
  Slice key(buf, sizeof(buf));
  cache_->Erase(key);
}

} // indexfs
