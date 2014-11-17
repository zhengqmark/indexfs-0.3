// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/dirhandle.h"

namespace indexfs {

DirMappingCache* DirHandle::dmap_cache_ = NULL;
DirCache* DirHandle::dir_cache_ = NULL;

} // namespace indexfs
