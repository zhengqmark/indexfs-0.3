// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_BITMAP_H_
#define _INDEXFS_COMMON_BITMAP_H_

#include <string.h>

extern "C" {
#include "common/giga_index.h" // legacy header
}

#include "thrift/indexfs_types.h"

namespace indexfs {

// Convert from the new bitmap struct to its legacy counterpart.
//
inline giga_mapping_t ToLegacyMapping(const GigaBitmap &mapping) {
  giga_mapping_t legacy;
  legacy.id = mapping.id;
  legacy.server_count = mapping.num_servers;
  legacy.zeroth_server = mapping.zeroth_server;
  legacy.curr_radix = mapping.curr_radix;
  memcpy(legacy.bitmap, mapping.bitmap.data(), sizeof(legacy.bitmap));
  return legacy;
}

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_BITMAP_H_ */
