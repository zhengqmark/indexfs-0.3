// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_LEGACY_OPTIONS_H_
#define _INDEXFS_LEGACY_OPTIONS_H_

#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX        512
#endif
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX   64
#endif

#define MAX_LEN         512
#define MAX_SERVERS     256

#define ROOT_DIR_ID             0
#define PARENT_OF_ROOT          0
#define PARTITION_OF_ROOT       0
#define FILE_THRESHOLD          65536

// Default server port number
#define DEFAULT_SRV_PORT 45678

// Default directory bulk insertion size
#define DEFAULT_DIR_BULK_SIZE    (1<<10)
// Default bulk insertion size
#define DEFAULT_BULK_SIZE        (1<<20)
// Default directory split threshold
#define DEFAULT_DIR_SPLIT_THR    (1<<11)
// Default number of directory control blocks
#define DEFAULT_DIR_CTRL_BLOCKS  (1<<20)
// Default size of the directory entry cache
#define DEFAULT_DENT_CACHE_SIZE  (1<<16)
// Default size of the directory mapping cache
#define DEFAULT_DMAP_CACHE_SIZE  (1<<15)

#endif /* _INDEXFS_LEGACY_OPTIONS_H_ */
