// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_LIBCLIENT_HELPER_H_
#define _INDEXFS_LIBCLIENT_HELPER_H_

#include <fcntl.h>
#include <stdlib.h>

#include "common/config.h"
#include "common/logging.h"
#include "client/libclient.h"

namespace indexfs {

DEFINE_string(configfn, GetDefaultConfigFileName(),
    "Set the IndexFS configuration file");

DEFINE_string(srvlstfn, GetDefaultServerListFileName(),
    "Set the IndexFS server list file");

#ifdef HDFS
DEFINE_string(hconfigfn, GetDefaultHDFSConfigFileName(),
    "Set the IndexFS-HDFS configuration file");
#endif

} // namespace indexfs

using google::SetUsageMessage;
using google::ParseCommandLineFlags;

#endif /* _INDEXFS_LIBCLIENT_HELPER_H_ */
