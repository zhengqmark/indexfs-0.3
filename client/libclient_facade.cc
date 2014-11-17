// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/config.h"

namespace indexfs {

DEFINE_string(configfn,
    GetDefaultConfigFileName(), "Set the config file");

DEFINE_string(srvlstfn,
    GetDefaultServerListFileName(), "Set the server list");

#ifdef HDFS
DEFINE_string(hconfigfn, GetDefaultHDFSConfigFileName(), "");
#endif

} /* namespace indexfs */

