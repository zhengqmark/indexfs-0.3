// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "client/libclient_helper.h"

int main(int argc, char* argv[]) {
  SetUsageMessage("IndexFS Client Toolkit - listdir++");
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    std::cerr << "== Usage: " << argv[0] << " <path> " << std::endl;
    return -1;
  }
  struct conf_t* config = NULL;
  if (IDX_Init(config) == 0) {
    const char* p = argv[1];
    int dir_size = IDX_ReaddirPlus(p);
    std::cout << "Directory Size: " << dir_size << std::endl;
    std::cout << "List Directory has not been fully implemented" << std::endl;
  }
  IDX_Destroy();
  return 0;
}
