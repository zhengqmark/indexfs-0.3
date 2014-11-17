// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "client/libclient_helper.h"

int main(int argc, char* argv[]) {
  SetUsageMessage("IndexFS Client Toolkit - listdir");
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    std::cerr << "== Usage: " << argv[0] << " <path> " << std::endl;
    return -1;
  }
  size_t num;
  char** list;
  struct conf_t* config = NULL;
  if (IDX_Init(config) == 0) {
    const char* p = argv[1];
    if (IDX_Readdir(p, &num, &list) == 0) {
      for (size_t i = 0; i < num; ++i) {
        std::cout << list[i] << std::endl;
        delete[] list[i];
      }
      delete[] list;
    }
  }
  IDX_Destroy();
  return 0;
}
