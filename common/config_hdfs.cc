// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/scanner.h"

namespace indexfs {

#ifdef HDFS

// Default HDFS configuration file
static const char* DEFAULT_HDFS_CONFIG_FILE = "/tmp/indexfs/hdfs";

// Legacy HDFS configuration file used in old releases
static const char* LEGACY_HDFS_CONFIG_FILE = "/tmp/hdfs_conf";

// Trying to figure out the path of the HDFS configuration file.
// Consult the command line argument first, then environmental variables,
// and then resort to a hard-coded legacy file path, if possible.
//
const char* GetHDFSConfigFileName() {
  if (!FLAGS_hconfigfn.empty()) {
    if (access(FLAGS_hconfigfn.c_str(), R_OK) == 0) {
      return FLAGS_hconfigfn.c_str();
    }
    LOG(WARNING)<< "No HDFS config file found at "<< FLAGS_hconfigfn;
  }
  const char* env = getenv("IDXFS_HDFS_CONFIG_FILE");
  if (env != NULL) {
    if (access(env, R_OK) == 0) {
      return env;
    }
    LOG(WARNING) << "No HDFS config file found at " << env;
  }
  LOG(WARNING) << "Resorting to legacy HDFS config file at " << LEGACY_HDFS_CONFIG_FILE;
  if (access(LEGACY_HDFS_CONFIG_FILE, R_OK) == 0) {
    return LEGACY_HDFS_CONFIG_FILE;
  }
  LOG(ERROR) << "Fail to locate HDFS config file -- will commit suicide now!";
  exit(EXIT_FAILURE);
}

// The default location of the HDFS configuration file.
//
const char* GetDefaultHDFSConfigFileName() {
  return DEFAULT_HDFS_CONFIG_FILE;
}

#endif /* ifdef HDFS */

Status Config::LoadHDFSOptionsFromFile(const std::string &file_name) {
  Scanner scanner(file_name);
  if (!scanner.IsOpen()) {
    return Status::IOError("Cannot open file", file_name);
  }
  std::string key, value;
  std::map<std::string, std::string> confs;
  while (scanner.HasNextLine()) {
    if (scanner.NextKeyValue(key, value)) {
      confs[key] = value;
    }
  }
  std::string hdfs_ip = confs["hdfs_ip"];
  std::string hdfs_port = confs["hdfs_port"];
  if (hdfs_ip.empty()) {
    return Status::NotFound("Missing option", "hdfs_ip");
  }
  if (hdfs_port.empty()) {
    return Status::NotFound("Missing option", "hdfs_port");
  }
  hdfs_ip_ = hdfs_ip;
  hdfs_port_ = atoi(hdfs_port.c_str());
  DLOG(INFO)<< "Setting hdfs_ip to: " << hdfs_ip_;
  DLOG(INFO)<< "Setting hdfs_port to: " << hdfs_port_;
  return Status::OK();
}

} /* namespace indexfs */
