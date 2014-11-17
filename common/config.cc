// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/scanner.h"
#include "common/network.h"

namespace indexfs {

// Default log file name
static const char* DEFAULT_LOG_FILE = "indexfs";
// Default log directory
static const char* DEFAULT_LOG_DIR = "/tmp/indexfs/logs";
// Default server list file
static const char* DEFAULT_SERVER_LIST = "/tmp/indexfs/servers";
// Default configuration file
static const char* DEFAULT_CONFIG_FILE = "/tmp/indexfs/config";
// Legacy server list file used in old releases
static const char* LEGACY_SERVER_LIST = "/tmp/giga_conf";
// Legacy configuration file used in old releases
static const char* LEGACY_CONFIG_FILE = "/tmp/idxfs_conf";

// Create a new configuration object for clients
//
Config* Config::CreateClientConfig() {
  return new Config(false);
}

// Create a new configuration object for servers
//
Config* Config::CreateServerConfig() {
  return new Config(true);
}

// Create a fresh configuration object.
//
Config::Config(bool is_server) :
    srv_id_(-1), server_side_(is_server), hdfs_port_(-1) {
}

/*---------------------------------------------------
 * Configuration
 * --------------------------------------------------
 */

Status Config::LoadNetworkInfo() {
  Status s;
  s = FetchHostname(&host_name_);
  if (!s.ok()) {
    return s;
  }
  s = GetHostIPAddrs(&ip_addrs_);
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string>::iterator it = ip_addrs_.begin();
  for (; it != ip_addrs_.end(); it++) {
    DLOG(INFO)<< "Local IP: " << *it;
  }
  DLOG(INFO)<< "Local host name: " << host_name_;
  return Status::OK();
}

// Reset the server ID unconditionally.
//
Status Config::SetServerID(int srv_id) {
  srv_id_ = srv_id;
  return Status::OK();
}

// Directly set the member servers by injecting a list of servers into the configuration
// object. No action will be taken if the provided server list is empty, otherwise, the
// original set of member servers will be overridden in its entirety.
// In addition to setting the servers, we will also try to figure out our server ID
// by comparing IP addresses of the local machine with IP addresses on the server list.
//
Status Config::SetServers(const std::vector<std::string> &servers) {
  if (!servers.empty()) {
    srv_addrs_.clear();
    std::vector<std::string>::const_iterator it = servers.begin();
    for (; it != servers.end(); it++) {
      const std::string &ip = *it;
      srv_addrs_.push_back(std::make_pair(ip, GetDefaultSrvPort()));
      for (size_t i = 0; srv_id_ < 0 && i < ip_addrs_.size(); i++) {
        if (ip == ip_addrs_[i]) {
          srv_id_ = srv_addrs_.size() - 1;
        }
      }
    }
  }
  return Status::OK();
}

// Directly set the member servers by injecting a list of servers into the configuration
// object. No action will be taken if the provided server list is empty, otherwise, the
// original set of member servers will be overridden in its entirety.
// In addition to setting the servers, we will also try to figure out our server ID
// by comparing IP addresses of the local machine with IP addresses on the server list.
//
Status Config::SetServers(const std::vector<std::pair<std::string, int> > &servers) {
  if (!servers.empty()) {
    srv_addrs_.clear();
    std::vector<std::pair<std::string, int> >::const_iterator it = servers.begin();
    for (; it != servers.end(); it++) {
      const std::string &ip = it->first;
      srv_addrs_.push_back(std::make_pair(ip, it->second));
      for (size_t i = 0; srv_id_ < 0 && i < ip_addrs_.size(); i++) {
        if (ip == ip_addrs_[i]) {
          srv_id_ = srv_addrs_.size() - 1;
        }
      }
    }
  }
  return Status::OK();
}

// Retrieve a fixed set of member servers by loading their IP addresses and port numbers
// from a user-specified server list file. If we already have a set of member servers, then
// the provided server list will be ignored and no server will be loaded.
// In addition to loading a list of servers, we will also try to figure out our server ID
// by comparing IP addresses of the local machine with IP addresses on the server list.
//
Status Config::LoadServerList(const std::string &file_name) {
  if (srv_addrs_.empty()) {
    Scanner scanner(file_name);
    if (!scanner.IsOpen()) {
      return Status::IOError("Cannot open file", file_name);
    }
    std::string ip, port;
    while (scanner.HasNextLine()) {
      if (scanner.NextServerAddress(ip, port)) {
        if (port.empty()) {
          srv_addrs_.push_back(std::make_pair(ip, GetDefaultSrvPort()));
        } else {
          srv_addrs_.push_back(std::make_pair(ip, atoi(port.c_str())));
        }
        for (size_t i = 0; srv_id_ < 0 && i < ip_addrs_.size(); i++) {
          if (ip == ip_addrs_[i]) {
            srv_id_ = srv_addrs_.size() - 1;
          }
        }
      }
    }
  }
  if (srv_addrs_.empty()) {
    return Status::Corruption("Empty server list", file_name);
  }
  if (srv_id_ < 0) {
    if (IsServer()) {
      return Status::Corruption("Missing local server in the server list",
          file_name);
    }
  }
  if (srv_id_ >= srv_addrs_.size()) {
    return Status::Corruption("Illegal server ID", "" + srv_id_);
  }
  std::vector<std::pair<std::string, int> >::iterator it = srv_addrs_.begin();
  for (; it != srv_addrs_.end(); it++) {
    DLOG(INFO)<< "Accepting server: " << it->first << ":" << it->second;
  }
  DLOG(INFO)<< "Setting server ID to: " << srv_id_;
  return Status::OK();
}

Status Config::LoadOptionsFromFile(const std::string &file_name) {
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
  file_dir_ = confs["file_dir"];
  split_dir_ = confs["split_dir"];
  leveldb_dir_ = confs["leveldb_dir"];
  if (leveldb_dir_.empty()) {
    return Status::NotFound("Missing option", "leveldb_dir");
  }
  if (split_dir_.empty()) {
    return Status::NotFound("Missing option", "split_dir");
  }
  if (file_dir_.empty()) {
    return Status::NotFound("Missing option", "file_dir");
  }
  DLOG(INFO)<< "Setting file_dir to: " << file_dir_;
  DLOG(INFO)<< "Setting spli_dir to: " << split_dir_;
  DLOG(INFO)<< "Setting leveldb_dir to: " << leveldb_dir_;
  return Status::OK();
}

/*---------------------------------------------------
 * Main Interface
 * --------------------------------------------------
 */

static inline
void CheckErrors(const Status &status) {
  CHECK(status.ok()) << status.ToString();
}

static
void LoadConfig(Config* config) {
  CheckErrors(config->LoadNetworkInfo());
  CheckErrors(config->LoadOptionsFromFile(GetConfigFileName()));
#ifdef HDFS
  CheckErrors(config->LoadHDFSOptionsFromFile(GetHDFSConfigFileName()));
#endif
  FlushFSLog();
}

// Create and prepare a configuration object for servers.
//
Config* LoadServerConfig(int srv_id,
    const std::vector<std::pair<std::string, int> > &servers) {
  Config* srv_conf = Config::CreateServerConfig();
  LoadConfig(srv_conf);
  CheckErrors(srv_conf->SetServerID(srv_id));
  CheckErrors(srv_conf->SetServers(servers));
  CheckErrors(srv_conf->LoadServerList(GetServerListFileName()));
  return srv_conf;
}

// Create and prepare a configuration object for clients.
//
Config* LoadClientConfig(const std::vector<std::string> &servers,
    const std::string &server_list, const std::string &config_file,
    const std::string &hconfig_file) {
  Config* cli_conf = Config::CreateClientConfig();
  if (!server_list.empty()) {
    FLAGS_srvlstfn = server_list;
  }
  if (!config_file.empty()) {
    FLAGS_configfn = config_file;
  }
#ifdef HDFS
  if (!hconfig_file.empty()) {
    FLAGS_hconfigfn = hconfig_file;
  }
#endif
  LoadConfig(cli_conf);
  CheckErrors(cli_conf->SetServers(servers));
  CheckErrors(cli_conf->LoadServerList(GetServerListFileName()));
  return cli_conf;
}

// Attempting to figure out the name of the log file. Try the command
// line argument first, then environmental variables, and then resort
// to a hard-coded default log file name.
//
const char* GetLogFileName() {
  if (!FLAGS_logfn.empty()) {
    return FLAGS_logfn.c_str();
  }
  const char* env = getenv("IDXFS_LOG_NAME");
  if (env != NULL) {
    return env;
  }
  LOG(WARNING)<< "No log file name specified -- use \"" << DEFAULT_LOG_FILE << "\" by default";
  return DEFAULT_LOG_FILE;
}

// Trying to figure out the path of the configuration file. Consult
// the command line argument first, then environmental variables, and
// then resort to a hard-coded legacy file path, if possible.
//
const char* GetConfigFileName() {
  if (!FLAGS_configfn.empty()) {
    if (access(FLAGS_configfn.c_str(), R_OK) == 0) {
      return FLAGS_configfn.c_str();
    }
    LOG(WARNING)<< "No config file found at "<< FLAGS_configfn;
  }
  const char* env = getenv("IDXFS_CONFIG_FILE");
  if (env != NULL) {
    if (access(env, R_OK) == 0) {
      return env;
    }
    LOG(WARNING) << "No config file found at " << env;
  }
  LOG(WARNING) << "Resorting to legacy config file at" << LEGACY_CONFIG_FILE;
  if (access(LEGACY_CONFIG_FILE, R_OK) == 0) {
    return LEGACY_CONFIG_FILE;
  }
  LOG(ERROR) << "Fail to locate config file -- will commit suicide now!";
  exit(EXIT_FAILURE);
}

// Trying to figure out the path of the server list file. Consult
// the command line argument first, then environmental variables, and
// then resort to a hard-coded legacy file path, if possible.
//
const char* GetServerListFileName() {
  if (!FLAGS_srvlstfn.empty()) {
    if (access(FLAGS_srvlstfn.c_str(), R_OK) == 0) {
      return FLAGS_srvlstfn.c_str();
    }
    LOG(WARNING)<< "No server list found at " << FLAGS_srvlstfn;
  }
  const char* env = getenv("IDXFS_SERVER_LIST");
  if (env != NULL) {
    if (access(env, R_OK) == 0) {
      return env;
    }
    LOG(WARNING) << "No server list found at " << FLAGS_srvlstfn;
  }
  LOG(WARNING) << "Resorting to legacy server list file at " << LEGACY_SERVER_LIST;
  if (access(LEGACY_SERVER_LIST, R_OK) == 0) {
    return LEGACY_SERVER_LIST;
  }
  LOG(ERROR) << "Fail to locate server list -- will commit suicide now!";
  exit(EXIT_FAILURE);
}

// The default log directory used to reset
// glog's default log directory, which is often "/tmp".
// NB: glog will not try to create any parent directories of its log files.
// Make sure they exist before glog attempts to create any log files.
//
const char* GetDefaultLogDir() {
  return DEFAULT_LOG_DIR;
}

// The default location of the configuration file.
//
const char* GetDefaultConfigFileName() {
  return DEFAULT_CONFIG_FILE;
}

// The default location of the server list file.
//
const char* GetDefaultServerListFileName() {
  return DEFAULT_SERVER_LIST;
}

} /* namespace indexfs */
