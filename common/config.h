// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_CONFIG_H_
#define _INDEXFS_COMMON_CONFIG_H_

#include "common/common.h"
#include "common/options.h"

#include <map>
#include <vector>
#include <string>

#include <string.h>
#include <stdlib.h>
#include <gflags/gflags.h>

namespace indexfs {

// The main configuration interface shared by both clients and servers
//
class Config {

  explicit Config(bool is_server);

  // Server ID, or -1 for clients
  //
  int srv_id_;

  // True iff running at the server-side
  //
  bool server_side_;

  // Local machine host name
  //
  std::string host_name_;

  // Local machine IP addresses
  // We assume our local machines may have multiple NICs,
  // so we must consider all of them
  //
  std::vector<std::string> ip_addrs_;

  // Server address list
  //
  std::vector<std::pair<std::string, int> > srv_addrs_;

  // Data directory for large files
  //
  std::string file_dir_;

  // Temporary directory for transient files generated during directory splitting
  //
  std::string split_dir_;

  // Data directory for METADATA persistence
  //
  std::string leveldb_dir_;

  // HDFS name node port number
  //
  int hdfs_port_;

  // HDFS name node IP address
  //
  std::string hdfs_ip_;

 public:

  virtual ~Config() { }

  static Config* CreateServerConfig();

  static Config* CreateClientConfig();

  // Returns the ID of the current server, or -1 for clients
  //
  int GetSrvID() { return srv_id_; }

  // Returns true iff running as a server
  //
  bool IsServer() { return server_side_; }

  // Returns the host name of the local machine.
  //
  const std::string& GetHostname() { return host_name_; }

  // Returns the total number of servers
  //
  int GetSrvNum() { return srv_addrs_.size(); }

  // Returns the default port number.
  //
  int GetDefaultSrvPort() { return DEFAULT_SRV_PORT; }

  // Returns the IP address of a given server.
  //
  const std::string& GetSrvIP(int srv_id) { return srv_addrs_[srv_id].first; }

  // Returns the port number of a given server.
  //
  int GetSrvPort(int srv_id) { return srv_addrs_[srv_id].second; }

  // Returns the IP address and port number of a given server.
  //
  const std::pair<std::string, int>& GetSrvAddr(int srv_id) { return srv_addrs_[srv_id]; }

  // Returns the data directory for LevelDB
  //
  const std::string& GetLevelDBDir() { return leveldb_dir_; }

  // Returns the storage directory for user file data
  //
  const std::string& GetFileDir() { return file_dir_; }

  // Returns the temporary directory for directory splitting
  //
  const std::string& GetSplitDir() { return split_dir_; }

  // Returns the host name of HDFS
  //
  const char* GetHDFSIP() { return hdfs_ip_.c_str(); }

  // Returns the port of HDFS
  //
  int GetHDFSPort() { return hdfs_port_; }

  // Returns the threshold for directory splitting
  //
  int GetSplitThreshold() {
    const char* env = getenv("FS_DIR_SPLIT_THR");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DIR_SPLIT_THR );
    return result > 0 ? result : DEFAULT_DIR_SPLIT_THR;
  }

  // Returns the max number of entries that could be bulk inserted into
  // a shadow namespace.
  //
  int GetBulkSize() {
    const char* env = getenv("FS_BULK_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_BULK_SIZE );
    return result > 0 ? result : DEFAULT_BULK_SIZE;
  }

  // Returns the max number of directories that could be bulk inserted into
  // a shadow namespace.
  //
  int GetDirBulkSize() {
    const char* env = getenv("FS_DIR_BULK_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DIR_BULK_SIZE );
    return result > 0 ? result : DEFAULT_DIR_BULK_SIZE;
  }

  // Returns the number of the directory control blocks.
  //
  int GetDirCacheSize() {
    const char* env = getenv("FS_DIR_CTRL_BLOCKS");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DIR_CTRL_BLOCKS );
    return result > 0 ? result : DEFAULT_DIR_CTRL_BLOCKS;
  }

  // Returns the size of the directory mapping cache.
  //
  int GetDirMappingCacheSize() {
    const char* env = getenv("FS_DMAP_CACHE_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DMAP_CACHE_SIZE );
    return result > 0 ? result : DEFAULT_DMAP_CACHE_SIZE;
  }

  // Returns the size of the directory entry cache.
  //
  int GetDirEntryCacheSize() {
    const char* env = getenv("FS_DENT_CACHE_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DENT_CACHE_SIZE );
    return result > 0 ? result : DEFAULT_DENT_CACHE_SIZE;
  }

  Status SetServerID(int srv_id);
  Status SetServers(const std::vector<std::string> &servers);
  Status SetServers(const std::vector<std::pair<std::string, int> > &servers);

  Status LoadNetworkInfo();
  Status LoadServerList(const std::string &file_name);
  Status LoadOptionsFromFile(const std::string &file_name);
  Status LoadHDFSOptionsFromFile(const std::string &file_name);
};

/*---------------------------------------------------
 * Command Line Arguments
 * --------------------------------------------------
 */

// file name for the main log file
DECLARE_string(logfn);
extern const char* GetLogFileName();
extern const char* GetDefaultLogDir();

// file name for the configuration file
DECLARE_string(configfn);
extern const char* GetConfigFileName();
extern const char* GetDefaultConfigFileName();

// file name for the server list file
DECLARE_string(srvlstfn);
extern const char* GetServerListFileName();
extern const char* GetDefaultServerListFileName();

// file name for the HDFS specific configuration file
#ifdef HDFS
DECLARE_string(hconfigfn);
extern const char* GetHDFSConfigFileName();
extern const char* GetDefaultHDFSConfigFileName();
#endif

/*---------------------------------------------------
 * Main Interface
 * --------------------------------------------------
 */

// This function should be called at the server's bootstrapping phase.
// It will load important system options from a set of configuration files
// whose locations are determined by a set of command line arguments or
// environmental variables. Callers of this function can optionally choose to
// explicitly specify the server ID and an initial set of member servers,
// bypassing or superseding the configuration files.
//
extern Config* LoadServerConfig(
    int srv_id = -1,
    const std::vector<std::pair<std::string, int> > &servers
        = std::vector<std::pair<std::string, int> >());

// This function should be called at the client's bootstrapping phase.
// It will load important system options from a set of configuration files
// whose locations are either given as arguments here or specified by a set of
// command-line arguments or environmental variables. Callers of this
// function can optionally choose to explicitly specify an initial set of
// member servers, bypassing or superseding the configuration files.
//
extern Config* LoadClientConfig(
    const std::vector<std::string> &servers = std::vector<std::string>(),
    const std::string &server_list = std::string(),
    const std::string &config_file = std::string(),
    const std::string &hconfig_file = std::string());

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_CONFIG_H_ */
