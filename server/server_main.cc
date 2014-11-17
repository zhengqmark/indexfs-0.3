// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <signal.h>
#include <sstream>
#include <iostream>
#include <gflags/gflags.h>

#include "common/config.h"
#include "common/logging.h"
#include "metadata_server.h"
#include "split_thread.h"
#include "util/monitor_thread.h"

namespace indexfs {
  
DEFINE_int32(srvid, -1,
    "Manually set the IndexFS server ID");

DEFINE_string(logfn, "metadata_server",
    "Set the IndexFS log file name");

DEFINE_string(configfn, GetDefaultConfigFileName(),
    "Set the IndexFS configuration file");

DEFINE_string(srvlstfn, GetDefaultServerListFileName(),
    "Set the IndexFS server list file");

#ifdef HDFS
DEFINE_string(hconfigfn, GetDefaultHDFSConfigFileName(),
    "Set the indexfs-hdfs configuration file");
#endif

static Env* env;
static Config* config;
static DirCache* dir_cache;
static DirMappingCache* dmap_cache;
static DirEntryCache<ServerDirEntryValue>* dent_cache;

namespace {

static RPC_Server* server;
static MetadataBackend mdb;
static Measurement* measure;
static MonitorThread* monitor;
static SplitThread* split_thread;

void SignalHandler(const int sig) {
  DLOG(INFO) << "SIGINT=" << sig << " handled";
  LOG(INFO) << "Stopping metadata server ...";
  server->Stop();
}

void SetupSignalHandler() {
  signal(SIGINT, SignalHandler);     // handling SIGINT
  signal(SIGTERM, SignalHandler);    // handling SIGTERM
}

void InitEnvironment() {
#if defined(OS_LINUX)
#if defined(HDFS)
    env = Env::HDFSEnv(config->GetHDFSIP(), config->GetHDFSPort());
#elif defined(PVFS)
    env = Env::PVFSEnv();
#else
    env = Env::Default();
#endif
#endif
  dir_cache = new DirCache(config->GetDirCacheSize());
  dmap_cache = new DirMappingCache(config->GetDirMappingCacheSize());
  dent_cache = new DirEntryCache<ServerDirEntryValue>(config->GetDirMappingCacheSize());
}

static
void PrepareStorageDirectory(const std::string &dirname) {
  Status s = env->CreateDir(dirname);
  CHECK(s.ok() || env->FileExists(dirname))
    << "Fail to create storage directory: " << dirname;
}

void InitRootPartition() {
  PrepareStorageDirectory(config->GetFileDir());
  PrepareStorageDirectory(config->GetLevelDBDir());
  PrepareStorageDirectory(config->GetSplitDir());

  srand(config->GetSrvID());

  std::stringstream ss;
  ss << config->GetLevelDBDir() << "/l" << config->GetSrvID();
  std::string leveldb_path = ss.str();

  int mdb_setup = mdb.Init(leveldb_path, config->GetHDFSIP(),
                           config->GetHDFSPort(), config->GetSrvID());
  CHECK(mdb_setup >= 0) << "Fail to initialize leveldb";

  int dir_id = ROOT_DIR_ID;
  struct giga_mapping_t mapping;

  int ret;
  switch (mdb_setup) {
    case 1:
    LOG(INFO) << "Creating new file system at " << leveldb_path;
    giga_init_mapping(&mapping, 0, dir_id, 0, config->GetSrvNum());
    dmap_cache->Insert(dir_id, mapping);
    ret = mdb.Mkdir(dir_id, -1, "", dir_id, config->GetSrvID(), config->GetSrvNum());
    CHECK(ret == 0) << "Error creating root mapping structure";
    break;

    case 0:
    LOG(INFO) << "Reading old file system from " << leveldb_path;
    ret = mdb.ReadBitmap(dir_id, &mapping);
    if (ret != 0) {
      giga_init_mapping(&mapping, 0, dir_id, 0, config->GetSrvNum());
      ret = mdb.Mkdir(dir_id, -1, "", dir_id, config->GetSrvID(), config->GetSrvNum());
      CHECK(ret == 0) << "Error creating root mapping structure";
    }
    dmap_cache->Insert(dir_id, mapping);
    break;
  }
}

void InitMonitor() {
  std::vector<std::string> metrics;
  MetadataServer::GetInstrumentPoints(metrics);
  measure = new Measurement(metrics, config->GetSrvID());
  monitor = new MonitorThread(measure);
}

void CleanMonitor() {
  delete monitor;
  delete measure;
}

void LaunchMetadataServer() {
  split_thread = new SplitThread(measure);
  MetadataServer::Init(config, &mdb,
                       env, dent_cache, dmap_cache, dir_cache,
                       measure, split_thread);
  MetadataServer* handler = new MetadataServer();
  server = RPC_Server::CreateRPCServer(config, handler);
  LOG(INFO)<< "Starting metadata server...";
  monitor->Start();
  server->RunForever();
}

void Cleanup() {
  mdb.Close();
  CleanMonitor();
  CloseFSLog();
  delete split_thread;
}

} //namespace

} //namespace indexfs

int main(int argc, char* argv[]) {
  FLAGS_logbufsecs = 5;
  FLAGS_log_dir = indexfs::GetDefaultLogDir();
  google::SetUsageMessage("IndexFS Scalable Metadata Server");
  google::ParseCommandLineFlags(&argc, &argv, true);

  indexfs::OpenServerLog(indexfs::GetLogFileName());
  indexfs::config = indexfs::LoadServerConfig(indexfs::FLAGS_srvid);
  indexfs::InitEnvironment();
  indexfs::InitRootPartition();
  indexfs::InitMonitor();
  indexfs::SetupSignalHandler();
  indexfs::LaunchMetadataServer();
  indexfs::Cleanup();

  return 0;
}
