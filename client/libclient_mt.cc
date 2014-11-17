// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <time.h>
#include <stdio.h>
#include <pthread.h>
#include <gflags/gflags.h>

#include "client.h"
#include "libclient.h"
#include "common/config.h"
#include "common/logging.h"

using ::indexfs::Path;
using ::indexfs::StatInfo;
using ::indexfs::Status;
using ::indexfs::Client;
using ::indexfs::ClientFactory;
using ::indexfs::Config;
using ::indexfs::LoadClientConfig;
using ::indexfs::GetLogFileName;
using ::indexfs::GetConfigFileName;
using ::indexfs::GetServerListFileName;
using ::indexfs::CloseFSLog;
using ::indexfs::OpenClientLog;
using ::indexfs::GetDefaultLogDir;
using ::indexfs::GetDefaultClientFactory;

namespace indexfs {
DEFINE_string(logfn, "libclient",
    "please ignore this option -- libclient log to stderr");
} /* namespace indexfs */

extern "C" {

// true iff initialized
static volatile bool initialized = false;
// Points to a thread-local client instance
static pthread_key_t cli_key;
// Make sure everything get initialized / disposed once!
static pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

static void InitEnv() {
  if (!initialized) {
    pthread_mutex_lock(&init_mutex);
    if (!initialized) {
      FLAGS_logtostderr = true;
      OpenClientLog(GetLogFileName());
      initialized = true;
      pthread_key_create(&cli_key, NULL);
    }
    pthread_mutex_unlock(&init_mutex);
  }
}

static void DisposeEnv() {

}

static int NotImplemented(const char* func) {
  return fprintf(stderr, "%s not implemented\n", func);
}

static int LogErrorAndReturn(Status &st) {
  if (!st.ok()) {
    std::string err = st.ToString();
    fprintf(stderr, "%s\n", err.c_str());
    return -1;
  }
  return 0;
}

static int LogErrorWithPathAndReturn(Status &st, const char* op,
    const char* path) {
  if (!st.ok()) {
    std::string err = st.ToString();
    fprintf(stderr, "Cannot %s at %s - %s\n", op, path, err.c_str());
    return -1;
  }
  return 0;
}

//////////////////////////////////////////////////////////////////////////////////
// LIFE-CYCLE MANAGEMENT
//

void IDX_Destroy() {
  Client* client = (Client*) pthread_getspecific(cli_key);
  if (client != NULL) {
    client->Dispose();
    delete client;
  }
  pthread_setspecific(cli_key, NULL);
  DisposeEnv();
}

static
int IDX_Internal_Init(Config* config) {
  ClientFactory* factory = GetDefaultClientFactory();
  Client* client = factory->GetClient(config);
  Status s = client->Init();
  pthread_setspecific(cli_key, client);
  delete factory;
  return LogErrorAndReturn(s);
}

int IDX_Init(struct conf_t* config) {
  InitEnv();
  if (config == NULL) {
    return IDX_Internal_Init(LoadClientConfig());
  }
  std::vector<std::string> servers;
  if (config->server_ip != NULL) {
    servers.push_back(config->server_ip);
  }
  const char* config_fn = config->config_fn;
  if (config_fn == NULL) {
    config_fn = "";
  }
  const char* serverlist_fn = config->serverlist_fn;
  if (serverlist_fn == NULL) {
    serverlist_fn = "";
  }
  return IDX_Internal_Init(LoadClientConfig(servers, serverlist_fn, config_fn));
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char *path, mode_t mode) {
  std::string p = path;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Mknod(p, mode) : Status::Corruption("System disposed");
  return LogErrorWithPathAndReturn(s, "mknod", path);
}

int IDX_Mkdir(const char *path, mode_t mode) {
  std::string p = path;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Mkdir(p, mode) : Status::Corruption("System disposed");
  if (s.IsIOError())
    s = Status::OK();
  return LogErrorWithPathAndReturn(s, "mkdir", path);
}

int IDX_Unlink(const char *path) {
  std::string p = path;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Remove(p) : Status::Corruption("System disposed");
  return LogErrorWithPathAndReturn(s, "remove", path);
}

int IDX_Chmod(const char *path, mode_t mode) {
  std::string p = path;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Chmod(p, mode) : Status::Corruption("System disposed");
  return LogErrorWithPathAndReturn(s, "chmod", path);
}

int IDX_Readdir(const char *path, size_t *num_entries, char*** list) {
  std::string p(path);
  std::vector<std::string> results;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Readdir(p, &results) : Status::Corruption("System disposed");
  if (s.ok()) {
    char** reslist = new char*[results.size()];
    *num_entries = results.size();
    for (int i = 0; i < results.size(); ++i) {
      reslist[i] = new char[results[i].size() + 1];
      strncpy(reslist[i], results[i].c_str(), results[i].size() + 1);
    }
    *list = reslist;
  }
  return LogErrorWithPathAndReturn(s, "readdir", path);
}

int IDX_ReaddirPlus(const char *path) {
  std::string p(path);
  std::vector<std::string> results;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Readdir(p, &results) : Status::Corruption("System disposed");
  if (s.ok()) {
    return results.size();
  }
  return LogErrorWithPathAndReturn(s, "listdir", path);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
  std::string p = path;
  StatInfo info;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Getattr(p, &info) : Status::Corruption("System disposed");
  if (s.ok()) {
    buf->st_ino = info.id;
    buf->st_mode = info.mode;
    buf->st_uid = info.uid;
    buf->st_gid = info.gid;
    buf->st_size = info.size;
    buf->st_dev = info.zeroth_server;
    buf->st_mtime = info.mtime;
    buf->st_ctime = info.ctime;
    buf->st_atime = time(NULL);
  }
  return LogErrorWithPathAndReturn(s, "getattr", path);
}

int IDX_GetInfo(const char *path, struct info_t *buf) {
  std::string p = path;
  StatInfo info;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Getattr(p, &info) : Status::Corruption("System disposed");
  if (s.ok()) {
    buf->permission = info.mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    buf->is_dir = S_ISDIR(info.mode);
    buf->size = info.size;
    buf->uid = info.uid;
    buf->gid = info.gid;
    buf->atime = time(NULL);
    buf->ctime = info.ctime;
  }
  return LogErrorWithPathAndReturn(s, "getattr", path);
}

int IDX_Create(const char *path, mode_t mode) {
  return IDX_Mknod(path, mode);
}

int IDX_Rmdir(const char *path) {
  return IDX_Unlink(path);
}

int IDX_RecMknod(const char *path, mode_t mode) {
  return IDX_Mknod(path, mode); // fail back to mknod
}

int IDX_RecMkdir(const char *path, mode_t mode) {
  return IDX_Mkdir(path, mode); // fail back to mkdir
}

int IDX_Access(const char* path) {
  std::string p = path;
  StatInfo info;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Getattr(p, &info) : Status::Corruption("System disposed");
  return (s.ok()) ? 0 : -1;
}

int IDX_AccessDir(const char* path) {
  std::string p = path;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->AccessDir(p) : Status::Corruption("System disposed");
  return (s.ok()) ? 0 : -1;
}

//////////////////////////////////////////////////////////////////////////////////
// IO OPERATIONS
//

int IDX_Fsync(int fd) {
  return 0; // Do nothing
}

int IDX_Close(int fd) {
  if (fd > 0) {
    Client* client = (Client*) pthread_getspecific(cli_key);
    Status s =
        client != NULL ?
            client->Close(fd) : Status::Corruption("System disposed");
    return LogErrorAndReturn(s);
  }
  return 0; // Simply ignore non-positive fds
}

int IDX_Open(const char *path, int flags, int *fd) {
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Open(std::string(path), (int16_t) flags, fd) :
          Status::Corruption("System disposed");
  return LogErrorAndReturn(s);
}

int IDX_Read(int fd, void *buf, size_t size) {
  return 0; // NotImplemented(__func__);
}

int IDX_Write(int fd, const void *buf, size_t size) {
  return 0; // NotImplemented(__func__);
}

int IDX_Pread(int fd, void *buf, off_t offset, size_t size) {
  int ret_size;
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Read(fd, offset, size, (char *) buf, &ret_size) :
          Status::Corruption("System disposed");
  return s.ok() ? ret_size : LogErrorAndReturn(s);
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  Client* client = (Client*) pthread_getspecific(cli_key);
  Status s =
      client != NULL ?
          client->Write(fd, offset, size, (char *) buf) :
          Status::Corruption("System disposed");
  return s.ok() ? size : LogErrorAndReturn(s);
}

} /* end extern "C" */
