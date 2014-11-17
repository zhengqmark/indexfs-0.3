// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <time.h>
#include <stdio.h>
#include <fcntl.h>
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

static Client* client = NULL; //  <-- The CXX interface being adapted

static void InitEnv() {
  FLAGS_logtostderr = true;
  OpenClientLog(GetLogFileName());
}

static void DisposeEnv() {
  CloseFSLog();
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

//////////////////////////////////////////////////////////////////////////////////
// LIFE-CYCLE MANAGEMENT
//

void IDX_Destroy() {
  client->Dispose();
  delete client;
  client = NULL;
  DisposeEnv();
}

int IDX_Init(struct conf_t* config) {
  InitEnv();
  ClientFactory* factory = GetDefaultClientFactory();
  client = factory->GetClient(LoadClientConfig());
  Status s = client->Init();
  delete factory;
  return LogErrorAndReturn(s);
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char *path, mode_t mode) {
  std::string p = path;
  Status s = client->Mknod(p, mode);
  return LogErrorAndReturn(s);
}

int IDX_Mkdir(const char *path, mode_t mode) {
  std::string p = path;
  Status s = client->Mkdir(p, mode);
  return LogErrorAndReturn(s);
}

int IDX_Unlink(const char *path) {
  std::string p = path;
  Status s = client->Remove(p);
  return LogErrorAndReturn(s);
}

int IDX_Chmod(const char *path, mode_t mode) {
  std::string p = path;
  Status s = client->Chmod(p, mode);
  return LogErrorAndReturn(s);
}

int IDX_Readdir(const char *path, size_t *num_entries, char*** list) {
  std::string p(path);
  std::vector<std::string> results;
  Status s = client->Readdir(p, &results);
  if (s.ok()) {
    char** reslist = new char*[results.size()];
    *num_entries = results.size();
    for (int i = 0; i < results.size(); ++i) {
      reslist[i] = new char[results[i].size() + 1];
      strncpy(reslist[i], results[i].c_str(), results[i].size() + 1);
    }
    *list = reslist;
  }
  return LogErrorAndReturn(s);
}

int IDX_ReaddirPlus(const char *path) {
  std::string p(path);
  std::vector<std::string> buf_names;
  std::vector<StatInfo> buf_entries;
  Status s = client->ReaddirPlus(p, &buf_names, &buf_entries);
  if (s.ok()) {
    return buf_entries.size();
  }
  return LogErrorAndReturn(s);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
  std::string p = path;
  StatInfo info;
  Status s = client->Getattr(p, &info);
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
  return LogErrorAndReturn(s);
}

int IDX_GetInfo(const char *path, struct info_t *buf) {
  std::string p = path;
  StatInfo info;
  Status s = client->Getattr(p, &info);
  if (s.ok()) {
    buf->permission = info.mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    buf->is_dir = S_ISDIR(info.mode);
    buf->size = info.size;
    buf->uid = info.uid;
    buf->gid = info.gid;
    buf->atime = time(NULL);
    buf->ctime = info.ctime;
  }
  return LogErrorAndReturn(s);
}

int IDX_Create(const char *path, mode_t mode) {
  IDX_Mknod(path, mode);
  int fd;
  if (IDX_Open(path, O_WRONLY, &fd) == 0) {
    return fd;
  } else {
    return -1;
  }
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
  Status s = client->Getattr(p, &info);
  return (s.ok()) ? 0 : -1;
}

int IDX_AccessDir(const char* path) {
  std::string p = path;
  Status s = client->AccessDir(p);
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
    Status s = client->Close(fd);
    return LogErrorAndReturn(s);
  }
  return 0; // Ignore non-positive fds
}

int IDX_Open(const char *path, int flags, int *fd) {
  Status s = client->Open(std::string(path), (int16_t) flags, fd);
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
  Status s = client->Read(fd, offset, size, (char *) buf, &ret_size);
  return s.ok() ? ret_size : LogErrorAndReturn(s);
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  Status s = client->Write(fd, offset, size, (char *) buf);
  return s.ok() ? size : LogErrorAndReturn(s);
}

} /* end extern "C" */
