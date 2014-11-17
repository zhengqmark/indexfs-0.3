// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "io_client.h"
#include "client/client.h"

#include <sys/stat.h>
#include "common/config.h"
#include "common/logging.h"

namespace indexfs {

DEFINE_string(logfn,
    "indexfs_iotest", "Set the name of the IndexFS log file");
DEFINE_string(configfn,
    GetDefaultConfigFileName(), "Set the IndexFS configuration file");
DEFINE_string(srvlstfn,
    GetDefaultServerListFileName(), "Set the IndexFS server list file");
#ifdef HDFS
DEFINE_string(hconfigfn,
    GetDefaultHDFSConfigFileName(), "Set the IndexFS-HDFS configuration file");
#endif

namespace mpi {

static const uint16_t BULK_BIT
    = (S_ISVTX); // --------t or --------T
static const uint16_t DEFAULT_FILE_PERMISSION
    = (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); // rw-r--r--
static const uint16_t DEFAULT_DIR_PERMISSION
    = (S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH); // rwxr-x-r-x

DEFINE_bool(bulk_insert,
    false, "Set to turn on the client-side bulk_insert feature (indexfs only)");

namespace {

// An IO Client implementation that uses IndexFS as its backend file system.
// Here we assume that such client will only be initialized once. Use --bulk_insert
// to enable the bulk_insert feature of IndexFS.
//
class IndexFSClient: public IOClient {
 public:

  virtual ~IndexFSClient() {
    delete cli_;
    DisposeIndexFSEnv();
  }

  IndexFSClient(): IOClient() {
    InitIndexFSEnv();
    ClientFactory* f = GetDefaultClientFactory();
    cli_ = f->GetClient(LoadClientConfig());
    delete f;
  }

  virtual Status Init() {
    return cli_->Init();
  }

  virtual Status Dispose() {
    return cli_->Dispose();
  }

  virtual void Noop() {
    cli_->Noop();
  }

  virtual void PrintMeasurements(FILE* output) {
    cli_->PrintMeasurements(output);
  }

  virtual Status NewFile          (Path &path);
  virtual Status MakeDirectory    (Path &path);
  virtual Status MakeDirectories  (Path &path);
  virtual Status SyncDirectory    (Path &path);
  virtual Status ResetMode        (Path &path);
  virtual Status GetAttr          (Path &path);
  virtual Status ListDirectory    (Path &path);
  virtual Status Remove           (Path &path);

  virtual Status Rename           (Path &source, Path &destination);

 protected:
  Client* cli_;
  static void InitIndexFSEnv() {
    OpenClientLog(GetLogFileName());
  }
  static void DisposeIndexFSEnv() {
    CloseFSLog();
  }

 private:
  // No copying allowed
  IndexFSClient(const IndexFSClient&);
  IndexFSClient& operator=(const IndexFSClient&);
};

Status IndexFSClient::NewFile
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("mknod %s ... ", path.c_str());
  }
  Status s = cli_->Mknod(path, DEFAULT_FILE_PERMISSION);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::MakeDirectory
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("mkdir %s ... ", path.c_str());
  }
  Status s = !FLAGS_bulk_insert ?
    cli_->Mkdir(path, DEFAULT_DIR_PERMISSION) :
    cli_->Mkdir(path, (BULK_BIT | DEFAULT_DIR_PERMISSION));
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

static
inline size_t NextEntry(Path &path, size_t start) {
  size_t pos = path.find('/', start);
  return pos == std::string::npos ? path.size() : pos;
}

Status IndexFSClient::MakeDirectories
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("mkdirs %s ... \n", path.c_str());
  }
  StatInfo info;
  std::string buffer;
  buffer.reserve(path.size());
  size_t entry = path.rfind('/');
  size_t parent = 0;
  while ((parent = NextEntry(path, parent + 1)) <= entry ) {
    buffer = path.substr(0, parent);
    Status s = cli_->Getattr(buffer, &info);
    if (s.IsNotFound()) {
      if (FLAGS_print_ops) {
        printf("  mkdir %s ... ", buffer.c_str());
      }
      s = !FLAGS_bulk_insert ?
        cli_->Mkdir(buffer, DEFAULT_DIR_PERMISSION) :
        cli_->Mkdir(buffer, (BULK_BIT | DEFAULT_DIR_PERMISSION));
      if (FLAGS_print_ops) {
        printf("%s\n", s.ToString().c_str());
      }
      if (s.IsIOError()) {
        if (FLAGS_print_ops) {
          fprintf(stderr, "warning: dir %s "
            "has been concurrently made by another client\n", buffer.c_str());
        }
      } else if (!s.ok()) return s;
    } else if (!s.ok()) return s;
  }
  if (FLAGS_print_ops) {
    printf("  mkdir %s ... ", path.c_str());
  }
  Status s = !FLAGS_bulk_insert ?
    cli_->Mkdir(path, DEFAULT_DIR_PERMISSION) :
    cli_->Mkdir(path, (BULK_BIT | DEFAULT_DIR_PERMISSION));
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
    printf("mkdirs done\n");
  }
  if (s.IsIOError()) {
    if (FLAGS_print_ops) {
      fprintf(stderr, "warning: dir %s "
        "has been concurrently made by another client\n", path.c_str());
    }
    return Status::OK();
  }
  return s;
}

Status IndexFSClient::SyncDirectory
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("fsyncdir %s ... ", path.c_str());
  }
  Status s = !FLAGS_bulk_insert ?
    Status::OK() : cli_->Fsyncdir(path);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::Rename
  (Path &src, Path &des) {
  if (FLAGS_print_ops) {
    printf("rename %s -> %s ... ", src.c_str(), des.c_str());
  }
  Status s = cli_->Rename(src, des);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::GetAttr
  (Path &path) {
  StatInfo info;
  if (FLAGS_print_ops) {
    printf("getattr %s ... ", path.c_str());
  }
  Status s = cli_->Getattr(path, &info);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::Remove
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("remove %s ... ", path.c_str());
  }
  Status s = cli_->Remove(path);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::ListDirectory
  (Path &path) {
  std::vector<std::string> list;
  if (FLAGS_print_ops) {
    printf("readdir %s ... ", path.c_str());
  }
  Status s = cli_->Readdir(path, &list);
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status IndexFSClient::ResetMode
  (Path &path) {
  if (FLAGS_print_ops) {
    printf("chmod %s ... ", path.c_str());
  }
  Status s = cli_->Chmod(path, (S_IRWXU | S_IRWXG | S_IRWXO));
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

} /* anonymous namespace */

IOClient* IOClient::NewIndexFSClient() {
  return new IndexFSClient();
}

} /* namespace mpi */ } /* namespace indexfs */
