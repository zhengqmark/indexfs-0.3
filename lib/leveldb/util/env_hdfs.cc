// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/posix_logger.h"
#if defined(OS_LINUX)
#if defined(HDFS)
#include "hdfs.h"

namespace leveldb {

namespace {

static const std::string hdfs_prefix = "hdfs://";

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

#define HDFS_VERBOSITY 1
void hdfsDebugLog(short verbosity, const char* format, ... ) {
  if(verbosity <= HDFS_VERBOSITY) {
    va_list args;
    va_start( args, format );
    vfprintf( stdout, format, args );
    va_end( args );
  }
}

bool stringEndsWith(const std::string &src, const std::string &suffix) {
  if (src.length() >= suffix.length()) {
    return src.compare(src.length()-suffix.length(),
                       suffix.length(),
                       suffix) == 0;
  } else {
    return false;
  }
}

bool stringStartsWith(const std::string &src, const std::string &prefix) {
  if (src.length() >= prefix.length()) {
    return src.compare(0,
                       prefix.length(),
                       prefix) == 0;
  } else {
    return false;
  }
}

bool lastComponentStartsWith(const std::string &src, const std::string &prefix) {
  if (src.length() >= prefix.length()) {
    size_t pos = src.find_last_of('/') + 1;
    return src.compare(pos,
                       prefix.length(),
                       prefix) == 0;
  } else {
    return false;
  }
}

static bool isRemote(const std::string& fname) {
  return fname.compare(0, hdfs_prefix.length(), hdfs_prefix) == 0;
}

std::string getHost(const std::string &fname) {
  size_t next_slash = fname.find('/', hdfs_prefix.length());
  std::string host = fname.substr(hdfs_prefix.length(),
                                  next_slash - hdfs_prefix.length());
  return host;
}

std::string getPath(const std::string &fname) {
  std::string new_path(fname);
  if(isRemote(fname)) {
    std::string host = getHost(fname);
    new_path = fname.substr(hdfs_prefix.length() + host.length(),
                     fname.length() - hdfs_prefix.length() - host.length());
  }
  return new_path;
}

class HDFSSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  hdfsFile file_;
  hdfsFS hdfs_fs_;

 public:
  HDFSSequentialFile(const std::string& filename,
                     hdfsFS hdfs_fs,
                     hdfsFile file)
      : filename_(filename), hdfs_fs_(hdfs_fs), file_(file) {
  }
  virtual ~HDFSSequentialFile() {
    hdfsCloseFile(hdfs_fs_, file_);
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    tSize r = hdfsRead(hdfs_fs_, file_, scratch, n);
    *result = Slice(scratch, r);
    if (r < n) {
      if ( r != -1) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    tOffset cur = hdfsTell(hdfs_fs_, file_);
    if(cur == -1) {
      return IOError(filename_, errno);
    }

    int r = hdfsSeek(hdfs_fs_, file_, cur + static_cast<tOffset>(n));
    if (r != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

class HDFSRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  hdfsFile file_;
  hdfsFS hdfs_fs_;

 public:
  HDFSRandomAccessFile(const std::string& filename,
                       hdfsFS hdfs_fs, hdfsFile file)
      : filename_(filename), hdfs_fs_(hdfs_fs), file_(file) {
  }
  virtual ~HDFSRandomAccessFile() {
    if (hdfs_fs_ != NULL && file_ != NULL)
        hdfsCloseFile(hdfs_fs_, file_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = hdfsPread(hdfs_fs_, file_,
                          static_cast<tOffset>(offset),
                          scratch, static_cast<tSize>(n));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class HDFSWritableFile : public WritableFile {
 private:
  std::string filename_;
  hdfsFile file_;
  hdfsFS hdfs_fs_;

 public:
  HDFSWritableFile(const std::string& filename,
                   hdfsFS hdfs_fs, hdfsFile file)
      : filename_(filename),
        hdfs_fs_(hdfs_fs),
        file_(file){
  }

  ~HDFSWritableFile() {
    if (file_ != NULL) {
      HDFSWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t data_size = data.size();
    if (hdfsWrite(hdfs_fs_, file_, src, data_size) < 0) {
      return IOError(filename_, errno);
    }

    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    if (hdfsCloseFile(hdfs_fs_, file_) < 0) {
      s = IOError(filename_, errno);
    }
    file_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
    if(hdfsHFlush(hdfs_fs_, file_) < 0) {
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  uint64_t file_offset_;  // Offset of base_ in file

 public:
  PosixWritableFile(const std::string& fname, int fd)
      : filename_(fname),
        fd_(fd),
        file_offset_(0) {
  }

  ~PosixWritableFile() {
    if (fd_ >= 0) {
      PosixWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t data_size = data.size();
    if (pwrite(fd_, src, data_size, file_offset_) < 0) {
      return IOError(filename_, errno);
    }
    file_offset_ += data_size;

    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    fd_ = -1;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (fsync(fd_) < 0) {
        s = IOError(filename_, errno);
    }

    return s;
  }
};


static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
//  return fcntl(fd, F_SETLK, &f);
  return 0;
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
};

class HDFSEnv : public Env {

private:
  hdfsFS hdfs_primary_fs_;

public:
  HDFSEnv(const char* host="default", tPort port=8020);
  virtual ~HDFSEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    hdfsDisconnect(hdfs_primary_fs_);
    exit(1);
  }

  //Check if the file should be accessed from HDFS
  bool onHDFS(const std::string &fname) {
    bool on_hdfs = false;
    const std::string ext_dat = ".dat";
    const std::string ext_sst = ".sst";
    const std::string ext_log = ".log";
    const std::string prefix_des = "MANIFEST";

    on_hdfs = stringEndsWith(fname, ext_sst) ||
              stringEndsWith(fname, ext_dat) ||
              stringEndsWith(fname, ext_log) ||
              lastComponentStartsWith(fname, prefix_des);

    return on_hdfs;
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    if(onHDFS(fname)) {
      std::string filename = getPath(fname);
      hdfsFile f = hdfsOpenFile(hdfs_primary_fs_,
                                filename.c_str(), O_RDONLY, 0, 0, 0);
      if (f == NULL) {
        *result = NULL;
        return IOError(fname, errno);
      } else {
        *result = new HDFSSequentialFile(fname, hdfs_primary_fs_, f);
        return Status::OK();
      }
    } else {
      FILE* f = fopen(fname.c_str(), "r");
      if (f == NULL) {
        *result = NULL;
        return IOError(fname, errno);
      } else {
        *result = new PosixSequentialFile(fname, f);
        return Status::OK();
      }
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;

    if(onHDFS(fname)) {
      std::string filename = getPath(fname);
      hdfsFile new_file = NULL;
      for (int tries = 0; tries < 3; ++tries) {
        new_file = hdfsOpenFile(hdfs_primary_fs_, filename.c_str(),
                                O_RDONLY, 0, 0, 0);
        if (new_file != NULL) {
          break;
        }
      }

      if (new_file == NULL) {
        s = IOError(fname, errno);
      } else {
        *result = new HDFSRandomAccessFile(fname, hdfs_primary_fs_, new_file);
      }
    } else {
      int fd = open(fname.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(fname, errno);
      } else {
        *result = new PosixRandomAccessFile(fname, fd);
      }
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;

    if(onHDFS(fname)) {
      std::string filename = getPath(fname);
      int exists = hdfsExists(hdfs_primary_fs_, filename.c_str());
      hdfsFile new_file;
      new_file = hdfsOpenFile(hdfs_primary_fs_,
                              filename.c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
      if (new_file == NULL) {
        *result = NULL;
        s = IOError(fname, errno);
      } else {
        *result = new HDFSWritableFile(fname, hdfs_primary_fs_, new_file);
      }
    } else {
      const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
      if (fd < 0) {
        *result = NULL;
        s = IOError(fname, errno);
      } else {
        *result = new PosixWritableFile(fname, fd);
      }
    }
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    if(onHDFS(fname)) {
      std::string filename = getPath(fname);
      return (hdfsExists(hdfs_primary_fs_, filename.c_str()) == 0);
    }
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();

    int num_entries = 0;
    hdfsFileInfo* entries = hdfsListDirectory(hdfs_primary_fs_,
                                              dir.c_str(), &num_entries);
    if(entries == NULL) {
      //return IOError(dir, "Not found entries");
    } else {
      for (int i = 0; i < num_entries; ++i) {
        char* last_component = strrchr(entries[i].mName, '/');
        if (last_component != NULL)
          result->push_back(last_component+1);
      }
      hdfsFreeFileInfo(entries, num_entries);
    }

    DIR* d = opendir(dir.c_str());
    if (d != NULL) {
      struct dirent* entry;
      while ((entry = readdir(d)) != NULL) {
        result->push_back(entry->d_name);
      }
      closedir(d);
    }
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if(onHDFS(fname)) {
      std::string filename = getPath(fname);
      if (hdfsDelete(hdfs_primary_fs_, filename.c_str(), 0) != 0) {
        result = IOError(fname, errno);
      }
    } else {
      if (unlink(fname.c_str()) != 0) {
        result = IOError(fname, errno);
      }
    }
    return result;
  };

  virtual Status CreateDir(const std::string& name) {

    Status result;
    if (hdfsCreateDirectory(hdfs_primary_fs_, name.c_str()) != 0) {
      result = IOError(name, errno);
    } else {
      hdfsChmod(hdfs_primary_fs_, name.c_str(), 0755);
    }
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (hdfsDelete(hdfs_primary_fs_, name.c_str(), 1) != 0) {
      result = IOError(name, errno);
    }
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    if(onHDFS(fname)) {
      hdfsFileInfo* fileInfo = hdfsGetPathInfo(hdfs_primary_fs_,
                                               fname.c_str());
      if (fileInfo != NULL) {
        *size = fileInfo->mSize;
        hdfsFreeFileInfo(fileInfo, 1);
      } else {
        s = IOError(fname, errno);
      }
    } else {
      struct stat sbuf;
      if (stat(fname.c_str(), &sbuf) != 0) {
        *size = 0;
        s = IOError(fname, errno);
      } else {
        *size = sbuf.st_size;
      }
    }
    return s;
  }

  virtual Status CopyFile(const std::string& src, const std::string& target) {
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Copy File src:%s target:%s not implemented\n",
              src.c_str(), target.c_str());
      exit(0);
    }

    Status result;
    int r_fd, w_fd;
    if ((r_fd = open(src.c_str(), O_RDONLY)) < 0) {
      result = IOError(src, errno);
      return result;
    }
    if ((w_fd = open(target.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
      result = IOError(target, errno);
      return result;
    }

#if (defined(OS_LINUX))
    int p[2];
    pipe(p);
    while(splice(p[0], 0, w_fd, 0, splice(r_fd, 0, p[1], 0, 4096, 0), 0) > 0);
#else
    char buf[4096];
    ssize_t len;
    while ((len = read(r_fd, buf, 4096)) > 0) {
      write(w_fd, buf, len);
    }
#endif

    close(r_fd);
    close(w_fd);

    return result;
  }

  virtual Status SymlinkFile(const std::string& src, const std::string& target)
  {
    if (!onHDFS(src) && !onHDFS(target)) {
      Status s;
      if (symlink(src.c_str(), target.c_str()) != 0) {
        s = IOError(src, errno);
      }
      return s;
    } else {
      return Status::IOError(src, "Cannot symlink across file systems");
    }
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status s;
    if(onHDFS(src) && onHDFS(target)) {
      if (hdfsRename(hdfs_primary_fs_, src.c_str(), target.c_str()) < 0) {
        s = IOError(src, errno);
      }
    } else if (!onHDFS(src) && !onHDFS(target)) {
      if (rename(src.c_str(), target.c_str()) != 0) {
        s = IOError(src, errno);
      }
    } else {
      s = Status::IOError(src, "Cannot rename across file systems");
    }
    return s;
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Link File src:%s target:%s. Not supported\n",
              src.c_str(), target.c_str());
      exit(0);
    }
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    if(onHDFS(fname)) {
      fprintf(stdout, "HDFS: Lock File %s. Not supported\n", fname.c_str());
      exit(0);
    }
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &HDFSEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    struct timespec req, rem;
    req.tv_sec = micros / 1000000;
    req.tv_nsec = (micros % 1000000) * 1000;
    nanosleep(&req, &rem);
    //usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      exit(1);
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<HDFSEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;
};

HDFSEnv::HDFSEnv(const char* host, tPort port) : page_size_(getpagesize()),
                                                 started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));

  hdfs_primary_fs_ = hdfsConnect(host, port);
}

void HDFSEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &HDFSEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void HDFSEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void HDFSEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;

static const char* server_ip;
static tPort server_port;
static Env* hdfs_env;
static void InitHDFSEnv() {
  hdfs_env = new HDFSEnv(server_ip, server_port);
}

Env* Env::HDFSEnv(const char* ip, int port) {
  server_ip = ip;
  server_port = (tPort) port;
  pthread_once(&once, InitHDFSEnv);
  return hdfs_env;
}

}  // namespace leveldb

#endif // if defined HDFS
#endif // if defined OS_LINUX
