// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <unistd.h>

#include "io_task.h"
#include <gflags/gflags.h>

#define IDXFS_TREETEST_STATFILE

namespace indexfs { namespace mpi {

DEFINE_int32(dirs,
    0, "Total number of directories to create");
DEFINE_int32(files,
    0, "Total number of files to create");
DEFINE_bool(share_dirs,
    false, "Set to enable clients to create files in each other's directories");
DEFINE_string(prefix,
    "prefix", "The prefix for each directory entry");

namespace {

class TreeTest: public IOTask {

  static inline
  void MakeDirectory(IOClient* IO, IOListener* L, int dno) {
    Status s = IO->MakeDirectory(dno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("mkdir");
      }
      throw IOError(dno, "mkdir", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("mkdir");
    }
  }

  static inline
  void CreateFile(IOClient* IO, IOListener* L, int dno, int fno) {
    Status s = IO->NewFile(dno, fno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("mknod");
      }
      throw IOError(dno, fno, "mknod", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("mknod");
    }
  }

  static inline
  void GetAttr(IOClient* IO, IOListener* L, int dno, int fno) {
    Status s = IO->GetAttr(dno, fno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("getattr");
      }
      throw IOError(dno, fno, "getattr", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("getattr");
    }
  }

  static inline
  void SyncDirectory(IOClient* IO, IOListener* L, int dno) {
    Status s = IO->SyncDirectory(dno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("fsyncdir");
      }
      throw IOError(dno, "fsyncdir", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("fsyncdir");
    }
  }

  int PrintSettings() {
    return printf("Test Settings:\n"
      "  total dirs to create -> %d\n"
      "  total files to create -> %d\n"
      "  total processes -> %d\n"
      "  share_dirs -> %s\n"
      "  backend_fs -> %s\n"
      "  bulk_insert -> %s\n"
      "  ignore_errors -> %s\n"
      "  log_file -> %s\n"
      "  run_id -> %s\n",
      num_dirs_,
      num_files_,
      comm_sz_,
      GetBoolString(FLAGS_share_dirs),
      FLAGS_fs.c_str(),
      GetBoolString(FLAGS_bulk_insert),
      GetBoolString(FLAGS_ignore_errors),
      FLAGS_log_file.c_str(),
      FLAGS_run_id.c_str());
  }

  int num_dirs_; // Total number of directories to create
  int num_files_; // Total number of files to create

 public:

  TreeTest(int my_rank, int comm_sz)
    : IOTask(my_rank, comm_sz)
    , num_dirs_(FLAGS_dirs), num_files_(FLAGS_files) {
  }

  virtual void Prepare() {
    Status s = IO_->Init();
    if (!s.ok()) {
      throw IOError("init", s.ToString());
    }
    IOMeasurements::EnableMonitoring(IO_, false);
    if (FLAGS_share_dirs && FLAGS_bulk_insert) {
      for (int i = 0; i < num_dirs_; i++) {
        try {
          MakeDirectory(IO_, listener_, i);
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
    } else {
      for (int i = my_rank_; i < num_dirs_; i += comm_sz_) {
        try {
          MakeDirectory(IO_, listener_, i);
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
    }
    IOMeasurements::EnableMonitoring(IO_, true);
  }

  virtual void Run() {
    IOMeasurements::Reset(IO_);
    for (int i = my_rank_; i < num_files_; i += comm_sz_) {
      int d = rand() % num_dirs_;
      if (!FLAGS_share_dirs) {
        d = my_rank_ + d - (d % comm_sz_);
        if (d >= num_dirs_)
          d = my_rank_;
      }
      try {
        CreateFile(IO_, listener_, d, i);
      } catch (IOError &err) {
        if (!FLAGS_ignore_errors)
          throw err;
      }
    }
    fprintf(LOG_, "== Main Phase Performance Data ==\n\n");
    IOMeasurements::PrintMeasurements(IO_, LOG_);
  }

  virtual void Clean() {
#ifdef IDXFS_TREETEST_STATFILE
    if (num_dirs_ == 1 && FLAGS_share_dirs) {
      IOMeasurements::Reset(IO_);
      for (int i = my_rank_; i < num_files_; i += comm_sz_) {
        int f = rand() % num_files_;
        try {
          GetAttr(IO_, listener_, 0, f);
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
      fprintf(LOG_, "== Clean Phase Performance Data ==\n\n");
      IOMeasurements::PrintMeasurements(IO_, LOG_);
    }
#else
    /**************************************************
    if (FLAGS_bulk_insert) {
      IOMeasurements::Reset(IO_);
      if (FLAGS_share_dirs) {
        for (int i = 0; i < num_dirs_; i++) {
          SyncDirectory(IO_, listener_, i);
        }
      } else {
        for (int i = my_rank_; i < num_dirs_; i += comm_sz_) {
          SyncDirectory(IO_, listener_, i);
        }
      }
      fprintf(LOG_, "== Clean Phase Performance Data ==\n\n");
      IOMeasurements::PrintMeasurements(IO_, LOG_);
    }
    ****************************************************/
#endif
  }

  virtual bool CheckPrecondition() {
    if (IO_ == NULL || LOG_ == NULL) {
      return false; // err has already been printed elsewhere
    }
    if (num_dirs_ <= 0) {
      my_rank_ == 0 ? fprintf(stderr, "%s! (%s)\n",
        "fail to specify the total number of directories to create",
        "use --dirs=xx to specify") : 0;
      return false;
    }
    if (num_files_ <= 0) {
      my_rank_ == 0 ? fprintf(stderr, "%s! (%s)\n",
        "fail to specify the total number of files to create",
        "use --files=xx to specify") : 0;
      return false;
    }
    if (!FLAGS_share_dirs) {
      if (num_dirs_ < comm_sz_) {
        my_rank_ == 0 ? fprintf(stderr, "%s and %s! (%s)\n",
          "number of directories is less than the number of processes",
          "share_dirs is not enabled",
          "use --share_dirs to enable") : 0;
        return false;
      }
    }
    if (num_files_ < num_dirs_) {
      my_rank_ == 0 ? fprintf(stderr, "warning: %s\n",
        "number of files to create is less than the number of directories to create") : 0;
    }
    my_rank_ == 0 ? PrintSettings() : 0;
    // All will check, yet only the zeroth process will do the printing
    return true;
  }
};

} /* anonymous namespace */

IOTask* IOTaskFactory::GetTreeTestTask(int my_rank, int comm_sz) {
  return new TreeTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
