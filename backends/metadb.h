// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_BACKEND_METADB_H_
#define _INDEXFS_BACKEND_METADB_H_

#include <vector>
#include <string>

extern "C" {
#include "operations.h"
}

#include "common/common.h"

namespace indexfs {

class MetadataBackend {
protected:
  MetaDB mdb;

public:

  int Init(const std::string& dbname,
           const char* hdfsIP,
           int hdfsPort,
           int serverID);

  int Create(const TINumber dir_id,
             const int partition_id,
             const std::string &objname,
             const std::string &realpath);

  // Returns "0" if MDB creates the directory successfully, otherwise "-1" on error.
  int Mkdir(const TINumber dir_id,
            const int partition_id,
            const std::string &objname,
            const TINumber object_id,
            const int server_id,
            const int num_servers);

  int CreateEntry(const TINumber dir_id,
                  const int partition_id,
                  const std::string &objname,
                  const StatInfo &info,
                  const std::string &realpath,
                  const std::string &data);

  // Returns "0" if MDB removes the file successfully, otherwise "-1" on error.
  int Remove(const TINumber dir_id,
             const int partition_id,
             const std::string &objname);

  // Returns "0" if MDB get the file stat successfully,
  // otherwise "-ENOENT" when no file is found.
  int Getattr(const TINumber dir_id,
              const int partition_id,
              const std::string &objname,
              StatInfo *info);

  // Returns "0" if MDB get directory entries successfully,
  // otherwise "-ENOENT" when no file is found.
  int Readdir(const TINumber dir_id,
              const int partition_id,
              const std::string &start_key,
              int entry_limit,
              std::vector<std::string> *buf,
              std::string *end_key,
              unsigned char *more_entries_flag);

  // Returns "0" if MDB get directory entries successfully,
  // otherwise "-ENOENT" when no file is found.
  int ReaddirPlus(const TINumber dir_id,
                  const int partition_id,
                  const std::string &start_key,
                  int entry_limit,
                  std::vector<std::string> *names,
                  std::vector<StatInfo> *entires,
                  std::string *end_key,
                  unsigned char *more_entries_flag);


  // Returns "0" if MDB extract entries successfully,
  // otherwise "-ENOENT" when the target directory is found.
  int Extract(const TINumber dir_id,
              const int old_partition_id,
              const int new_partition_id,
              const std::string &dir_with_new_partition,
              uint64_t *min_sequence_number,
              uint64_t *max_sequence_number);

  // Returns "0" if MDB clean extraction successfully,
  // otherwise negative integer on error.
  int ExtractClean();

  // Returns "0" if MDB bulkinsert entries successfully,
  // otherwise negative integer on error.
  int BulkInsert(const std::string &dir_with_new_partition,
                 uint64_t min_sequence_number,
                 uint64_t max_sequence_number);

  int ReadBitmap(const TINumber dir_id,
                 struct giga_mapping_t *map_val);

  int CreateBitmap(const TINumber dir_id,
                   struct giga_mapping_t &map_val,
                   const int server_id);

  int UpdateBitmap(const TINumber dir_id,
                   const struct giga_mapping_t &map_val);

  int Setattr(const TINumber dir_id,
              const int partition_id,
              const std::string &objname,
              const StatInfo &info);

  int Chmod(const TINumber dir_id,
            const int partition_id,
            const std::string &objname,
            mode_t new_mode);

  int OpenFile(const TINumber dir_id, const int partition_id,
               const std::string &objname,
               bool *is_embedded, int *data_len, char *data);

  int ReadFile(const TINumber dir_id, const int partition_id,
               const std::string &objname,
               const size_t offset, const size_t *data_len, const char *data);

  int WriteFile(const TINumber dir_id, const int partition_id,
                const std::string &objname,
                const size_t offset, size_t data_len, const char *data);

  int WriteLink(const TINumber dir_id, const int partition_id,
                const std::string &objname, const std::string &link);

  void Close() { metadb_close(&mdb); }

  TINumber NewInodeNumber();

  TINumber NewInodeBatch(int bulk_size);

};

class ClientMetadataBackend : public MetadataBackend {
public:

  std::string path_;

  int Init(const std::string& dbname,
             const char* hdfsIP,
             int hdfsPort,
             int serverID) {
    return metadb_cliside_init(
        &mdb, dbname.c_str(), hdfsIP, hdfsPort, serverID);
  };

  void Close() { metadb_cliside_close(&mdb); }

};

class ReadonlyMetadataBackend : public MetadataBackend {
public:

  int Init(const std::string& dbname,
             const char* hdfsIP,
             int hdfsPort,
             int serverID) {
    return metadb_readonly_init(
        &mdb, dbname.c_str(), hdfsIP, hdfsPort, serverID);
  };

  void Close() { metadb_readonly_close(&mdb); }

};

} // namespace indexfs

#endif /* _INDEXFS_BACKEND_METADB_H_ */
