// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "metadb.h"
#include "common/options.h"

namespace indexfs {

int MetadataBackend::Init(const std::string& dbname,
                          const char* hdfsIP,
                          int hdfsPort,
                          int serverID) {
  return metadb_init(&mdb, dbname.c_str(), hdfsIP, hdfsPort, serverID);
}

int MetadataBackend::Create(const TINumber dir_id,
                            const int partition_id,
                            const std::string &objname,
                            const std::string &realpath) {
  return metadb_create(&mdb, dir_id, partition_id,
                      objname.c_str(), realpath.c_str());
}

// Returns "0" if MDB creates the directory successfully, otherwise "-1" on error.
int MetadataBackend::Mkdir(const TINumber dir_id,
                           const int partition_id,
                           const std::string &objname,
                           const TINumber object_id,
                           const int server_id,
                           const int num_servers) {
  if (objname.size() == 0) {
    struct giga_mapping_t dir_mapping;
    giga_init_mapping(&dir_mapping, 0, object_id, server_id, num_servers);
    return metadb_create_dir(&mdb, dir_id, -1, NULL, object_id,
                             server_id, &dir_mapping);
  } else {
    return metadb_create_dir(&mdb, dir_id, partition_id, objname.c_str(),
                             object_id, server_id, NULL);
  }
}

int MetadataBackend::CreateEntry(const TINumber dir_id,
                                 const int partition_id,
                                 const std::string &objname,
                                 const StatInfo &info,
                                 const std::string &realpath,
                                 const std::string &data) {
  struct stat stbuf;
  stbuf.st_mode = info.mode;
  stbuf.st_uid = info.uid;
  stbuf.st_gid = info.gid;
  stbuf.st_size = info.size;
  stbuf.st_mtime = info.mtime;
  stbuf.st_ctime = info.ctime;
  stbuf.st_ino = info.id;
  metadb_create_entry(&mdb, dir_id, partition_id, objname.c_str(),
                      &stbuf, realpath.c_str(), data.size(), data.c_str());
}

// Returns "0" if MDB removes the file successfully, otherwise "-1" on error.
int MetadataBackend::Remove(const TINumber dir_id,
                            const int partition_id,
                            const std::string &objname) {
  return metadb_remove(&mdb, dir_id, partition_id, objname.c_str());
}

// Returns "0" if MDB get the file stat successfully,
// otherwise "-ENOENT" when no file is found.
int MetadataBackend::Getattr(const TINumber dir_id,
                             const int partition_id,
                             const std::string &objname,
                             StatInfo *info) {
  struct stat stbuf;
  int state;
  int ret = metadb_lookup(&mdb, dir_id, partition_id, objname.c_str(),
                          &stbuf, &state);
  if (ret == 0) {
    info->mode = stbuf.st_mode;
    info->uid = stbuf.st_uid;
    info->gid = stbuf.st_gid;
    info->size = stbuf.st_size;
    info->mtime = stbuf.st_mtime;
    info->ctime = stbuf.st_ctime;
    info->id = stbuf.st_ino;
    info->zeroth_server = stbuf.st_dev;
    info->is_embedded = (state == RPC_LEVELDB_FILE_IN_DB);
  }
  return ret;
}

// Returns "0" if MDB get directory entries successfully,
// otherwise "-ENOENT" when no file is found.
int MetadataBackend::Readdir(const TINumber dir_id,
                             const int partition_id,
                             const std::string &start_key,
                             const int entries_limit,
                             std::vector<std::string> *entries,
                             std::string *end_key,
                             unsigned char *more_entries_flag) {

  char buf[entries_limit * MAX_LEN];
  char end_key_cstr[MAX_LEN];
  size_t buf_len = (size_t) entries_limit * MAX_LEN;
  int num_entries;
  int tmp_partition_id = partition_id;
  const char* start_key_cstr = (start_key.size() > 0) ? start_key.c_str() : NULL;
  int ret = metadb_readdir(&mdb, dir_id, &tmp_partition_id, start_key_cstr,
                           buf, buf_len, &num_entries, end_key_cstr, more_entries_flag);
  if (ret != 0)
    return ret;

  char* buf_ptr = buf;
  for (int i = 0; i < num_entries; ++i) {
    readdir_rec_len_t rec_len = *((readdir_rec_len_t *) buf_ptr);
    entries->push_back(std::string(buf_ptr+sizeof(rec_len), rec_len));
    buf_ptr += sizeof(rec_len) + rec_len;
  }
  end_key->insert(0, end_key_cstr, HASH_LEN);

  return 0;
}

int MetadataBackend::ReaddirPlus(const TINumber dir_id,
                                 const int partition_id,
                                 const std::string &start_key,
                                 const int entries_limit,
                                 std::vector<std::string> *names,
                                 std::vector<StatInfo> *entries,
                                 std::string *end_key,
                                 unsigned char *more_entries_flag) {

  char* buf_names[entries_limit];
  struct stat* buf_entries[entries_limit];
  char end_key_cstr[MAX_LEN];
  int num_entries;
  int tmp_partition_id = partition_id;
  const char* start_key_cstr = (start_key.size() > 0) ? start_key.c_str() : NULL;
  int ret = metadb_readdirplus(&mdb, dir_id, &tmp_partition_id, start_key_cstr,
                               entries_limit, buf_names, buf_entries,
                               &num_entries, end_key_cstr, more_entries_flag);
  if (ret != 0)
    return ret;

  for (int i = 0; i < num_entries; ++i) {
    names->push_back(buf_names[i]);
    StatInfo info;
    struct stat* stbuf = buf_entries[i];
    info.mode = stbuf->st_mode;
    info.uid = stbuf->st_uid;
    info.gid = stbuf->st_gid;
    info.size = stbuf->st_size;
    info.mtime = stbuf->st_mtime;
    info.ctime = stbuf->st_ctime;
    info.id = stbuf->st_ino;
    info.zeroth_server = stbuf->st_dev;
    free(buf_names[i]);
    free(buf_entries[i]);
    entries->push_back(info);
  }
  end_key->insert(0, end_key_cstr, HASH_LEN);

  return 0;
}


// Returns "0" if MDB extract entries successfully,
// otherwise "-ENOENT" when the target directory is found.
int MetadataBackend::Extract(const TINumber dir_id,
                             const int old_partition_id,
                             const int new_partition_id,
                             const std::string &dir_with_new_partition,
                             uint64_t *min_sequence_number,
                             uint64_t *max_sequence_number) {
  return metadb_extract_do(&mdb, dir_id, old_partition_id, new_partition_id,
                           dir_with_new_partition.c_str(),
                           min_sequence_number, max_sequence_number);
}

// Returns "0" if MDB clean extraction successfully,
// otherwise negative integer on error.
int MetadataBackend::ExtractClean() {
  return metadb_extract_clean(&mdb);
}

// Returns "0" if MDB bulkinsert entries successfully,
// otherwise negative integer on error.
int MetadataBackend::BulkInsert(const std::string &dir_with_new_partition,
                                uint64_t min_sequence_number,
                                uint64_t max_sequence_number) {
  return metadb_bulkinsert(&mdb, dir_with_new_partition.c_str(),
                           min_sequence_number, max_sequence_number);
}

int MetadataBackend::ReadBitmap(const TINumber dir_id,
                                struct giga_mapping_t *map_val) {
  return metadb_read_bitmap(&mdb, dir_id, -1, NULL, map_val);
}

int MetadataBackend::CreateBitmap(const TINumber dir_id,
                                  struct giga_mapping_t &map_val,
                                  const int server_id) {

  return metadb_create_dir(&mdb, dir_id, -1,
                           NULL, dir_id, server_id, &map_val);
}

int MetadataBackend::UpdateBitmap(const TINumber dir_id,
                                 const struct giga_mapping_t &map_val) {
  return metadb_write_bitmap(&mdb, dir_id, -1, NULL, &map_val);
}


int MetadataBackend::Setattr(const TINumber dir_id,
                             const int partition_id,
                             const std::string &objname,
                             const StatInfo &info) {
  struct stat stbuf;
  stbuf.st_mode = info.mode;
  stbuf.st_uid = info.uid;
  stbuf.st_gid = info.gid;
  stbuf.st_size = info.size;
  stbuf.st_mtime = info.mtime;
  stbuf.st_ctime = info.ctime;
  stbuf.st_ino = info.id;
  return metadb_setattr(&mdb, dir_id, partition_id,
                        objname.c_str(), &stbuf);
}

int MetadataBackend::Chmod(const TINumber dir_id,
                           const int partition_id,
                           const std::string &objname,
                           mode_t new_mode) {
  return metadb_chmod(&mdb, dir_id, partition_id,
                      objname.c_str(), new_mode);
}


int MetadataBackend::OpenFile(const TINumber dir_id, const int partition_id,
                              const std::string &objname,
                              bool *is_embedded, int *data_len, char *data) {
  int state = 0;
  int ret = metadb_get_file(&mdb, dir_id, partition_id, objname.c_str(),
                            &state, data, data_len);
  *is_embedded = (state == RPC_LEVELDB_FILE_IN_DB);
  return ret;
}

int MetadataBackend::ReadFile(const TINumber dir_id, const int partition_id,
                              const std::string &objname, const size_t offset,
                              const size_t *data_len, const char *data) {
  return 0;
}

int MetadataBackend::WriteFile(const TINumber dir_id, const int partition_id,
                               const std::string &objname, const size_t offset,
                               size_t data_len, const char *data) {
  return metadb_write_file(&mdb, dir_id, partition_id, objname.c_str(),
                           data, data_len, offset);
}

int MetadataBackend::WriteLink(const TINumber dir_id, const int partition_id,
                         const std::string &objname, const std::string &link) {
  return metadb_write_link(&mdb, dir_id, partition_id,
                           objname.c_str(), link.c_str());
}


TINumber MetadataBackend::NewInodeNumber() {
  return metadb_get_next_inode_count(&mdb);
}

TINumber MetadataBackend::NewInodeBatch(int bulk_size) {
  return metadb_get_next_inode_batch(&mdb, bulk_size);
}

} // indexfs
