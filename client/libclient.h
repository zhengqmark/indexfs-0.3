// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_C_LIBCLIENT_H_
#define _INDEXFS_C_LIBCLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/stat.h>

struct info_t {
  int permission;
  int is_dir;
  int uid;
  int gid;
  int size;
  int atime;
  int ctime;
};

struct conf_t {
  const char* server_ip;
  const char* config_fn;
  const char* serverlist_fn;
};

// Initialize FS client.
// Note that this function can only be called once.
//
extern int IDX_Init(struct conf_t* config);

// Shutdown FS client.
// Should only be called after IDX_Init.
//
extern void IDX_Destroy();

// Create a file at the given path.
//
extern int IDX_Create(const char *path, mode_t mode);

// Create a file at the given path.
//
extern int IDX_Mknod(const char *path, mode_t mode);

// Make a new directory at the given path.
//
extern int IDX_Mkdir(const char *path, mode_t mode);

// Create a file at the give path.
// Create parent directories if necessary.
//
extern int IDX_RecMknod(const char *path, mode_t mode);

// Create a directory at the given path.
// Create parent directories if necessary.
//
extern int IDX_RecMkdir(const char *path, mode_t mode);

// Remove an empty directory identified by the given path.
//
extern int IDX_Rmdir(const char *path);

// Delete a file identified by the given path.
//
extern int IDX_Unlink(const char *path);

// Update the permission bits of the specified file or directory.
//
extern int IDX_Chmod(const char *path, mode_t mode);

// Check if the given file or directory exists.
//
extern int IDX_Access(const char* path);

// Check if the given directory exists with lightweight caching.
//
extern int IDX_AccessDir(const char* path);

// List entries in a directory.
//
extern int IDX_Readdir(const char *path, size_t *num_entries, char*** list);

extern int IDX_ReaddirPlus(const char *path);

// Retrieve various stats of the specified file or directory.
//
extern int IDX_GetAttr(const char *path, struct stat *buf);

extern int IDX_GetInfo(const char *path, struct info_t *buf);

// Open a file at the specified path and return its file descriptor.
//
extern int IDX_Open(const char *path, int flags, int *fd);

// Close the file associated with the given file descriptor.
//
extern int IDX_Close(int fd);

// Request a synchronization on the given file.
//
extern int IDX_Fsync(int fd);

// Perform an un-buffered read on the given file.
//
extern int IDX_Read(int fd, void *buf, size_t size);

// Perform an un-buffered write on the given file.
//
extern int IDX_Write(int fd, const void *buf, size_t size);

// Perform an un-buffered read on the given file at the given offset.
//
extern int IDX_Pread(int fd, void *buf, off_t offset, size_t size);

// Perform an un-buffered write on the given file at the given offset.
//
extern int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif /* _INDEXFS_C_LIBCLIENT_H_ */
