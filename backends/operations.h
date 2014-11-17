
#ifndef OPERATIONS_H
#define OPERATIONS_H

#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "leveldb/c.h"
#include "common/giga_index.h"

/*
 * File/Directory permission bits
 * */
#define DEFAULT_MODE    (S_IRWXU | S_IRWXG | S_IRWXO )

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0

#define INODE_COUNT_KEY     "inode_count"
#define INODE_COUNT_KEY_LEN 11
#define INODE_COUNT_VAL_FORMAT  "%020llu"
#define INODE_COUNT_VAL_LEN 21

/*
 * Operations for local file system as the backend.
 */
int local_getattr(const char *path, struct stat *stbuf);
int local_symlink(const char *path, const char *link);
int local_readlink(const char *path, char *link, size_t size);
int local_open(const char *path, int flags, int *fd_to_return);
int local_mknod(const char *path, mode_t mode, dev_t dev);
int local_mkdir(const char *path, mode_t mode);

/*
 * Operations for MDB
 */

typedef enum MetaDB_obj_type {
    OBJ_DIR,
    OBJ_FILE,
    OBJ_MKNOD,
    OBJ_SLINK,
    OBJ_HLINK
} metadb_obj_type_t;

typedef uint64_t mdb_seq_num_t;
typedef uint32_t readdir_rec_len_t;
typedef uint64_t metadb_inode_t;

typedef struct MetaDB_key {
    metadb_inode_t parent_id;
    long int partition_id;
    char name_hash[HASH_LEN];
} metadb_key_t;

typedef struct {
    struct stat statbuf;
    int state;
    size_t objname_len;
    char* objname;
    size_t realpath_len;
    char* realpath;
} metadb_val_header_t;

typedef struct {
    char data;
} metadb_val_file_t;

typedef struct giga_mapping_t metadb_val_dir_t;

typedef struct {
    size_t size;
    char* value;
} metadb_val_t;

typedef struct Extract {
    metadb_inode_t dir_id;
    int old_partition_id;
    int new_partition_id;
    char dir_with_new_partition[PATH_MAX];

    leveldb_t* extract_db;
    int in_extraction;
} metadb_extract_t;

typedef struct {
    const char* buf;
    size_t buf_len;
    size_t num_ent;
    size_t offset;
    size_t cur_ent;
} metadb_readdir_iterator_t;

/*
 * LevelDB specific definitions
 */
struct MetaDB {
    leveldb_t* db;              // DB instance
    leveldb_comparator_t* cmp;  // Compartor object that allows user-defined
                                // object comparions functions.
    leveldb_cache_t* cache;     // Cache object: If set, individual blocks 
                                // (of levelDB files) are cached using LRU.
    leveldb_env_t* env;
    leveldb_options_t* options;
    leveldb_readoptions_t*  lookup_options;
    leveldb_readoptions_t*  scan_options;
    leveldb_writeoptions_t* insert_options;
    leveldb_writeoptions_t* ext_insert_options;
    leveldb_writeoptions_t* sync_insert_options;

    metadb_extract_t*  extraction;

    pthread_rwlock_t    rwlock_extract;
    pthread_mutex_t     mtx_bulkload;
    pthread_mutex_t     mtx_extract;
    pthread_mutex_t     mtx_leveldb;

    FILE* logfile;
    int use_hdfs;
    int server_id;
    metadb_inode_t inode_count;
};

typedef int (*update_func_t)(metadb_val_t* mval, void* arg1);

metadb_readdir_iterator_t* metadb_create_readdir_iterator(const char* buf,
        size_t buf_len, size_t num_entries);
void metadb_destroy_readdir_iterator(metadb_readdir_iterator_t *iter);
void metadb_readdir_iter_begin(metadb_readdir_iterator_t *iter);
int metadb_readdir_iter_valid(metadb_readdir_iterator_t *iter);
void metadb_readdir_iter_next(metadb_readdir_iterator_t *iter);
const char* metadb_readdir_iter_get_objname(metadb_readdir_iterator_t *iter,
                                      size_t *name_len);
const char* metadb_readdir_iter_get_realpath(metadb_readdir_iterator_t *iter,
                                       size_t *path_len);
const struct stat* metadb_readdir_iter_get_stat(metadb_readdir_iterator_t *iter);

char* metadb_get_metric(struct MetaDB *mdb);

int metadb_get_next_inode_count(struct MetaDB *mdb);
int metadb_get_next_inode_batch(struct MetaDB *mdb, int bulk_size);

// Returns "0" if a new LDB is created successfully, "1" if an existing LDB is
// opened successfully, and "-1" on error.
int metadb_init(struct MetaDB *mdb, const char *mdb_name,
                const char* serverIP, int serverPort,
                int server_id);

// Initialize MetaDB only to extract its records
int metadb_readonly_init(struct MetaDB *mdb, const char *mdb_name,
                         const char* serverIP, int serverPort,
                         int server_id);

// Client-side Meta DB initialization
int metadb_cliside_init(struct MetaDB *mdb, const char *mdb_name,
                        const char* serverIP, int serverPort,
                        int server_id);

int metadb_close(struct MetaDB *mdb);

// Close a MetaDB opened only for extraction
int metadb_readonly_close(struct MetaDB *mdb);
// Client-side Meta DB closing
int metadb_cliside_close(struct MetaDB *mdb);

int metadb_valid(struct MetaDB *mdb);

// Returns "0" if MDB creates the file successfully, otherwise "-1" on error.
int metadb_create(struct MetaDB *mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *objname,
                  const char *realpath);

// Returns "0" if MDB creates the directory successfully, otherwise "-1" on error.
int metadb_create_dir(struct MetaDB *mdb,
                      const metadb_inode_t dir_id,
                      const int partition_id,
                      const char *objname,
                      const metadb_inode_t inode_id,
                      const int server_id,
                      metadb_val_dir_t* dir_mapping);

// Returns "0" if MDB creates the entry successfully, otherwise "-1" on error.
int metadb_create_entry(struct MetaDB *mdb,
                        const metadb_inode_t dir_id, const int partition_id,
                        const char *path,  const struct stat *statbuf,
                        const char *realpath,
                        const size_t data_len, const char *data);

int metadb_insert_inode(struct MetaDB *mdb,
                        const metadb_inode_t dir_id, const int partition_id,
                        const char *path,
                        char* data, int size);

// Returns "0" if MDB removes the file successfully, otherwise "-1" on error.
int metadb_remove(struct MetaDB *mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *objname);

// Returns "0" if MDB get the file stat successfully,
// otherwise "-ENOENT" when no file is found.
int metadb_lookup(struct MetaDB *mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *objname,
                  struct stat *stbuf,
                  int* state);

// Returns "0" if MDB get directory entries successfully,
// otherwise "-ENOENT" when no file is found.
int metadb_readdir(struct MetaDB *mdb,
                   const metadb_inode_t dir_id,
                   int *partition_id,
                   const char* start_key,
                   char* buf,
                   const size_t buf_len,
                   int *num_entries,
                   char* end_key,
                   unsigned char* more_entries_flag);

int metadb_readdirplus(struct MetaDB *mdb,
                       const metadb_inode_t dir_id,
                       int* partition_id,
                       const char* start_key,
                       const size_t entry_limit,
                       char** names,
                       struct stat ** entries,
                       int *num_entries,
                       char* end_key,
                       unsigned char* more_entries_flag);

// Returns "0" if MDB extract entries successfully,
// otherwise "-ENOENT" when the target directory is found.
int metadb_extract_do(struct MetaDB *mdb,
                      const metadb_inode_t dir_id,
                      const int old_partition_id,
                      const int new_partition_id,
                      const char* dir_with_new_partition,
                      uint64_t *min_sequence_number,
                      uint64_t *max_sequence_number);

// Returns "0" if MDB clean extraction successfully,
// otherwise negative integer on error.
int metadb_extract_clean(struct MetaDB *mdb);

// Returns "0" if MDB bulkinsert entries successfully,
// otherwise negative integer on error.
int metadb_bulkinsert(struct MetaDB *mdb,
                      const char* dir_with_new_partition,
                      uint64_t min_sequence_number,
                      uint64_t max_sequence_number);

int metadb_read_bitmap(struct MetaDB *mdb,
                       const metadb_inode_t dir_id,
                       const int partition_id,
                       const char* objname,
                       struct giga_mapping_t* map_val);

int metadb_write_bitmap(struct MetaDB *mdb,
                        const metadb_inode_t dir_id,
                        const int partition_id,
                        const char* objname,
                        const struct giga_mapping_t* map_val);

int metadb_get_val(struct MetaDB *mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   const char* objname,
                   char* *buf, int* buf_len);


int metadb_get_file(struct MetaDB *mdb,
                    const metadb_inode_t dir_id,
                    const int partition_id,
                    const char* objname,
                    int* state, char* buf, int* buf_len);

int metadb_get_state(struct MetaDB *mdb,
                     const metadb_inode_t dir_id,
                     const int partition_id,
                     const char* objname,
                     int* state, char* link, int* link_len);

int metadb_write_file(struct MetaDB *mdb,
                      const metadb_inode_t dir_id,
                      const int partition_id,
                      const char* objname,
                      const char* buf, int buf_len, int offset);

int metadb_write_link(struct MetaDB *mdb,
                      const metadb_inode_t dir_id,
                      const int partition_id,
                      const char* objname,
                      const char* pathname);

int metadb_setattr(struct MetaDB *mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   const char* path,
                   const struct stat* stbuf);

int metadb_chmod(struct MetaDB *mdb,
                 const metadb_inode_t dir_id,
                 const int partition_id,
                 const char* path,
                 mode_t new_mode);

#endif /* OPERATIONS_H */
