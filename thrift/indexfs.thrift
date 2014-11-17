namespace cpp indexfs
namespace java edu.cmu.pdl.indexfs.rpc

typedef i16 TNumServer
typedef i64 TInodeID

struct StatInfo {
  1: required i32 mode
  2: required i16 uid
  3: required i16 gid
  4: required i64 size
  5: required i64 mtime
  6: required i64 ctime
  7: required TInodeID id
  8: required TNumServer zeroth_server
  9: required bool is_embedded
  10: required i64 lease_time
}

struct AccessInfo {
  1: required TInodeID id
  2: required TNumServer zeroth_server
  3: required i64 lease_time
}

struct GigaBitmap {
  1: required TInodeID id;
  2: required string bitmap
  3: required i16 curr_radix
  4: required TNumServer zeroth_server
  5: required TNumServer num_servers
}

struct ScanResult {
  1: required list<string> entries
  2: required string end_key
  3: required i16 end_partition
  4: required i16 more_entries
  5: required GigaBitmap mapping;
}

struct ScanPlusResult {
  1: required list<string> names
  2: required list<StatInfo> entries
  3: required string end_key
  4: required i16 end_partition
  5: required i16 more_entries
  6: required GigaBitmap mapping;
}

struct OpenResult {
  1: required bool is_embedded
  2: required string data
}

struct ReadResult {
  1: required bool is_embedded
  2: required string data
}

struct WriteResult {
  1: required bool is_embedded
  2: required string link
  3: required string data
}

struct LeaseInfo {
  1: i64 timeout
  2: TInodeID next_inode
  3: TNumServer next_zeroth_server
  4: i32 max_dirs
}

exception ServerRedirectionException {
  1: required GigaBitmap redirect
}

exception FileNotFoundException {
  1: required string message
}

exception FileAlreadyExistException {
  1: required string message
}

exception ServerNotFound {
  1: required string message
}

exception FileNotInSameServer {
  1: required string message
}

exception NotDirectoryException {
  1: required string message
}

exception IOError {
  1: required string message
}

exception ServerInternalError {
  1: required string message
}

exception IllegalPath {
  1: required string message
}

exception ParentPathNotFound {
  1: required string path
}

exception NotADirectory {
  1: required string path
}

exception NotAFile {
}

exception NoSuchFileOrDirectory {
}

exception FileAlreadyExists {
}

service MetadataService {

  bool InitRPC()
    throws (1: ServerNotFound e)

  StatInfo Getattr(1: TInodeID dir_id, 2: string path, 3: i32 lease_time)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  AccessInfo Access(1: TInodeID dir_id, 2: string path, 3: i32 lease_time)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF, 4: NotDirectoryException eD)

  void Mknod(1: TInodeID dir_id, 2: string path, 3: i16 permission)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF, 4: FileNotFoundException eNF)

  void Mkdir(1: TInodeID dir_id, 2: string path, 3: i16 permission,
             4: i16 hint_server)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF, 4: FileNotFoundException eNF)

  void CreateEntry(1: TInodeID dir_id, 2: string path, 3: StatInfo info,
                   4: string link, 5: string data)
     throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF, 4: FileNotFoundException eNF)

  LeaseInfo CreateNamespace(1: TInodeID dir_id, 2: string path,
                            3: i16 permission)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF, 4: FileNotFoundException eNF)

  void CloseNamespace(1: TInodeID dir_id)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF, 4: FileNotFoundException eNF)

  void CreateZeroth(1: TInodeID dir_id)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileAlreadyExistException eF)

  void Chmod(1: TInodeID dir_id, 2: string path, 3: i16 permission)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  void Remove(1: TInodeID dir_id, 2: string path)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  void Rename(1: TInodeID src_id, 2: string src_path,
              3: TInodeID dst_id, 4: string dst_path)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF, 4: FileNotInSameServer eSS)

  ScanResult Readdir(1: TInodeID dir_id, 2: i64 partition, 3: string start_key,
                     4: i16 max_num_entries)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  ScanPlusResult ReaddirPlus(1: TInodeID dir_id, 2: i64 partition, 3: string start_key,
                             4: i16 max_num_entries)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  GigaBitmap ReadBitmap(1: TInodeID dir_id)
    throws (1: ServerNotFound eS, 2: FileNotFoundException eF)

  void UpdateBitmap(1: TInodeID dir_id, 2: GigaBitmap bitmap)
    throws (1: ServerNotFound eS, 2: FileNotFoundException eF)

  OpenResult OpenFile(1: TInodeID dir_id, 2: string path, 3: i16 mode,
                      4: i16 auth)
      throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  ReadResult Read(1: TInodeID dir_id, 2: string path, 3: i32 offset,
                  4: i32 size)
      throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF, 4: IOError eI)

  WriteResult Write(1: TInodeID dir_id, 2: string path, 3: string data,
                    4: i32 offset)
      throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF, 4: IOError eI)

  void CloseFile(1: TInodeID dir_id, 2: string path, 3: i16 mode)
      throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF, 4: IOError eI)

  void InsertSplit(1: TInodeID dir_id, 2: i16 parent_index, 3: i16 child_index,
                   4: string path_split_files, 5: GigaBitmap bitmap,
                   6: i64 min_seq, 7: i64 max_seq, 8: i64 num_entries)
    throws (1: ServerRedirectionException r, 2: ServerNotFound eS,
            3: FileNotFoundException eF)

  StatInfo IGetattr(1: string path)
    throws (1: IllegalPath bad_path,
            2: NoSuchFileOrDirectory not_found,
            3: ParentPathNotFound no_parent,
            4: NotADirectory not_a_dir,
            5: IOError io_error,
            6: ServerInternalError srv_error)

  void IMknod(1: string path, 2: i16 permission)
    throws (1: IllegalPath bad_path,
            2: FileAlreadyExists file_exists,
            3: ParentPathNotFound no_parent,
            4: NotADirectory not_a_dir,
            5: IOError io_error,
            6: ServerInternalError srv_error)

  void IMkdir(1: string path, 2: i16 permission)
    throws (1: IllegalPath bad_path,
            2: FileAlreadyExists file_exists,
            3: ParentPathNotFound no_parent,
            4: NotADirectory not_a_dir,
            5: IOError io_error,
            6: ServerInternalError srv_error)

  void IChmod(1: string path, 2: i16 permission)
    throws (1: IllegalPath bad_path,
            2: NoSuchFileOrDirectory not_found,
            3: ParentPathNotFound no_parent,
            4: NotADirectory not_a_dir,
            5: IOError io_error,
            6: ServerInternalError srv_error)

  void IChfmod(1: string path, 2: i16 permission)
    throws (1: IllegalPath bad_path,
            2: NoSuchFileOrDirectory not_found,
            3: NotAFile not_a_file,
            4: ParentPathNotFound no_parent,
            5: NotADirectory not_a_dir,
            6: IOError io_error,
            7: ServerInternalError srv_error)

  void IRemove(1: string path)
    throws (1: IllegalPath bad_path,
            2: NoSuchFileOrDirectory not_found,
            3: ParentPathNotFound no_parent,
            4: NotADirectory not_a_dir,
            5: IOError io_error,
            6: ServerInternalError srv_error)

  void IRename(1: string src_path 2: string dst_path)
    throws (1: IllegalPath bad_path,
            2: NoSuchFileOrDirectory not_found,
            3: FileAlreadyExists file_exists,
            4: ParentPathNotFound no_parent,
            5: NotADirectory not_a_dir,
            6: IOError io_error,
            7: ServerInternalError srv_error)

}
