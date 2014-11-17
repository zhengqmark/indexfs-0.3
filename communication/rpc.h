// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMM_RPC_H_
#define _INDEXFS_COMM_RPC_H_

#include "common/common.h"
#include "common/config.h"
#include "common/logging.h"

#include "thrift/MetadataService.h"

namespace indexfs {

class RPC {
 public:

  Status Init();
  Status Shutdown();

  static RPC* CreateRPC(Config* conf) {
    return new RPC(conf);
  }

  virtual ~RPC();
  Mutex* GetMutex(int srv_id);
  MetadataServiceIf* GetClient(int srv_id);
  Status GetMetadataService(int srv_id, MetadataServiceIf** _return);

 private:

  explicit RPC(Config* conf, MetadataServiceIf* self = NULL)
    : conf_(conf)
    , self_(self)
    , mtxes_(new Mutex[conf->GetSrvNum()])
    , clients_(new RPC_Client*[conf->GetSrvNum()]) {
    for (int i = 0; i < conf->GetSrvNum(); i++) {
      clients_[i] = CreateClientIfNotLocal(i);
    }
  }

  Config* conf_;
  MetadataServiceIf* self_;

  // Advisory lock for RPC client sharing in MT contexts
  Mutex* mtxes_;

  struct RPC_Client;
  RPC_Client** clients_;
  bool IsServerLocal(int srv_id);
  RPC_Client* CreateClientFor(int srv_id);
  RPC_Client* CreateClientIfNotLocal(int srv_id);

  // No copy allowed
  RPC(const RPC&);
  RPC& operator=(const RPC&);
};

class RPC_Server {
 public:

  void Stop();
  void RunForever();
  
  static RPC_Server* CreateRPCServer(
    Config* conf, MetadataServiceIf* handler) {
    return new RPC_Server(conf, handler);
  }

  virtual ~RPC_Server();
    
 private:
  
  explicit RPC_Server(Config* conf, MetadataServiceIf* handler)
    : conf_(conf), handler_(handler) {
    server_ = CreateInteralServer();
  }
  
  Config* conf_;
  MetadataServiceIf* handler_;
  
  // Server implementation
  struct RPC_Internal_Server;
  RPC_Internal_Server* server_;
  RPC_Internal_Server* CreateInteralServer();
  
  // No copy allowed
  RPC_Server(const RPC_Server&);
  RPC_Server& operator=(const RPC_Server&); 
};

} /* namespace indexfs */

#endif /* _INDEXFS_COMM_RPC_H_ */
