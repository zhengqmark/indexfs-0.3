// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rpc.h"
#include "rpc_helper.h"

namespace indexfs {

struct RPC::RPC_Client {

  virtual ~RPC_Client() {
    if (stub_ != NULL) {
      delete stub_;
    }
  }

  RPC_Client(const std::string &ip, int port)
    : socket_(new TSocket(ip, port))
    , transport_(new TBufferedTransport(socket_))
    , protocol_(new TBinaryProtocol(transport_))
    , stub_(new MetadataServiceClient(protocol_)) {
    alive_ = false;
  }

  bool IsAlive() { return alive_; }

  void Close() {
    try {
      transport_->close();
    } catch (TTransportException &tx) {
      LOG(WARNING) << "Fail to close socket: " << tx.what();
    }
  }

  Status Open() {
    DLOG_ASSERT(!alive_);
    try {
      transport_->open();
      alive_ = stub_->InitRPC();
    } catch (TTransportException &tx) {
      alive_ = false;
      LOG(ERROR) << "Fail to open socket: " << tx.what();
      return Status::IOError("Cannot open socket");
    }
    CHECK(alive_) << "Unexpected server behavior";
    return Status::OK();
  }

  bool alive_;
  shared_ptr<TSocket> socket_;
  shared_ptr<TTransport> transport_;
  shared_ptr<TProtocol> protocol_;
  MetadataServiceClient* stub_;
};

RPC::~RPC() {
  if (clients_ != NULL) {
    for (int i = 0; i < conf_->GetSrvNum(); i++) {
      if (clients_[i] != NULL) {
        delete clients_[i];
        clients_[i] = NULL;
      }
    }
    delete [] clients_;
  }
  if (mtxes_ != NULL) delete [] mtxes_;
}

Status RPC::Init() {
  for (int i = 0; i < conf_->GetSrvNum(); i++) {
    if (clients_[i] != NULL) {
      DLOG(INFO) << "Initializing RPC client #" << i;
      Status s = clients_[i]->Open();
      if (!s.ok()) {
        delete clients_[i];
        clients_[i] = CreateClientFor(i);
        std::stringstream ss;
        ss << "Fail to open RPC client #" << i;
        return Status::IOError(ss.str(), s.ToString());
      }
      DLOG(INFO) << "RPC client #" << i << " initialized";
    }
  }
  return Status::OK();
}

Status RPC::Shutdown() {
  for (int i = 0; i < conf_->GetSrvNum(); i++) {
    if (clients_[i] != NULL) {
      clients_[i]->Close();
      DLOG(INFO) << "RPC client #" << i << " closed";
    }
  }
  return Status::OK();
}

bool RPC::IsServerLocal(int srv_id) {
  return conf_->GetSrvID() == srv_id;
}

RPC::RPC_Client* RPC::CreateClientFor(int srv_id) {
  const std::pair<std::string, int> &addr = conf_->GetSrvAddr(srv_id);
  DLOG(INFO) << "Creating RPC client #" << srv_id;
  return new RPC_Client(addr.first, addr.second);
}

RPC::RPC_Client* RPC::CreateClientIfNotLocal(int srv_id) {
  if (self_ == NULL) {
    return CreateClientFor(srv_id);
  }
  return IsServerLocal(srv_id) ? NULL : CreateClientFor(srv_id);
}

// Retrieves the advisory lock associated with a given RPC client.
//
Mutex* RPC::GetMutex(int srv_id) {
  DLOG_ASSERT(srv_id >= 0);
  DLOG_ASSERT(srv_id < conf_->GetSrvNum());
  return mtxes_ + srv_id;
}

// Retrieves the "service" of a given server. Returns the local server handle if possible,
// otherwise returns a RPC client stub associated with the remote server.
// Re-establishes the TCP connection to the server if previous attempts failed.
//
Status RPC::GetMetadataService(int srv_id, MetadataServiceIf** _return) {
  DLOG_ASSERT(srv_id >= 0);
  DLOG_ASSERT(srv_id < conf_->GetSrvNum());
  if (self_ != NULL && IsServerLocal(srv_id)) {
    *_return = self_;
    return Status::OK();
  }
  DLOG_ASSERT(clients_[srv_id] != NULL);
  if (!clients_[srv_id]->IsAlive()) {
    Mutex* mtx = mtxes_ + srv_id;
    MutexLock l(mtx);
    if (!clients_[srv_id]->IsAlive()) {
      DLOG(INFO) << "Re-Initializing RPC client #" << srv_id;
      Status s = clients_[srv_id]->Open();
      if (!s.ok()) {
        delete clients_[srv_id];
        clients_[srv_id] = CreateClientFor(srv_id);
        return Status::IOError("Cannot open client", s.ToString());
      }
      DLOG(INFO) << "RPC client #" << srv_id << " re-initialized";
    }
  }
  *_return = clients_[srv_id]->stub_;
  return Status::OK();
}

// Returns the "RPC client" associated with a given server.
// Commits suicide (by aborting the running process) if the server is currently unreachable.
// This method is introduced for backward compatibility and will be removed in future.
//
// (deprecated)
//
MetadataServiceIf* RPC::GetClient(int srv_id) {
  MetadataServiceIf* _return;
  Status s = GetMetadataService(srv_id, &_return);
  CHECK(s.ok()) << s.ToString();
  return _return;
}

struct RPC_Server::RPC_Internal_Server {
  
  virtual ~RPC_Internal_Server() {
    if (server_ != NULL) {
      delete server_;
    }
  }

  RPC_Internal_Server(MetadataServiceIf* handler, int port)
    : handler_(handler)
    , processor_(new MetadataServiceProcessor(handler_))
    , socket_(new TServerSocket(port))
    , protocol_factory_(new TBinaryProtocolFactory())
    , transport_factory_(new TBufferedTransportFactory()) {
    server_ = new TThreadedServer(
      processor_, socket_, transport_factory_, protocol_factory_);
    CHECK(server_ != NULL) << "Fail to establish a new RPC server";
  }

  void Start() {
    server_->serve();
  }

  void Stop() {
    server_->stop();
  }

  TServer* server_;

  shared_ptr<MetadataServiceIf> handler_;
  shared_ptr<MetadataServiceProcessor> processor_;
  shared_ptr<TServerTransport> socket_;
  shared_ptr<TProtocolFactory> protocol_factory_;
  shared_ptr<TTransportFactory> transport_factory_;
};

RPC_Server::~RPC_Server() {
  if (server_ != NULL) {
    delete server_;
  }  
}

// Interrupt the server and stop its service.
//
void RPC_Server::Stop() {
  server_->Stop();
}

// Ask the server to start listening to client requests.
// This call should and will never return.
//
void RPC_Server::RunForever() {
  server_->Start();
}

// Creates an internal RPC server using pre-specified server configurations.
//
RPC_Server::RPC_Internal_Server* RPC_Server::CreateInteralServer() {
  int srv_id = conf_->GetSrvID();
  CHECK(srv_id >= 0) << "Unexpected server id";
  int srv_port = conf_->GetSrvAddr(srv_id).second;
  return new RPC_Internal_Server(handler_, srv_port);
}

} /* namespace indexfs */
