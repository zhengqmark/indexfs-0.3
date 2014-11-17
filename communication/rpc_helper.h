// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMM_RPC_HELPER_H_
#define _INDEXFS_COMM_RPC_HELPER_H_

#include <boost/shared_ptr.hpp>

using boost::weak_ptr;
using boost::shared_ptr;

#include <thrift/TProcessor.h>

using apache::thrift::TProcessor;

#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocolFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TTransportException.h>

using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::transport::TBufferedTransportFactory;
using apache::thrift::transport::TTransportException;

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

using apache::thrift::concurrency::ThreadManager;
using apache::thrift::concurrency::PosixThreadFactory;

#include <thrift/server/TThreadedServer.h>
#include <thrift/server/TThreadPoolServer.h>

using apache::thrift::server::TServer;
using apache::thrift::server::TThreadedServer;
using apache::thrift::server::TThreadPoolServer;

#endif /* _INDEXFS_COMM_RPC_HELPER_H_ */
