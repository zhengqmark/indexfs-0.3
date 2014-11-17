// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_NETWORK_H_
#define _INDEXFS_COMMON_NETWORK_H_

#include <net/if.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>

#include <vector>
#include "common/common.h"
#include "common/options.h"

namespace indexfs {

struct Socket {
  int fd_;
  struct ifconf ifconf_;
  struct ifreq ifr_[64];
  Socket() {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    ifconf_.ifc_buf = (char *) ifr_;
    ifconf_.ifc_len = sizeof(ifr_);
  }
  virtual ~Socket() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }
  bool IsOpen() {
    return fd_ >= 0;
  }
  Status GetSocketConfig() {
    if (ioctl(fd_, SIOCGIFCONF, &ifconf_) < 0) {
      return Status::IOError("Cannot get socket configurations");
    }
    return Status::OK();
  }
  Status GetHostIPAddresses(std::vector<std::string>* ips) {
    char ip[INET_ADDRSTRLEN];
    int num_ips = ifconf_.ifc_len / sizeof(ifr_[0]);
    for (int i = 0; i < num_ips; i++) {
      struct sockaddr_in* s_in = (struct sockaddr_in *) &ifr_[i].ifr_addr;
      if (inet_ntop(AF_INET, &s_in->sin_addr, ip, INET_ADDRSTRLEN) == NULL) {
        return Status::IOError("Cannot get IP address");
      }
      ips->push_back(ip);
    }
    return Status::OK();
  }
};

Status FetchHostname(std::string* hostname) {
  char buffer[HOST_NAME_MAX];
  if (gethostname(buffer, HOST_NAME_MAX) < 0) {
    return Status::IOError("Cannot get local host name");
  }
  *hostname = buffer;
  return Status::OK();
}

Status GetHostIPAddrs(std::vector<std::string>* ips) {
  Socket socket;
  if (!socket.IsOpen()) {
    return Status::IOError("Cannot create socket");
  }
  Status s = socket.GetSocketConfig();
  if (!s.ok()) {
    return s;
  }
  return socket.GetHostIPAddresses(ips);
}

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_NETWORK_H_ */
