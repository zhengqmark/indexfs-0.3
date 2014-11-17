// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "client.h"
#include "metadata_client.h"

namespace indexfs {

class DefaultClientFactory: public ClientFactory {
 public:
  virtual ~DefaultClientFactory() { }
  virtual Client* GetClient(Config* config) { return new MetadataClient(config); }

};

ClientFactory* GetDefaultClientFactory() {
  return new DefaultClientFactory();
}

} /* namespace indexfs */
