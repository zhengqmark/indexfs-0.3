// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef MONITORTHREAD_H_
#define MONITORTHREAD_H_

#include "measurement.h"
#include "leveldb/util/socket.h"

namespace indexfs {

class MonitorThread {
public:
  enum ReportMethod {
    kLOG, kOpenTSDB
  };

  MonitorThread(Measurement *measure,
                int frequency = 2,
                ReportMethod method = kOpenTSDB);
  virtual ~MonitorThread();

  void Start();

  void Stop();

private:
  bool IsDone() { return done_; }

  void SetDone(bool done) { done_ = done; }

  void SendMetrics();

  static void* Run(void* arg);
  // the followsings are used only by the main thread
  //
  Measurement* measure_;
  bool done_;
  pthread_t tid_;
  int frequency_;
  ReportMethod method_;
  leveldb::UDPSocket socket_;
};

} // namespace indexfs

#endif /* MONITORTHREAD_H_ */
