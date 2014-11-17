// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitor_thread.h"
#include "common/common.h"
#include "leveldb/env.h"
#include <sstream>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

namespace indexfs {

static pthread_t CreateThread
    (void*(*func)(void*), void* arg) {
  pthread_t tid;
  if (pthread_create(&tid, NULL, func, arg) < 0) {
    perror("Fail to create thread!");
    abort();
  }
  return tid;
}

static void JoinThread(pthread_t tid) {
  if (pthread_join(tid, NULL) < 0) {
    perror("Fail to join thread!");
    abort();
  }
}

MonitorThread::MonitorThread(Measurement *measure, int frequency,
                             ReportMethod method) :
                             measure_(measure), done_(false), tid_(0),
                             frequency_(frequency), method_(method) {
}

MonitorThread::~MonitorThread() {
  if (!done_)
    Stop();
}

void MonitorThread::Start() {
  tid_ = CreateThread(&Run, this);
}

void MonitorThread::Stop() {
  SetDone(true);
  JoinThread(tid_);
}

void MonitorThread::SendMetrics() {
  if (method_ == kOpenTSDB) {
    std::stringstream stream;
    measure_->GetStatus(stream);
    std::string report = stream.str();
    try {
      socket_.sendTo(report.data(), report.size(),
                     std::string("127.0.0.1"), 10600);
    } catch (leveldb::SocketException &e) {
    }
  }
}

void* MonitorThread::Run(void* arg) {
  MonitorThread* mon = reinterpret_cast<MonitorThread*>(arg);
  while (!mon->done_) {
    leveldb::Env::Default()->SleepForMicroseconds(mon->frequency_*1000000);
    mon->SendMetrics();
  };
  return 0;
}

} // namespace indexfs
