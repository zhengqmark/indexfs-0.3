// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef MEASUREMENT_H_
#define MEASUREMENT_H_

#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include "leveldb/util/histogram.h"
#include "leveldb/env.h"

namespace indexfs {

class Measurement {
public:
  Measurement(const std::vector<std::string> &metrics,
              int server_id, int window_size = 5) :
              server_id_(server_id), metrics_(metrics),
              window_size_(window_size)  {
    num_metrics_ = metrics.size() + 1;
    metrics_.push_back(std::string("total"));
    hists_ = new leveldb::Histogram[num_metrics_];
    for (int i = 0; i < num_metrics_; ++i)
      hists_[i].Clear();
    count_ = new int[num_metrics_];
    memset(count_, 0, sizeof(int) * num_metrics_);
    window_start_ = 0;
  }

  void AddMetric(int no_metric, double latency) {
    Check(no_metric);
    if (no_metric >= 0 && no_metric < num_metrics_) {
      hists_[no_metric].Add(latency);
      count_[no_metric]++;
    }
    hists_[num_metrics_ - 1].Add(latency);
    count_[num_metrics_ - 1]++;
  }

  void AddMetricNoCheck(int no_metric, double latency) {
    if (no_metric >= 0 && no_metric < num_metrics_)
      hists_[no_metric].Add(latency);
    hists_[num_metrics_ - 1].Add(latency);
  }

  void GetStatus(std::stringstream &report) {
    Check(-1);
    time_t now = time(NULL);
    for (int i = 0; i < num_metrics_; ++i) {
      report << metrics_[i] << "_num" << " ";
      report << now << " ";
      report << count_[i];
      report << " rank=" << server_id_  << '\n';
      report << metrics_[i] << "_max_lat" << " ";
      report << now << " ";
      report << hists_[i].Max();
      report << " rank=" << server_id_  << '\n';
      report << metrics_[i] << "_avg_lat" << " ";
      report << now << " ";
      report << hists_[i].Average();
      report << " rank=" << server_id_  << '\n';
    }
  }

  void Print(FILE* output) {
    for (int i = 0; i < num_metrics_; ++i) {
      fprintf(output, "== Latencies for %s ops:\n", metrics_[i].c_str());
      fprintf(output, "%s\n", hists_[i].ToString().c_str());
    }
  }

  virtual ~Measurement() {
    delete [] hists_;
    delete [] count_;
  }

private:
  int num_metrics_;
  int* count_;
  int server_id_;

  leveldb::Histogram* hists_;
  std::vector<std::string> metrics_;
  int window_size_;
  int window_start_;

  void Check(int no_metric) {
    if (window_size_ <= 0) return;
    int now_time = time(NULL);
    if (now_time - window_start_ > window_size_) {
      if (no_metric < 0) {
        for (int i = 0; i < num_metrics_; ++i)
          hists_[i].Clear();
      } else {
        hists_[no_metric].Clear();
      }
      window_start_ = now_time;
    }
  }
};

struct MeasurementHelper {
  int no_metric_;
  Measurement* measure_;
  uint64_t latency_;

  MeasurementHelper(int no_metric, Measurement* measure) :
    no_metric_(no_metric), measure_(measure) {
    latency_ = leveldb::Env::Default()->NowMicros();
  }

  ~MeasurementHelper() {
    latency_ = leveldb::Env::Default()->NowMicros() - latency_;
    measure_->AddMetric(no_metric_, (double) latency_);
  }
};

} // namespace indexfs

#endif /* MEASUREMENT_H_ */
