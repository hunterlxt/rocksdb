// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <time.h>

#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "temp_db";

uint64_t NUM = 0;
uint64_t LAST = 0;
int VAL_SIZE = 512;
uint64_t LOOP_SIZE = 1000000000000000;
uint64_t BATCH_SIZE = 4;
uint64_t QPS_GAP = 6;
bool INSERT = true;

std::string gen_random(const int len) {
  char *s = (char *)malloc(len + 1);
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;
  auto ret = std::string(s);
  free(s);
  return ret;
}

Slice get_key() {
  NUM += 1;
  if (NUM % 8000000 == 0) {
    std::cout << "NUM:" << NUM << std::endl;
  }
  return std::to_string(NUM);
}

std::string get_value() { return std::string(gen_random(VAL_SIZE)); }

void do_stat(std::shared_ptr<Statistics> stat) {
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(QPS_GAP));
    auto cur_num = NUM;
    auto cur_last = LAST;
    LAST = NUM;
    std::cout << "qps: " << (cur_num - cur_last) / QPS_GAP << std::endl;
    std::cout << "GetBytesInserted:" << stat->getAndResetTickerCount(17)
              << std::endl;
  }
}

void do_compact(DB *db) {
  std::cout << "comapct" << std::endl;
  CompactRangeOptions compact_options;
  compact_options.max_subcompactions = 4;
  Status s = db->CompactRange(compact_options, nullptr, nullptr);
  assert(s.ok());
  std::cout << "comapct done" << std::endl;
}

void do_delete_files_in_range(DB *db) {
  std::cout << "delete_files_in_range" << std::endl;
  Status s =
      DeleteFilesInRange(db, db->DefaultColumnFamily(), nullptr, nullptr);
  assert(s.ok());
  std::cout << "delete_files_in_range done" << std::endl;
}

void exec_command(DB *db) {
  // warm up
  for (int i = 0; i < 10000; i++) {
    std::string value;
    db->Get(ReadOptions(), "100", &value);
  }
  while (1) {
    int input = 0;
    std::cin >> input;
    std::cout << "recv cmd" << std::endl;
    if (input == 1) {
      do_compact(db);
    }
    if (input == 2) {
      do_delete_files_in_range(db);
    }
    if (input == 0) {
      std::cout << "cmd finished" << std::endl;
      break;
    }
    std::cout << "cmd loop" << std::endl;
  }
}

int main() {
  DB *db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  std::thread t1(exec_command, db);
  std::thread t2(do_stat, statistics);

  if (INSERT) {
    s = db->Put(WriteOptions(), "0", "value");
    assert(s.ok());
    std::string value;
    s = db->Get(ReadOptions(), "0", &value);
    assert(s.ok());
    assert(value == "value");
    for (uint64_t i = 0; i < LOOP_SIZE; i++) {
      WriteBatch batch;
      for (uint64_t j = 0; j < BATCH_SIZE; j++) {
        batch.Put(get_key(), get_value());
      }
      s = db->Write(WriteOptions(), &batch);
      assert(s.ok());
    }
  }

  std::cout << "all test done" << std::endl;
  t1.join();
  delete db;

  return 0;
}
