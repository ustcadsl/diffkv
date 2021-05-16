//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

extern double ops_time[4];
extern long ops_cnt[4];

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }
  
  virtual Operation DoInsert();
  virtual Operation DoTransaction();
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  
  DB &db_;
  CoreWorkload &workload_;
};

//FILE* fw = fopen("write_latencies","a");
//FILE* fr = fopen("read_latencies","a");

  inline Operation Client::DoInsert() {
    std::string key = workload_.NextSequenceKey();
    std::vector<DB::KVPair> pairs;
    workload_.BuildValues(pairs);
    assert (db_.Insert(workload_.NextTable(), key, pairs) >=0);
    return (Operation::INSERT);
  }

inline Operation Client::DoTransaction() {
  int status = -1;
  utils::Timer timer;
  Operation operation_type = workload_.NextOperation();
  timer.Start();
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      ops_time[READ] += timer.End();
      ops_cnt[READ]++;
//      fprintf(fr,"%.0f,",timer.End());
      break;
    case UPDATE:
      status = TransactionUpdate();
      ops_time[INSERT] += timer.End();
      ops_cnt[INSERT]++;
//      fprintf(fw,"%.0f,",timer.End());
      break;
    case INSERT:
      status = TransactionInsert();
      ops_time[INSERT] += timer.End();
      ops_cnt[INSERT]++;
//      fprintf(fw,"%.0f,",timer.End());
      break;
    case SCAN:
      status = TransactionScan();
      ops_time[SCAN] += timer.End();
      ops_cnt[SCAN]++;
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (operation_type);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
