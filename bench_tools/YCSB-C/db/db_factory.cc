//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/redis_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
#include "db/leveldb_db.h"
#include "db/rocksdb_db.h"
#include "db/titandb_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
  } else if (props["dbname"] == "redis") {
    int port = stoi(props["port"]);
    int slaves = stoi(props["slaves"]);
    return new RedisDB(props["host"].c_str(), port, slaves);
  } else if (props["dbname"] == "tbb_rand") {
    return new TbbRandDB;
  } else if (props["dbname"] == "tbb_scan") {
    return new TbbScanDB;
  } else if (props["dbname"] == "leveldb" || props["dbname"]=="pebblesdb") {
    return new LevelDB(props["dbfilename"].c_str(), props["configpath"]);
  } else if (props["dbname"] == "rocksdb" || props["dbname"]=="rocksdb_tiered") {
    return new RocksDB(props["dbfilename"].c_str(),props["configpath"]);
  } else if (props["dbname"]=="diffkv" || props["dbname"] == "titandb") {
    return new TitanDB(props["dbfilename"].c_str(),props["configpath"]);
  }
  else return NULL;
}

