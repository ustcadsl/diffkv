//
// Created by wujy on 1/23/19.
//

#include "rocksdb_db.h"
#include "leveldb_config.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include <iostream>

using namespace std;

namespace ycsbc {
RocksDB::RocksDB(const char* dbfilename,const std::string & config_file_path)
    : noResult(0)
{
  //get rocksdb config
  ConfigLevelDB config = ConfigLevelDB(config_file_path);
  int bloomBits = config.getBloomBits();
  size_t blockCache = config.getBlockCache();
  bool seekCompaction = config.getSeekCompaction();
  bool compression = config.getCompression();
  bool directIO = config.getDirectIO();
  size_t memtable = config.getMemtable();
  //set optionssc
  rocksdb::BlockBasedTableOptions bbto;
  options.create_if_missing = true;
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.write_buffer_size = memtable;
  options.target_file_size_base = 64 << 20;
  //options.compaction_pri = rocksdb::kMinOverlappingRatio;
  if (config.getTiered())
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
  options.max_background_jobs = config.getNumThreads();
  options.disable_auto_compactions = config.getNoCompaction();
  //options.level_compaction_dynamic_level_bytes = true;
  //options.target_file_size_base = 8<<20;
  cerr << "write buffer size" << options.write_buffer_size << endl;
  cerr << "write buffer number" << options.max_write_buffer_number << endl;
  cerr << "num compaction trigger" << options.level0_file_num_compaction_trigger << endl;
  cerr << "targe file size base" << options.target_file_size_base << endl;
  cerr << "level size base" << options.max_bytes_for_level_base << endl;
  if (!compression)
    options.compression = rocksdb::kNoCompression;
  if (bloomBits > 0) {
    bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
  }
  bbto.block_cache = rocksdb::NewLRUCache(blockCache);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

  rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
  if (!s.ok()) {
    cerr << "Can't open rocksdb " << dbfilename << endl;
    exit(0);
  }
  cout << "\nbloom bits:" << bloomBits << "bits\ndirectIO:" << (bool)directIO << "\nseekCompaction:" << (bool)seekCompaction << endl;
}

int RocksDB::Read(const std::string& table, const std::string& key, const std::vector<std::string>* fields,
    std::vector<KVPair>& result)
{
  string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  // if(s.ok()) return DB::kOK;
  // if(s.IsNotFound()){
  //     noResult++;
  //     cout<<noResult<<"not found"<<endl;
  //     return DB::kOK;
  // }else{
  //     cerr<<"read error"<<endl;
  //     exit(0);
  // }
  return DB::kOK;
}

int RocksDB::Scan(const std::string& table, const std::string& key, int len, const std::vector<std::string>* fields,
    std::vector<std::vector<KVPair>>& result)
{
  auto it = db_->NewIterator(rocksdb::ReadOptions());
  it->Seek(key);
  std::string val;
  std::string k;
  int i;
  int cnt = 0;
  for (i = 0; i < len && it->Valid(); i++) {
    k = it->key().ToString();
    val = it->value().ToString();
    it->Next();
    if (val.empty())
      cnt++;
  }
  // if(i<len) {
  //     std::cout<<" get "<<i<<" for length "<<len<<"."<<std::endl;
  //     std::cerr<<" get "<<i<<" for length "<<len<<"."<<std::endl;
  // }
  // if(cnt>0) {
  //     std::cout<<cnt<<"empty values"<<std::endl;
  // }
  delete it;
  return DB::kOK;
}

int RocksDB::Insert(const std::string& table, const std::string& key,
    std::vector<KVPair>& values)
{
  rocksdb::Status s;
  for (KVPair& p : values) {
    s = db_->Put(rocksdb::WriteOptions(), key, p.second);
    if (!s.ok()) {
      cerr << "insert error" << s.ToString() << "\n"
           << endl;
      exit(0);
    }
  }
  return DB::kOK;
}

int RocksDB::Update(const std::string& table, const std::string& key, std::vector<KVPair>& values)
{
  return Insert(table, key, values);
}

int RocksDB::Delete(const std::string& table, const std::string& key)
{
  vector<DB::KVPair> values;
  return Insert(table, key, values);
}

void RocksDB::printStats()
{
  string stats;
  db_->GetProperty("rocksdb.stats", &stats);
  // cout << stats << endl;
  // cout << options.statistics->ToString() << endl;
}

RocksDB::~RocksDB()
{
  delete db_;
}
}
