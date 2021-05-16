//
// Created by wujy on 18-1-21.
//

#include "leveldb_db.h"
#include <iostream>
#include "pebblesdb/filter_policy.h"
#include "pebblesdb/cache.h"

using namespace std;

namespace ycsbc {
    LevelDB::LevelDB(const char *dbfilename,const std::string &config_file_path) :noResult(0){
        //get leveldb config
        cerr<<"begin\n";
        ConfigLevelDB config = ConfigLevelDB(config_file_path);
        int bloomBits = config.getBloomBits();
        bool compression = config.getCompression();

        //set options
        leveldb::Options options;
        options.create_if_missing = true;
//	options.max_open_files = 20000;
        options.exp_ops.noCompaction = config.getNoCompaction();
        if(!compression)
            options.compression = leveldb::kNoCompression;
        if(bloomBits>0)
            options.filter_policy = leveldb::NewBloomFilterPolicy(bloomBits);
        // options.exp_ops.seekCompaction = config.getSeekCompaction();
        // options.exp_ops.directIO = config.getDirectIO();
        options.block_cache = leveldb::NewLRUCache(config.getBlockCache());
        options.write_buffer_size = config.getMemtable();
	    // options.exp_ops.numThreads = config.getNumThreads();
	    // options.exp_ops.smallThreshold = config.getSmallThresh();
	    // options.exp_ops.mediumThreshold = config.getMidThresh();
        cerr<<"write buffer: "<<options.write_buffer_size<<endl;
        //options.exp_ops.baseLevelSize = options.write_buffer_size*10.0/(4*32);
        // options.exp_ops.baseLevelSize = options.write_buffer_size*10.0/4;
        // options.exp_ops.gcRatio = config.getGCRatio();
        // options.exp_ops.gcLevel = config.getGCLevel();
        // options.exp_ops.mergeLevel = config.getMergeLevel();

        leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cerr<<"Can't open leveldb "<<dbfilename<<endl;
            exit(0);
        }
        cout<<"\nbloom bits:"<<bloomBits<<"bits"<<endl;
    }

    int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
        if(s.ok()) return DB::kOK;
        // if(s.IsNotFound()){
        //     noResult++;
        //     cout<<noResult<<endl;
        //     return DB::kOK;
        // }else{
        //     cerr<<"read error"<<endl;
        //     exit(0);
        // }
        return DB::kOK;
    }


    int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        // vector<string> keys;
        // vector<string> values;
        // db_->Scan(leveldb::ReadOptions(),key,"",keys,values,len);
        auto it=db_->NewIterator(leveldb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        int cnt = 0;
        int i;
        for(i=0;i<len&&it->Valid();i++){
                k = it->key().ToString();
                val = it->value().ToString();
                it->Next();
                if(val.empty()) cnt++;
            }
        if(cnt>0) std::cout<<cnt<<std::endl;
        // if(i<len) {
        //     std::cout<<" get "<<i<<" for length "<<len<<"."<<std::endl;
        //     std::cerr<<" get "<<i<<" for length "<<len<<"."<<std::endl;
        // }

        // std::cerr<<i<<std::endl;
        delete it;
        return DB::kOK;
    }

    int LevelDB::Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        leveldb::Status s;
        for(KVPair &p:values){
            s = db_->Put(leveldb::WriteOptions(),key,p.second);
            if(!s.ok()){
                cerr<<"insert error "<<s.ToString()<<endl;
            }
        }
        return DB::kOK;
    }

    int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int LevelDB::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void LevelDB::printStats() {
        string stats;
        db_->GetProperty("leveldb.stats",&stats);
        cout<<stats<<endl;
    }

    LevelDB::~LevelDB() {
        delete db_;
    }
}
