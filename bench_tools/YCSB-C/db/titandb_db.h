//
// Created by 吴加禹 on 2019-07-17.
//

#ifndef YCSB_C_TITANDB_DB_H
#define YCSB_C_TITANDB_DB_H

#include "titan/db.h"
#include "core/db.h"
#include <string>
#include "leveldb_config.h"

namespace ycsbc {
    class TitanDB : public DB{
    public:
        TitanDB(const char *dbfilename,const std::string & config_file_path);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void printStats();

        ~TitanDB();

    private:
        rocksdb::titandb::TitanDB *db_;
        unsigned noResult;
        bool nowal{false};
        rocksdb::titandb::TitanOptions options;
        rocksdb::Iterator* it{nullptr};
    };
}

#endif //YCSB_C_TITANDB_DB_H
