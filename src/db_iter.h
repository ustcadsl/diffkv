#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <memory>
#include <unordered_map>

#include "db/db_iter.h"
#include "future"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "vector"

#include "threadpool.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(const TitanReadOptions& options, BlobStorage* storage,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter, Env* env,
                  TitanStats* stats, Logger* info_log)
      : options_(options),
        storage_(storage),
        snap_(snap),
        iter_(std::move(iter)),
        env_(env),
        stats_(stats),
        info_log_(info_log) {}

  bool Valid() const override { return iter_->Valid(); /*&& status_.ok(); */ }

  Status status() const override {
    // assume volatile inner iter
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Next() override {
    assert(Valid());
    iter_->Next();
    if (ShouldGetBlobValue()) {
      StopWatch next_sw(env_, stats_, BLOB_DB_NEXT_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_NEXT);
    }
  }

void Scan(const Slice& target, int& len, std::vector<std::string>& keys,
            std::vector<std::string>& values) {
    std::vector<BlobIndex> indexes(len);
    int i = 0;
    iter_->Seek(target);
    while (i < len && Valid()) {
      if (ShouldGetBlobValue()) {
        assert(iter_->status().ok());
        status_ = DecodeInto(iter_->value(), &indexes[i]);
        if (!status_.ok()) {
          return;
        }
        auto it = files_.find(indexes[i].file_number);
        if (it == files_.end()) {
          std::unique_ptr<BlobFilePrefetcher> prefetcher;
          status_ =
              storage_->NewPrefetcher(indexes[i].file_number, &prefetcher);
          if (!status_.ok()) {
            return;
          }
          it = files_.emplace(indexes[i].file_number, std::move(prefetcher))
                   .first;
        }
        it->second->Prefetch(indexes[i].blob_handle);
      } else {
        values[i] = iter_->value().ToString();
      }
      keys[i] = iter_->key().ToString();
      iter_->Next();
      i++;
    }
    len = i;
    BlobRecord record;
    PinnableSlice buffer;
    for (int j=0;j<len;j++){
      if(values[j].empty()){
        auto it = files_.find(indexes[j].file_number);
        it->second->PointGet(options_, indexes[j].blob_handle, &record, &buffer);
        values[j] = std::move(record.value.ToString());
      }
    }
  }

/*
  void Scan(const Slice& target, int& len, std::vector<std::string>& keys,
            std::vector<std::string>& values) {
    std::vector<BlobIndex> indexes(len);
    int i = 0;
    iter_->Seek(target);
    while (i < len && Valid()) {
      if (ShouldGetBlobValue()) {
        assert(iter_->status().ok());
        status_ = DecodeInto(iter_->value(), &indexes[i]);
        if (!status_.ok()) {
          return;
        }
        auto it = files_.find(indexes[i].file_number);
        if (it == files_.end()) {
          std::unique_ptr<BlobFilePrefetcher> prefetcher;
          status_ =
              storage_->NewPrefetcher(indexes[i].file_number, &prefetcher);
          if (!status_.ok()) {
            return;
          }
          it = files_.emplace(indexes[i].file_number, std::move(prefetcher))
                   .first;
        }
      } else {
        values[i] = iter_->value().ToString();
      }
      keys[i] = iter_->key().ToString();
      iter_->Next();
      i++;
    }
    len = i;
    int bulk = std::max(16, len / 8);
    std::vector<std::future<Status>> ss;
    for (int j = 0; j < len; j += bulk) {
      ss.emplace_back(pool_->addTask(
          &TitanDBIterator::BulkRead, this, std::ref(indexes), std::ref(keys),
          std::ref(values), j, std::min(j + bulk, len)));
    }
    for (auto& fs : ss) {
      auto s = fs.get();
    }
    return;
  }
  */
  // */
  void Prev() override {
    assert(Valid());
    iter_->Prev();
    if (ShouldGetBlobValue()) {
      StopWatch prev_sw(env_, stats_, BLOB_DB_PREV_MICROS);
      GetBlobValue();
      // RecordTick(stats_, BLOB_DB_NUM_PREV);
    }
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid() && !options_.key_only);
    if (options_.key_only) return Slice();
    if (!iter_->IsBlob()) return iter_->value();
    return record_.value;
  }

 private:
  Status BulkRead(const std::vector<BlobIndex>& indexes,
                  std::vector<std::string>& keys,
                  std::vector<std::string>& vals, int start, int end) {
    PinnableSlice buffer;
    BlobRecord record;
    Status s;
    for (int j = start; j < end; j++) {
      if (vals[j] == "") {
        auto it = files_.find(indexes[j].file_number);
        auto r =
            it->second->Get(options_, indexes[j].blob_handle, &record, &buffer);
        if (!r.ok()) {
          s = r;
        } else {
          vals[j] = record.value.ToString();
        }
      }
    }
    return s;
  }

  bool ShouldGetBlobValue() {
    if (!iter_->Valid() || !iter_->IsBlob() || options_.key_only) {
      status_ = iter_->status();
      return false;
    }
    return true;
  }

  void GetBlobValue() {
    assert(iter_->status().ok());

    BlobIndex index;
    status_ = DecodeInto(iter_->value(), &index);
    if (!status_.ok()) {
      ROCKS_LOG_ERROR(info_log_,
                      "Titan iterator: failed to decode blob index %s: %s",
                      iter_->value().ToString(true /*hex*/).c_str(),
                      status_.ToString().c_str());
      return;
    }

    auto it = files_.find(index.file_number);
    if (it == files_.end()) {
      std::unique_ptr<BlobFilePrefetcher> prefetcher;
      status_ = storage_->NewPrefetcher(index.file_number, &prefetcher);
      if (!status_.ok()) {
        ROCKS_LOG_ERROR(
            info_log_,
            "Titan iterator: failed to create prefetcher for blob file %" PRIu64
            ": %s",
            index.file_number, status_.ToString().c_str());
        return;
      }
      it = files_.emplace(index.file_number, std::move(prefetcher)).first;
    }

    buffer_.Reset();
    status_ = it->second->Get(options_, index.blob_handle, &record_, &buffer_);
    if (!status_.ok()) {
      ROCKS_LOG_ERROR(
          info_log_,
          "Titan iterator: failed to read blob value from file %" PRIu64
          ", offset %" PRIu64 ", size %" PRIu64 ": %s\n",
          index.file_number, index.blob_handle.offset, index.blob_handle.size,
          status_.ToString().c_str());
    }
    return;
  }

  Status status_;
  BlobRecord record_;
  PinnableSlice buffer_;

  TitanReadOptions options_;
  BlobStorage* storage_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>> files_;

  Env* env_;
  TitanStats* stats_;
  Logger* info_log_;
  static ThreadPool* pool_;
};

}  // namespace titandb
}  // namespace rocksdb
