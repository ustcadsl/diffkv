#pragma once

#include "options/db_options.h"
#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/file_reader_writer.h"

#include <list>
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

template <class T>
class BlockQueue {
 public:
  BlockQueue() : mutex_(), cv_(&mutex_), q_() {}

  T Get() {
    MutexLock ml(&mutex_);
    while (q_.empty()) cv_.Wait();
    T ret = q_.front();
    q_.pop_front();
    return ret;
  }

  void Put(const T& item) {
    MutexLock ml(&mutex_);
    q_.push_back(item);
    if (q_.size() == 1) cv_.SignalAll();
  }

  std::list<T> GetBulk(size_t max = 32) {
    std::list<T> res;
    MutexLock ml(&mutex_);
    while (q_.empty()) cv_.Wait();
    res.splice(res.begin(), q_);
    return std::move(res);
  }

 private:
  port::Mutex mutex_;
  port::CondVar cv_;
  std::list<T> q_;
};

// A slice pointed to an owned buffer.
class OwnedSlice : public Slice {
 public:
  void reset(CacheAllocationPtr _data, size_t _size) {
    data_ = _data.get();
    size_ = _size;
    buffer_ = std::move(_data);
  }

  void reset(CacheAllocationPtr buffer, const Slice& s) {
    data_ = s.data();
    size_ = s.size();
    buffer_ = std::move(buffer);
  }

  char* release() {
    data_ = nullptr;
    size_ = 0;
    return buffer_.release();
  }

  static void CleanupFunc(void* buffer, void*) {
    delete[] reinterpret_cast<char*>(buffer);
  }

 private:
  CacheAllocationPtr buffer_;
};

// A slice pointed to a fixed size buffer.
template <size_t T>
class FixedSlice : public Slice {
 public:
  FixedSlice() : Slice(buffer_, T) {}

  char* get() { return buffer_; }

 private:
  char buffer_[T];
};

// Compresses the input data according to the compression context.
// Returns a slice with the output data and sets "*type" to the output
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(const CompressionInfo& info, const Slice& input,
               std::string* output, CompressionType* type);

// Uncompresses the input data according to the uncompression type.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(const UncompressionInfo& info, const Slice& input,
                  OwnedSlice* output);

void UnrefCacheHandle(void* cache, void* handle);

template <class T>
void DeleteCacheValue(const Slice&, void* value) {
  delete reinterpret_cast<T*>(value);
}

Status SyncTitanManifest(Env* env, TitanStats* stats,
                         const ImmutableDBOptions* db_options,
                         WritableFileWriter* file);

}  // namespace titandb
}  // namespace rocksdb
