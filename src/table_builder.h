#pragma once

#include "blob_file_builder.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "future"
#include "iostream"
#include "table/table_builder.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "unordered_map"
#include "util.h"
#include "vector"

namespace rocksdb {
namespace titandb {

class TitanTableBuilder : public TableBuilder {
 public:
  TitanTableBuilder(uint32_t cf_id, const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options,
                    std::unique_ptr<TableBuilder> base_builder,
                    std::shared_ptr<BlobFileManager> blob_manager,
                    std::weak_ptr<BlobStorage> blob_storage, TitanStats *stats,
                    int merge_level, int target_level, int start_level = -1)
      : cf_id_(cf_id),
        db_options_(db_options),
        cf_options_(cf_options),
        base_builder_(std::move(base_builder)),
        blob_manager_(blob_manager),
        blob_storage_(blob_storage),
        stats_(stats),
        target_level_(target_level),
        merge_level_(merge_level),
        start_level_(start_level) {
          merge_low_level_ = blob_storage_.lock()->ShouldGCLowLevel();
          // std::cerr<<"start level: "<<start_level_<<"merge level: "<<merge_level_<<"target level: "<<target_level_<<"merge_low_level: "<<merge_level_<<".\n";
        }

  void Add(const Slice &key, const Slice &value) override;

  Status status() const override;

  Status Finish() override;

  void Abandon() override;

  uint64_t NumEntries() const override;

  uint64_t FileSize() const override;

  bool NeedCompact() const override;

  TableProperties GetTableProperties() const override;

 private:
  friend class TableBuilderTest;

  bool ok() const { return status().ok(); }

  void AddBlob(const Slice &key, const Slice &value, std::string *index_value);

  bool ShouldMerge(const std::shared_ptr<BlobFileMeta> &file);

  void FinishBlobFile();

  void UpdateInternalOpStats();

  ~TitanTableBuilder();

  Status status_;
  uint32_t cf_id_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::unique_ptr<TableBuilder> base_builder_;
  std::unique_ptr<BlobFileHandle> blob_handle_;
  std::shared_ptr<BlobFileManager> blob_manager_;
  std::unique_ptr<BlobFileBuilder> blob_builder_;
  std::weak_ptr<BlobStorage> blob_storage_;
  std::vector<
      std::pair<std::shared_ptr<BlobFileMeta>, std::unique_ptr<BlobFileHandle>>>
      finished_blobs_;
  TitanStats *stats_;
  std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>>
      merging_files_;
  std::unordered_map<uint64_t, std::shared_ptr<BlobFileMeta>> encountered_files_;
  bool merge_low_level_;
  uint64_t blob_merge_time_{0};
  uint64_t blob_read_time_{0};
  uint64_t blob_add_time_{0};
  uint64_t blob_finish_time_{0};

  // target level in LSM-Tree for generated SSTs and blob files
  int target_level_;
  // with cf_options_.level_merge == true, if target_level_ is higher than or
  // equals to merge_level_, values belong to blob files which have lower level
  // than target_level_ will be merged to new blob file
  int merge_level_;
  int start_level_;

  // counters
  uint64_t bytes_read_ = 0;
  uint64_t bytes_written_ = 0;
  uint64_t io_bytes_read_ = 0;
  uint64_t io_bytes_written_ = 0;
};


// separete kv before memtable
class ForegroundBuilder {
 public:
  friend class BlobGCJob;
  struct Request {
    Slice key;
    Slice val;
    WriteBatch *wb;
    std::promise<Status> res;

    Request() = default;
    Request(const Slice &k, const Slice &v, WriteBatch *w)
        : key(k), val(v), wb(w), res() {}
  };

  Status Add(const Slice &key, const Slice &value, WriteBatch *wb);

  void Finish();

  void Flush();

  void Init() {
    for (int i = 0; i < num_builders_; i++) {
      handle_[i].reset();
      builder_[i].reset();
      finished_files_[i].clear();
      pool_.emplace_back(&ForegroundBuilder::handleRequest, this, i);
    }
  }

  ForegroundBuilder(uint32_t cf_id,
                    std::shared_ptr<BlobFileManager> blob_file_manager,
                    std::weak_ptr<BlobStorage> blob_storage,
                    const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options,
                    TitanStats *stats)
      : num_builders_(db_options.num_foreground_builders),
        cf_id_(cf_id),
        blob_file_manager_(blob_file_manager),
        blob_storage_(blob_storage),
        db_options_(db_options),
        cf_options_(cf_options),
        env_options_(db_options_),
        handle_(db_options.num_foreground_builders),
        builder_(db_options.num_foreground_builders),
        finished_files_(db_options.num_foreground_builders),
        requests_(db_options.num_foreground_builders),
        stats_(stats) {
    env_options_.writable_file_max_buffer_size = 4*1024;
  }

  ForegroundBuilder() = default;

 private:
  int num_builders_;
  uint32_t cf_id_;
  std::shared_ptr<BlobFileManager> blob_file_manager_;
  std::weak_ptr<BlobStorage> blob_storage_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  EnvOptions env_options_;
  std::vector<std::unique_ptr<BlobFileHandle>> handle_;
  std::vector<std::unique_ptr<BlobFileBuilder>> builder_;
  std::vector<std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                                    std::unique_ptr<BlobFileHandle>>>>
      finished_files_;
  std::hash<std::string> hash{};
  std::vector<BlockQueue<Request *>> requests_;
  std::vector<std::thread> pool_{};
  TitanStats *stats_;

  void handleRequest(int i);

  Status FinishBlob(int b);
};

}  // namespace titandb
}  // namespace rocksdb
