#include "blob_gc_picker.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <iostream>

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker(TitanDBOptions db_options,
                                     TitanCFOptions cf_options,
                                     TitanStats* stats)
    : db_options_(db_options), cf_options_(cf_options), stats_(stats) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Status s;
  std::vector<BlobFileMeta*> blob_files;

  uint64_t batch_size = 0;
  uint64_t estimate_output_size = 0;
  bool stop_picking = false;
  bool maybe_continue_next_time = false;
  uint64_t next_gc_size = 0;
  int cnt = 0;
  for (auto& gc_score : blob_storage->gc_score()) {
    cnt++;
    // if (gc_score.score < cf_options_.blob_file_discardable_ratio &&
    // cf_options_.level_merge &&
    // cf_options_.blob_file_discardable_ratio != 0.01) {
    // std::cerr<<"break"<<std::endl;
    // break;
    // }
    // assert(gc_score.score >= cf_options_.blob_file_discardable_ratio);
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    if (!CheckBlobFile(blob_file.get()) ||
        (cf_options_.level_merge && blob_file->file_type() == kSorted)) {
      // RecordTick(stats_, TitanStats::GC_NO_NEED, 1);
      // Skip this file id this file is being GCed
      // or this file had been GCed
      ROCKS_LOG_INFO(db_options_.info_log, "Blob file %" PRIu64 " no need gc",
                     blob_file->file_number());
      // std::cerr<<"no need gc"<<std::endl;
      // if(!blob_file) std::cerr<<"no such file"<<std::endl;
      // if (!CheckBlobFile(blob_file.get()))
      // std::cerr << "check failed" << std::endl;
      continue;
    }
    if (!stop_picking) {
      // if (gc_score.score == 0) continue;
      blob_files.push_back(blob_file.get());
      batch_size += blob_file->file_size();
      // std::cerr<<"batch size add "<<blob_file->file_size()<<"
      // size"<<std::endl;
      estimate_output_size +=
          (blob_file->file_size() - blob_file->discardable_size());
      if (batch_size >= cf_options_.max_gc_batch_size /*||
          estimate_output_size >= cf_options_.blob_file_target_size*/) {
        stop_picking = true;
      }
    } else {
      // next_gc_size += blob_file->file_size();
      // if (next_gc_size > cf_options_.min_gc_batch_size) {
        if (blob_storage->gc_score().size()-cnt>(1<<30)/blob_storage->cf_options().blob_file_target_size){
        maybe_continue_next_time = true;
        

        // RecordTick(stats_, TitanStats::GC_REMAIN, 1);
        ROCKS_LOG_INFO(db_options_.info_log,
                       "remain more than %" PRIu64
                       " bytes to be gc and trigger after this gc",
                       next_gc_size);
        break;
      }
    }
  }
  ROCKS_LOG_DEBUG(db_options_.info_log,
                  "got batch size %" PRIu64 ", estimate output %" PRIu64
                  " bytes",
                  batch_size, estimate_output_size);
  if (blob_files.empty() || batch_size < cf_options_.min_gc_batch_size) {
    return nullptr;
  }
  // if there is only one small file to merge, no need to perform
  if (blob_files.size() == 1 &&
      blob_files[0]->file_size() <= cf_options_.merge_small_file_threshold &&
      blob_files[0]->gc_mark() == false /*&&
      blob_files[0]->GetDiscardableRatio() <
          cf_options_.blob_file_discardable_ratio*/) {
    return nullptr;
  }
  // std::cerr<<"return "<<blob_files.size()<<"blob to gc"<<std::endl;
  return std::unique_ptr<BlobGC>(new BlobGC(
      std::move(blob_files), std::move(cf_options_), maybe_continue_next_time));
}

bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file) const {
  assert(blob_file == nullptr ||
         blob_file->file_state() != BlobFileMeta::FileState::kInit);
  if (blob_file == nullptr ||
      blob_file->file_state() != BlobFileMeta::FileState::kNormal) {
    if (blob_file == nullptr) {
      std::cerr << "check null blob file\n" << std::endl;
    }
    return false;
  }

  return true;
}

}  // namespace titandb
}  // namespace rocksdb
