#include "blob_storage.h"
#include "atomic"
#include "blob_file_set.h"
#include "iostream"

std::atomic<uint64_t> compute_gc_score{0};

namespace rocksdb {
namespace titandb {

extern Env *env_;

bool BlobStorage::ShouldGCLowLevel() {
    uint64_t low_size = 0;
    uint64_t high_size = 0;
    int i;
    for(i=0;i<cf_options_.num_levels-2;i++){
      low_size += level_blob_size_[i].load();
    }
    for(;i<cf_options_.num_levels+1;i++){
      high_size += level_blob_size_[i].load();
    }
    // std::cerr<<"level size:"<<low_size<<" "<<high_size<<std::endl;
    return high_size>low_size&&low_size>0&&(low_size+high_size)>((uint64_t)100<<30)&&(low_size+high_size)/low_size<=10;
  }

Status BlobStorage::Get(const ReadOptions &options, const BlobIndex &index,
                        BlobRecord *record, PinnableSlice *buffer) {
  auto sfile = FindFile(index.file_number).lock();
  if (!sfile) {
    if (db_options_.sep_before_flush) {
      return ReadBuildingFile(options, index, record);
    }
    return Status::Corruption("Missing blob file: " +
                              std::to_string(index.file_number));
  }
  if (cf_options_.level_merge && sfile->file_type() == kSorted) {
    record->only_value = true;
  } else {
    record->only_value = false;
  }
  return file_cache_->Get(options, sfile->file_number(), sfile->file_size(),
                          index.blob_handle, record, buffer);
}

Status BlobStorage::ReadBuildingFile(const ReadOptions &options,
                                     const BlobIndex &index,
                                     BlobRecord *record) {
  auto reader = FindBuildingFile(index.file_number).lock();
  if (!reader) {
    return Status::Corruption("File " + std::to_string(index.file_number) +
                              " not exist");
  }
  Slice blob;
  OwnedSlice buffer;
  CacheAllocationPtr ubuf(new char[index.blob_handle.size]);
  auto s = reader->Read(index.blob_handle.offset, index.blob_handle.size, &blob,
                        ubuf.get());
  if (!s.ok()) {
    return s;
  }
  if (index.blob_handle.size != static_cast<uint64_t>(blob.size())) {
    return Status::Corruption(
        "ReadRecord actual size: " + ToString(blob.size()) +
        " not equal to blob size " + ToString(index.blob_handle.size));
  }

  BlobDecoder decoder;
  s = decoder.DecodeHeader(&blob);
  if (!s.ok()) {
    return s;
  }
  buffer.reset(std::move(ubuf), blob);
  s = decoder.DecodeRecord(&blob, record, &buffer);
  return s;
}

Status BlobStorage::NewPrefetcher(uint64_t file_number,
                                  std::unique_ptr<BlobFilePrefetcher> *result) {
  auto sfile = FindFile(file_number).lock();
  if (!sfile)
    return Status::Corruption("Missing blob wfile: " +
                              std::to_string(file_number));
  return file_cache_->NewPrefetcher(
      sfile->file_number(), sfile->file_size(), result,
      sfile->file_type() == kSorted && cf_options_.level_merge);
}

Status BlobStorage::GetBlobFilesInRanges(const RangePtr *ranges, size_t n,
                                         bool include_end,
                                         std::vector<uint64_t> *files) {
  std::unique_lock<std::mutex> l(mutex_);
  for (size_t i = 0; i < n; i++) {
    const Slice *begin = ranges[i].start;
    const Slice *end = ranges[i].limit;
    auto cmp = cf_options_.comparator;

    std::string tmp;
    // nullptr means the minimum or maximum.
    for (auto it = ((begin != nullptr) ? blob_ranges_.lower_bound(*begin)
                                       : blob_ranges_.begin());
         it != ((end != nullptr) ? blob_ranges_.upper_bound(*end)
                                 : blob_ranges_.end());
         it++) {
      // Obsolete files are to be deleted, so just skip.
      if (it->second->is_obsolete()) continue;
      // The smallest and largest key of blob file meta of the old version are
      // empty, so skip.
      if (it->second->largest_key().empty() && end) continue;

      if ((end == nullptr) ||
          (include_end && cmp->Compare(it->second->largest_key(), *end) <= 0) ||
          (!include_end && cmp->Compare(it->second->largest_key(), *end) < 0)) {
        files->push_back(it->second->file_number());
        if (!tmp.empty()) {
          tmp.append(" ");
        }
        tmp.append(std::to_string(it->second->file_number()));
      }
      assert(it->second->smallest_key().empty() ||
             (!begin || cmp->Compare(it->second->smallest_key(), *begin) >= 0));
    }
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "Get %" PRIuPTR " blob files [%s] in the range [%s, %s%c",
        files->size(), tmp.c_str(), begin ? begin->ToString(true).c_str() : " ",
        end ? end->ToString(true).c_str() : " ", include_end ? ']' : ')');
  }
  return Status::OK();
}

std::weak_ptr<BlobFileMeta> BlobStorage::FindFile(uint64_t file_number) const {
  std::unique_lock<std::mutex> l(mutex_);
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    assert(file_number == it->second->file_number());
    return it->second;
  }
  return std::weak_ptr<BlobFileMeta>();
}

std::weak_ptr<RandomAccessFileReader> BlobStorage::FindBuildingFile(
    uint64_t file_number) const {
  std::unique_lock<std::mutex> l(mutex_);
  auto it = building_files_.find(file_number);
  if (it != building_files_.end()) {
    return it->second;
  }
  return std::weak_ptr<RandomAccessFileReader>();
}

void BlobStorage::ExportBlobFiles(
    std::map<uint64_t, std::weak_ptr<BlobFileMeta>> &ret) const {
  std::unique_lock<std::mutex> l(mutex_);
  for (auto &kv : files_) {
    ret.emplace(kv.first, std::weak_ptr<BlobFileMeta>(kv.second));
  }
}

void BlobStorage::AddBlobFile(std::shared_ptr<BlobFileMeta> &file) {
  std::unique_lock<std::mutex> l(mutex_);
  files_.emplace(std::make_pair(file->file_number(), file));
  blob_ranges_.emplace(std::make_pair(Slice(file->smallest_key()), file));
  AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
           file->file_size());
  AddStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
  if(file->file_type()==kSorted){
  level_blob_size_[file->file_level()].fetch_add(file->file_size());
  } else {
    level_blob_size_[cf_options_.num_levels].fetch_add(file->file_size());
  }
  if (db_options_.sep_before_flush) {
    building_files_.erase(file->file_number());
  }
}

Status BlobStorage::AddBuildingFile(uint64_t file_number) {
  std::shared_ptr<RandomAccessFileReader> reader;
  Status s;
  {
    std::unique_ptr<RandomAccessFile> file;
    auto file_name = BlobFileName(db_options_.dirname, file_number);
    s = env_->NewRandomAccessFile(file_name, &file, env_options_);
    if (!s.ok()) return s;
    if (db_options_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }
    reader.reset(new RandomAccessFileReader(std::move(file), file_name));
  }
  std::unique_lock<std::mutex> l(mutex_);
  building_files_.emplace(std::make_pair(file_number, reader));
  return s;
}

bool BlobStorage::MarkFileObsolete(uint64_t file_number,
                                   SequenceNumber obsolete_sequence) {
  std::unique_lock<std::mutex> l(mutex_);
  auto file = files_.find(file_number);
  if (file == files_.end()) {
    return false;
  }
  MarkFileObsoleteLocked(file->second, obsolete_sequence);
  return true;
}

void BlobStorage::MarkFileObsoleteLocked(std::shared_ptr<BlobFileMeta> file,
                                         SequenceNumber obsolete_sequence) {
  // mutex_.AssertHeld();

  obsolete_files_.push_back(
      std::make_pair(file->file_number(), obsolete_sequence));
  file->FileStateTransit(BlobFileMeta::FileEvent::kDelete);
  SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE,
           file->file_size() - file->discardable_size());
  SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
           file->file_size());
  SubStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
  AddStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           file->file_size());
  AddStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
  if(file->file_type()==kSorted){
    level_blob_size_[file->file_level()].fetch_sub(file->file_size());
  } else {
    level_blob_size_[cf_options_.num_levels].fetch_sub(file->file_size());
  }
}

bool BlobStorage::RemoveFile(uint64_t file_number) {
  // mutex_.AssertHeld();

  auto file = files_.find(file_number);
  if (file == files_.end()) {
    return false;
  }
  // Removes from blob_ranges_
  auto p = blob_ranges_.equal_range(file->second->smallest_key());
  for (auto it = p.first; it != p.second; it++) {
    if (it->second->file_number() == file->second->file_number()) {
      it = blob_ranges_.erase(it);
      break;
    }
  }
  SubStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
           file->second->file_size());
  SubStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
  files_.erase(file_number);
  file_cache_->Evict(file_number);
  return true;
}



std::vector<int> CountSortedRun(std::vector<std::shared_ptr<BlobFileMeta>>& files) {
  auto cmp = [](std::shared_ptr<BlobFileMeta>& f1, std::shared_ptr<BlobFileMeta>& f2){
    return f1->smallest_key()<f2->smallest_key();
  };
  std::sort(files.begin(), files.end(), cmp);
  std::vector<std::vector<BlobFileMeta*>> ss;
  for(auto& f:files){
    bool success = false;
    for(auto& s:ss){
      if(f->smallest_key()>s.back()->largest_key()){
        s.push_back(f.get());
        success = true;
        break;
      }
    }
    if(!success){
      ss.push_back(std::vector<BlobFileMeta*>());
      ss.back().push_back(f.get());
    }
  }
  std::vector<int> res;
  for(auto& s:ss) res.push_back(s.size());
  return res;
  /*
  if (files.empty()) return 0;
  // store and sort both ends of blob files to count sorted runs
  std::vector<std::pair<BlobFileMeta*, bool>> blob_ends;
  for (const auto& file : files) {
    blob_ends.emplace_back(std::make_pair(file.get(), true));
    blob_ends.emplace_back(std::make_pair(file.get(), false));
  }
  auto blob_ends_cmp = [](const std::pair<BlobFileMeta*, bool>& end1,
                          const std::pair<BlobFileMeta*, bool>& end2) {
    const std::string& key1 =
        end1.second ? end1.first->smallest_key() : end1.first->largest_key();
    const std::string& key2 =
        end2.second ? end2.first->smallest_key() : end2.first->largest_key();
    int cmp = key1.compare(key2);
    // when the key being the same, order largest_key before smallest_key
    if(cmp==0) {
      return end1.first == end2.first? end1.second : (!end1.second && end2.second);
    }
    return cmp < 0;
  };
  std::sort(blob_ends.begin(), blob_ends.end(), blob_ends_cmp);

  int cur_add = 0;
  int cur_remove = 0;
  int size = blob_ends.size();
  int marked = 0;
  int maxsr = 0;
  std::unordered_map<BlobFileMeta*, int> tmp;
  for (int i = 0; i < size; i++) {
    if (blob_ends[i].second) {
      ++cur_add;
      tmp[blob_ends[i].first] = cur_remove;
    } else {
      ++cur_remove;
      auto record = tmp.find(blob_ends[i].first);
      if(record == tmp.end()){
        std::cout<<"maybe bug in mark range merge\n";
        continue;
      }
      if (cur_add - record->second > maxsr) {
        maxsr = cur_add - record->second;
      }
    }
  }
  return maxsr;
  */
}

void BlobStorage::PrintFileStates() {
    std::unique_lock<std::mutex> l(mutex_);
    std::vector<int> numObsolete(cf_options_.num_levels);
    std::vector<int> numNeedMerge(cf_options_.num_levels);
    std::vector<int> numNeedGC(cf_options_.num_levels);
    std::vector<int> numFile(cf_options_.num_levels);
    std::vector<uint64_t> gc_discardable_size(cf_options_.num_levels);
    std::vector<uint64_t> nomark_discardable_size(cf_options_.num_levels);
    std::vector<uint64_t> merge_discardable_size(cf_options_.num_levels);
    std::vector<uint64_t> reach_without_mark(cf_options_.num_levels);
    uint64_t num_unsorted = 0;
    uint64_t discardable_unsorted = 0;
    uint64_t discardable_reach_unsorted = 0;
    std::vector<std::vector<std::shared_ptr<BlobFileMeta>>> files(cf_options_.num_levels);
    for (auto& file:files_){
      if(file.second->file_type()==kUnSorted){
        num_unsorted++;
        discardable_unsorted += file.second->discardable_size();
        if(file.second->GetDiscardableRatio()>cf_options_.blob_file_discardable_ratio){
          discardable_reach_unsorted += file.second->discardable_size();
        }
        continue;
      }
      int level = file.second->file_level();
      files[level].push_back(file.second);
      numFile[level]++;
      switch (file.second->file_state())
      {
      case BlobFileMeta::FileState::kObsolete:
        numObsolete[level]++;
        break;
      case BlobFileMeta::FileState::kToMerge:
        numNeedMerge[level]++;
        merge_discardable_size[level]+=file.second->discardable_size();
        break;
      case BlobFileMeta::FileState::kToGC:
        numNeedGC[level]++;
        gc_discardable_size[level]+=file.second->discardable_size();
        break;
      default:
        nomark_discardable_size[level]+=file.second->discardable_size();
        if(file.second->GetDiscardableRatio()>cf_options_.blob_file_discardable_ratio){
          reach_without_mark[level]++;
        }
        break;
      }
    }
    std::cout<<"~~~~~ overall ~~~~~\n";
    std::cout<<"num blob files"<<files_.size()<<", obsolete files in storage records:"<<obsolete_files_.size()<<std::endl;
    for(int i=0;i<cf_options_.num_levels;i++){
      std::cout << "~~~~~ level "<<i<<" ~~~~~~\n";
      std::cout<<"level "<<i<<" has "<<numFile[i]<<" files"<<std::endl;
      auto sr = CountSortedRun(files[i]);
      std::cout<<"level "<<i<<" has "<<sr.size()<<" sorted runs, each of them cotains"<<"[";
      for(int c:sr){
        std::cout<<c<<", ";
      }
      
      std::cout<<"] blob files"<<std::endl;
      std::cout<<"numBlobsolete files: "<<numObsolete[i]<<"\nnum need merge files: "<<numNeedMerge[i]<<"\nnum need gc files: "<<numNeedGC[i]<<"\ndiscardable size of need gc: "<<gc_discardable_size[i]<<"\ndiscardable size of need merge: "<<merge_discardable_size[i]<<"\ndiscardable size of no mark: "<<nomark_discardable_size[i]<<"."<<std::endl;
      std::cout<<"reach gc thresh but no mark:"<<reach_without_mark[i]<<std::endl;
    }
    std::cout << "~~~~~ unsorted file ~~~~~~\n";
    std::cout<<"discardable: "<<discardable_unsorted<<"\n discardable that reach threshold: "<<discardable_reach_unsorted<<std::endl;
  }

void BlobStorage::GetObsoleteFiles(std::vector<std::string> *obsolete_files,
                                   SequenceNumber oldest_sequence) {
  std::unique_lock<std::mutex> l(mutex_);

  uint32_t file_dropped = 0;
  uint64_t file_dropped_size = 0;
  for (auto it = obsolete_files_.begin(); it != obsolete_files_.end();) {
    auto &file_number = it->first;
    auto &obsolete_sequence = it->second;
    // We check whether the oldest snapshot is no less than the last sequence
    // by the time the blob file become obsolete. If so, the blob file is not
    // visible to all existing snapshots.
    if (oldest_sequence > obsolete_sequence) {
      // remove obsolete files
      bool __attribute__((__unused__)) removed = RemoveFile(file_number);
      assert(removed);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Obsolete blob file %" PRIu64 " (obsolete at %" PRIu64
                     ") not visible to oldest snapshot %" PRIu64 ", delete it.",
                     file_number, obsolete_sequence, oldest_sequence);
      if (obsolete_files) {
        obsolete_files->emplace_back(
            BlobFileName(db_options_.dirname, file_number));
      }

      it = obsolete_files_.erase(it);
      continue;
    }
    ++it;
  }
}

size_t BlobStorage::ComputeGCScore() {
  // TODO: no need to recompute all everytime
  std::unique_lock<std::mutex> l(mutex_);
  uint64_t start = 0;
  {
    TitanStopWatch sw(env_, start);
    gc_score_.clear();

    for (auto &file : files_) {
      if (file.second->is_obsolete() ||
          (cf_options_.level_merge && (file.second->file_type() == kSorted || file.second->GetDiscardableRatio() <
              cf_options_.blob_file_discardable_ratio))) {
        continue;
      }
      gc_score_.push_back({});
      auto &gcs = gc_score_.back();
      gcs.file_number = file.first;
      if (file.second->file_size() <
          cf_options_.merge_small_file_threshold /* ||
file.second->gc_mark()*/
      ) {
        // for the small file or file with gc mark (usually the file that just
        // recovered) we want gc these file but more hope to gc other file with
        // more invalid data
        gcs.score = cf_options_.blob_file_discardable_ratio;
      } else {
        gcs.score = file.second->GetDiscardableRatio();
      }
    }

    if (!cf_options_.level_merge) {
      std::sort(gc_score_.begin(), gc_score_.end(),
                [](const GCScore &first, const GCScore &second) {
                  return first.file_number < second.file_number;
                });
    } else {
      std::sort(gc_score_.begin(), gc_score_.end(),
                [](const GCScore &first, const GCScore &second) {
                  return first.score > second.score;
                });
    }
  }
  compute_gc_score += start;
  return gc_score_.size();
}

}  // namespace titandb
}  // namespace rocksdb
