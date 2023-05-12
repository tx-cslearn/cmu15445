//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstdlib>

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (free_list_.empty() || !replacer_->IsEvict()) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->IsEvict()) {
    replacer_->Evict(&frame_id);
  }

  (*page_id) = AllocatePage();
  page_table_->Insert((*page_id), frame_id);

  return &pages_[(*page_id)];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { return nullptr; }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id) == false) {
    return false;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id) == false) {
    return false;
  }
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id) == false) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    FlushPgImp(page_id);
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  free_list_.emplace_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
