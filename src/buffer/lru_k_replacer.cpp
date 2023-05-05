//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <bits/types/time_t.h>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (!history_list_.empty()) {
    for (size_t i = 0; i < history_list_.size(); i++) {
      frame_id_t id = history_list_[i];
      auto it = all_.find(id);
      if ((*it).second->evictable_ == false) {
        continue;
      }
      *frame_id = id;
      history_list_.erase(history_list_.begin() + i);
      all_.erase(it);
      curr_size_--;
      return true;
    }
  }

  for (int i = cache_list_.size() - 1; i >= 0; i--) {
    frame_id_t id = cache_list_[i];
    auto it = all_.find(id);
    if ((*it).second->evictable_ == false) {
      continue;
    }
    *frame_id = id;
    cache_list_.erase(cache_list_.begin() + i);
    all_.erase(it);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  std::chrono::milliseconds t =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
  auto it = all_.find(frame_id);
  if (it == all_.end()) {
    all_[frame_id] = std::make_shared<FrameInfo>(t.count());
    history_list_.emplace_back(frame_id);
  } else {
    auto iteminfo = (*it).second;
    iteminfo->time_.emplace_back(t.count());
    if (iteminfo->is_cached_ == false && iteminfo->time_.size() >= k_) {
      iteminfo->is_cached_ = true;
      for (auto i = history_list_.begin(); i != history_list_.end(); i++) {
        if ((*i) == frame_id) {
          history_list_.erase(i);
          break;
        }
      }
      cache_list_.emplace_front(frame_id);
    } else if (iteminfo->is_cached_) {
      for (auto i = cache_list_.begin(); i != cache_list_.end(); i++) {
        if ((*i) == frame_id) {
          cache_list_.erase(i);
          break;
        }
      }
      cache_list_.emplace_front(frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  auto it = all_.find(frame_id);
  if (it == all_.end()) {
    throw std::exception();
  }
  auto iteminfo = (*it).second;
  if (iteminfo->evictable_ && !set_evictable) {
    iteminfo->evictable_ = set_evictable;
    curr_size_--;
  } else if (!iteminfo->evictable_ && set_evictable) {
    iteminfo->evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto it = all_.find(frame_id);
  if (it == all_.end()) {
    return;
  }
  auto iteminfo = (*it).second;
  if (!iteminfo->evictable_) {
    throw std::exception();
  }
  if (iteminfo->is_cached_) {
    for (auto i = cache_list_.begin(); i != cache_list_.end(); i++) {
      if ((*i) == frame_id) {
        cache_list_.erase(i);
      }
    }
  } else {
    for (auto i = history_list_.begin(); i != history_list_.end(); i++) {
      if ((*i) == frame_id) {
        history_list_.erase(i);
      }
    }
  }
  all_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
