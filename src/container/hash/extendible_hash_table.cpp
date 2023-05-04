//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>
#include <vector>

#include "binder/bound_table_ref.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bu = dir_[idx];
  Bucket *b = bu.get();
  if (b->Find(key, value)) {
    b = nullptr;
    return true;
  }
  b = nullptr;
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bu = dir_[idx];
  Bucket *b = bu.get();
  V value;
  if (b->Find(key, value)) {
    b->Remove(key);
    b = nullptr;
    return true;
  }
  b = nullptr;
  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //当要插入的桶满了时，如果桶的局部深度等于全局深度，则增加全局深度并将目录大小加倍。
  //增加桶的局部深度
  //拆分桶并重新分配目录指针和桶中的 kv 对。
  std::lock_guard<std::mutex> guard(latch_);
  while (dir_[IndexOf(key)]->IsFull()) {
    int id = IndexOf(key);
    auto target_bucket = dir_[id];
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      size_t size = dir_.size();
      dir_.resize(size << 1);
      for (size_t i = 0; i < size; i++) {
        dir_[i + size] = dir_[i];
      }
    }

    int mask = 1 << target_bucket->GetDepth();
    target_bucket->IncrementDepth();
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth());
    auto l = target_bucket->GetItems();
    target_bucket->ClearItems();
    for (const auto &item : l) {
      size_t hashkey = std::hash<K>()(item.first);
      if ((hashkey & mask) != 0U) {
        one_bucket->Insert(item.first, item.second);
      } else {
        target_bucket->Insert(item.first, item.second);
      }
    }
    num_buckets_++;
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = one_bucket;
        }
      }
    }
  }
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  for (auto &item : target_bucket->GetItems()) {
    if (item.first == key) {
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto [k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      (*it).second = value;
      return true;
    }
  }
  if (list_.size() < size_) {
    list_.emplace_back(key, value);
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
