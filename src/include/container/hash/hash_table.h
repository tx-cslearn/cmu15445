//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_hash_table.h
//
// Identification: src/include/container/hash/hash_table.h
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * hash_table.h
 *
 * Abstract class for hash table implementation
 */

#pragma once

namespace bustub {

template <typename K, typename V>
class HashTable {
 public:
  HashTable() = default;
  virtual ~HashTable() = default;  //虚析构函数 父类指针析构子类对象
  // lookup and modifier
  virtual auto Find(const K &key, V &value) -> bool = 0;
  virtual auto Remove(const K &key) -> bool = 0;
  virtual void Insert(const K &key, const V &value) = 0;
};

}  // namespace bustub
