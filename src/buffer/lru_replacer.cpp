//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  WriteLock write_lock(mutex_);
  if (replacer_.empty()) {
    return false;
  }
  *frame_id = replacer_.front();
  replacer_.pop_front();
  map_.erase(map_.find(*frame_id));
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  WriteLock write_lock(mutex_);
  auto iter = map_.find(frame_id);
  if (iter != map_.end()) {
    replacer_.erase(iter->second);
    map_.erase(iter);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  WriteLock write_lock(mutex_);
  auto iter = map_.find(frame_id);
  if (iter != map_.end()) {
    return;
  }
  if (replacer_.size() < max_pages_) {
    replacer_.push_back(frame_id);
    auto it = --replacer_.end();
    map_.emplace(frame_id, it);
  }
}

auto LRUReplacer::Size() -> size_t {
  ReadLock read_lock(mutex_);
  return replacer_.size();
}

}  // namespace bustub
