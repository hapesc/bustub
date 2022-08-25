//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"
#include "include/common/logger.h"
namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

  using ReadLock = std::shared_lock<std::shared_mutex>;
  using WriteLock = std::lock_guard<std::shared_mutex>;

 private:
  size_t max_pages_;

  mutable std::shared_mutex mutex_;
  std::list<frame_id_t> replacer_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> map_;
};

}  // namespace bustub
