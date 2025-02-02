//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances > 0 ? num_instances : 1),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size_, num_instances_, i, disk_manager_, log_manager_);
    buffer_pool_manager_.push_back(bpm);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (auto it : buffer_pool_manager_) {
    delete it;
  }
  buffer_pool_manager_.erase(buffer_pool_manager_.begin(), buffer_pool_manager_.end());
};

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances

  return pool_size_ * num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  auto pool_id = page_id % num_instances_;
  return buffer_pool_manager_.at(pool_id);
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto pool = GetBufferPoolManager(page_id);
  return pool->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto pool = GetBufferPoolManager(page_id);
  return pool->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto pool = GetBufferPoolManager(page_id);
  return pool->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lock(latch_);
  Page *p = nullptr;
  for (size_t i = 0; i <= num_instances_; i++) {
    auto pos = i % num_instances_;
    auto manager = buffer_pool_manager_.at(pos);
    p = manager->NewPage(page_id);
    if (p != nullptr) {
      break;
    }
  }
  return p;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto pool = GetBufferPoolManager(page_id);
  return pool->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto it : buffer_pool_manager_) {
    it->FlushAllPages();
  }
}

}  // namespace bustub
