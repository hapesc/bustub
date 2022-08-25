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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);

  if (page_id == INVALID_PAGE_ID || iter == page_table_.end()) {
    return false;
  }
  // update page_table
  auto frame_id = iter->second;
  page_table_.erase(iter);
  Page *page = &pages_[frame_id];
  // write to disk before delete the page
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->data_);
  }
  // delete the page and put it back to free_list
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  page_id_t page_id;
  frame_id_t frame_id;
  Page *page;

  for (auto item : page_table_) {
    page_id = item.first;
    frame_id = item.second;
    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->data_);
    }
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
  }
  // update page_table and free_list
  page_table_.erase(page_table_.begin(), page_table_.end());
  free_list_.erase(free_list_.begin(), free_list_.end());
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  //  0.   Make sure you call AllocatePage!
  //  1.   If all the pages in the buffer pool are pinned, return nullptr.
  //  2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  //  3.   Update P's metadata, zero out memory and add P to the page table.
  //  4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  size_t pinned_num = 0;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      if (pages_[i].pin_count_ != 0) {
        pinned_num++;
      }
    }
  }
  //  all pinned
  if (pinned_num == pool_size_) {
    return nullptr;
  }
  //  find from free list or replace a page
  frame_id_t victim_id;
  Page *page;
  if (!free_list_.empty()) {
    victim_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&victim_id)) {
      return nullptr;
    }
  }
  // find the origin page_id of the victim_id
  page_id_t origin_id;
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if (it->second == victim_id) {
      origin_id = it->first;
      page_table_.erase(it);
      break;
    }
  }
  // write to disk before delete the page
  page = &pages_[victim_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(origin_id, page->GetData());
  }
  // reset the page
  *page_id = AllocatePage();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
  // update the page_table
  page_table_.emplace(*page_id, victim_id);
  disk_manager_->ReadPage(*page_id, page->GetData());
  replacer_->Pin(victim_id);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  //  1.     Search the page table for the requested page (P).
  //  1.1    If P exists, pin it and return it immediately.
  //  1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //         Note that pages are always found from the free list first.
  //  2.     If R is dirty, write it back to the disk.
  //  3.     Delete R from the page table and insert P.
  //  4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  // P exists
  if (iter != page_table_.end()) {
    auto frame_id = iter->second;
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }
  // P does not exist
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

  } else {
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
  }
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  // delete R & insert P
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if (it->second == frame_id) {
      page_table_.erase(it);
      break;
    }
  }
  // update page_table
  page_table_.emplace(page_id, frame_id);
  // reset the page
  page->page_id_ = page_id;
  page->pin_count_ = 1;  // note:pin_count should be 1 because you want to use the page
  page->is_dirty_ = false;
  page->ResetMemory();
  disk_manager_->ReadPage(page_id, page->GetData());
  replacer_->Pin(frame_id);
  return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  //  0.   Make sure you call DeallocatePage!
  //  1.   Search the page table for the requested page (P).
  //  1.   If P does not exist, return true.
  //  2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  //  3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
  Page *page;
  if (iter != page_table_.end()) {
    auto frame_id = iter->second;
    page = &pages_[frame_id];

    if (page->GetPinCount() == 0) {
      if (page->IsDirty()) {
        disk_manager_->WritePage(page_id, page->GetData());
      }
      // delete P & return the page to the free list
      page->page_id_ = INVALID_PAGE_ID;
      page->is_dirty_ = false;
      page->pin_count_ = 0;
      page->ResetMemory();
      page_table_.erase(iter);
      free_list_.push_back(frame_id);
    } else {
      return false;
    }
  }
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    auto frame_id = iter->second;
    Page *page = &pages_[frame_id];
    // can be unpinned
    if (page->GetPinCount() > 0) {
      page->pin_count_--;
      if (is_dirty) {
        page->is_dirty_ = is_dirty;
      }
      replacer_->Unpin(frame_id);
      return true;
    }
  }
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
