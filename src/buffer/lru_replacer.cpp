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

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
  latch.lock();
  if (lru_map.empty()) {
    latch.unlock();
    return false; 
  }
  *frame_id = lru_list.back();
  lru_list.pop_back();
  lru_map.erase(*frame_id);
  latch.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch.lock();
  if (lru_map.count(frame_id)) {
      auto it = lru_map[frame_id];
      lru_map.erase(frame_id);
      lru_list.erase(it);
  }
  latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch.lock();
  if(lru_map.count(frame_id)) {
    latch.unlock();
    return;
  }
  if(lru_map.size() == capacity) {
    frame_id_t vic = lru_list.back();
    lru_map.erase(vic);
  }
  lru_list.push_front(frame_id);
  lru_map[frame_id] = lru_list.begin();
  latch.unlock();
}

size_t LRUReplacer::Size() { return lru_map.size(); }

}  // namespace bustub
