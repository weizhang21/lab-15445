//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
#define KVC KeyType, ValueType, KeyComparator

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();

  IndexIterator(Page *page, int start, BufferPoolManager *buffer_pool_manager);

  bool isEnd() const;

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (cur_node_ == nullptr) {
      return itr.isEnd();
    }
    return cur_node_->GetPageId() == itr.GetPageId() && cur_idx_ == itr.GetIndex();
  }

  bool operator!=(const IndexIterator &itr) const {
    if (isEnd() && itr.isEnd()) {
      return false;
    }
    if (isEnd() || itr.isEnd()) {
      return true;
    }
    return cur_node_->GetPageId() != itr.GetPageId() || cur_idx_ != itr.GetIndex();
  }

  page_id_t GetPageId() const { return cur_node_->GetPageId(); }
  int GetIndex() const { return cur_idx_; }

 private:
  // add your own private member variables here
  Page *page_;
  BPlusTreeLeafPage<KVC> *cur_node_;
  int cur_idx_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
