/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int start, BufferPoolManager *buffer_pool_manager)
    : page_(page), cur_node_(nullptr), cur_idx_(start), buffer_pool_manager_(buffer_pool_manager) {
  if (page != nullptr) {
    cur_node_ = reinterpret_cast<BPlusTreeLeafPage<KVC> *>(page->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const { return cur_node_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return cur_node_->GetItem(cur_idx_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  cur_idx_++;
  if (cur_idx_ == cur_node_->GetSize()) {
    page_id_t next_page_id = cur_node_->GetNextPageId();
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_node_->GetPageId(), false);
    if (next_page_id != INVALID_PAGE_ID) {
      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      next_page->RLatch();
      page_ = next_page;
      cur_node_ = reinterpret_cast<BPlusTreeLeafPage<KVC> *>(next_page->GetData());
      cur_idx_ = 0;
    } else {
      page_ = nullptr;
      cur_node_ = nullptr;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
