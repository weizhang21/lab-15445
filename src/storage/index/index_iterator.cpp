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
INDEXITERATOR_TYPE::~IndexIterator() {
  buffer_pool_manager_->UnpinPage(cur_node_->GetPageId(), false);
};

 INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KVC>* leaf_node, int start,
            BufferPoolManager *buffer_pool_manager)
        : cur_node_(leaf_node),
          cur_idx_(start),
          buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() { 
  return cur_node_->GetNextPageId() == INVALID_PAGE_ID && 
         cur_idx_ == cur_node_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  return cur_node_->GetItem(cur_idx_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
  if (cur_idx_ == cur_node_->GetSize()) {
    page_id_t next_page_id = cur_node_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(cur_node_->GetPageId(), false);
    Page* next_page = buffer_pool_manager_->FetchPage(next_page_id);
    cur_node_ = reinterpret_cast<BPlusTreeLeafPage<KVC>*>(next_page->GetData());
    cur_idx_ = 0;
  }
  cur_idx_ ++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
