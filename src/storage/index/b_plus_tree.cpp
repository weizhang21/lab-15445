//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                              Transaction *transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  Page* root = buffer_pool_manager_->FetchPage(root_page_id_);
  Page* leaf_page = Search(key, root);
  if (root->GetPageId() != leaf_page->GetPageId()) {
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
  }
  BPlusTreeLeafPage<KVC>* leaf_node = 
        reinterpret_cast<BPlusTreeLeafPage<KVC>*>(leaf_page->GetData());                 
  int key_idx = leaf_node->KeyIndex(key, comparator_);
  if (key_idx == -1) {
    return false;
  }
  result->emplace_back(leaf_node->GetItem(key_idx).second);
  return !result->empty();
}

// todo: Concurrent
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::Search(const KeyType &key, Page* node) {
  char* data = node->GetData();
  BPlusTreePage* b_node = reinterpret_cast<BPlusTreePage*>(data);
  if (b_node->IsLeafPage()) {
    return node;
  }
  BPlusTreeInternalPage<INTERNAL_KVC>* in_node = 
      static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(b_node);
  page_id_t child_id = in_node->Lookup(key, comparator_);
  Page* child_page = buffer_pool_manager_->FetchPage(child_id);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return Search(key, child_page);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if (IsEmpty()) {
     StartNewTree(key, value);
     return true;
  }              
  return InsertIntoLeaf(key, value); 
}
/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 1, search which page should insert to 
  Page* root = buffer_pool_manager_->FetchPage(root_page_id_);
  Page* leaf_page = Search(key, root);
  if (root->GetPageId() != leaf_page->GetPageId()) {
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
  }
  BPlusTreeLeafPage<KVC>* leaf_node = 
        reinterpret_cast<BPlusTreeLeafPage<KVC>*>(leaf_page->GetData());
  
  // 2, check only support unique key
  if (leaf_node->KeyIndex(key, comparator_) != -1) {
    return false;
  }
  // 3, insert
  int size = leaf_node->Insert(key, value, comparator_);
  // 4, check if need to split
  if (size == leaf_max_size_) {
    BPlusTreeLeafPage<KVC>* new_node = 
        static_cast<BPlusTreeLeafPage<KVC>*>(Split<BPlusTreePage>(leaf_node));
    KeyType key = new_node->KeyAt(0);
    InsertIntoParent(leaf_node, key, new_node);
    buffer_pool_manager_->UnpinPage(new_node->GetNextPageId(), true);
  } 
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
// todo: consider split
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page* root = buffer_pool_manager_->NewPage(&root_page_id_);
  BUSTUB_ASSERT(root != nullptr, "out of memory when start new b plus tree");
  BPlusTreeLeafPage<KVC>* node = reinterpret_cast<BPlusTreeLeafPage<KVC>*>(root->GetData());
  node->Init(root->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);
  node->Insert(key, value, comparator_);
  root_page_id_ = root->GetPageId();
  UpdateRootPageId(0);
  buffer_pool_manager_->FlushPage(root->GetPageId());
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&page_id);
  BUSTUB_ASSERT(new_page != nullptr, "out of memory when split and new page");
  BPlusTreePage* bplus_page = static_cast<BPlusTreePage*>(node);
  if (bplus_page->IsLeafPage()) {
    BPlusTreeLeafPage<KVC>* leaf_node = static_cast<BPlusTreeLeafPage<KVC>*>(bplus_page);
    BPlusTreeLeafPage<KVC>* new_node = reinterpret_cast<BPlusTreeLeafPage<KVC>*>(new_page->GetData());
    new_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf_node->MoveHalfTo(new_node);
    new_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_node->GetPageId());
    return static_cast<N*>(new_node);
  } 
  BPlusTreeInternalPage<INTERNAL_KVC>* internal_node = 
    static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(bplus_page);
  BPlusTreeInternalPage<INTERNAL_KVC>* new_node = 
    reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(new_page->GetData());
    new_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    internal_node->MoveHalfTo(new_node, buffer_pool_manager_);
  return static_cast<N*>(new_node);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 1, get parent id and parent node
  page_id_t parent_id = old_node->GetParentPageId();
  Page* parent_page;
  BPlusTreeInternalPage<INTERNAL_KVC>* parent_node;
  if (parent_id == INVALID_PAGE_ID) {
    parent_page = buffer_pool_manager_->NewPage(&parent_id);
    parent_node = reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(parent_page->GetData());
    parent_node->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
    old_node->SetParentPageId(parent_id);
  } else {
    parent_page = buffer_pool_manager_->FetchPage(parent_id);
    parent_node = reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(parent_page->GetData());
  }
  new_node->SetParentPageId(parent_id);
  // 2, deal insert into parent
  // 2.1 new root page, polplate new root;
  // 2.2 not new root page , insert after old_value(page id of old node)
  if (parent_node->GetSize() == 0) {
    parent_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = parent_id;
    UpdateRootPageId(0);
  } else {
    // 2) if size > internal_max_size_ , split
    if (parent_node->GetSize() == internal_max_size_) {
      #define KPSIZE sizeof(std::pair<KeyType, page_id_t>)
      char* tmp = new char[KPSIZE + PAGE_SIZE];
      memcpy(tmp, parent_page->GetData(), PAGE_SIZE);
      BPlusTreeInternalPage<INTERNAL_KVC>* tmp_node = 
        reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(tmp);
      tmp_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  
      BPlusTreeInternalPage<INTERNAL_KVC>* new_internal_node = 
          static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(Split<BPlusTreePage>(tmp_node));
      KeyType key = new_internal_node->KeyAt(0);
      memcpy(parent_page->GetData(), tmp, PAGE_SIZE);
      delete[] tmp;
      InsertIntoParent(parent_node, key, new_internal_node);
    } else {
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    }
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

// find sibling's node
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage* BPLUSTREE_TYPE::FindSiblingRedistribute(BPlusTreePage* node, int max_size, bool* is_right) {
  if (node->IsRootPage()) {
    return nullptr;
  }
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  BPlusTreeInternalPage<INTERNAL_KVC>* parent_node =  
      reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(parent_page->GetData());
  int middle_val_index = parent_node->ValueIndex(node->GetPageId());
  page_id_t sib_page_id;
  Page* sib_page = nullptr;
  BPlusTreePage* sib_node =nullptr;

  // find sibling node
  // find left first
  if (middle_val_index > 0) {
    sib_page_id = static_cast<page_id_t>(parent_node->ValueAt(middle_val_index - 1));
    sib_page = buffer_pool_manager_->FetchPage(sib_page_id);
    sib_node = reinterpret_cast<BPlusTreePage*>(sib_page->GetData());
  }
  // then find right
  if (middle_val_index == 0 || sib_node->GetSize() + node->GetSize() <= max_size) {
    sib_page_id = static_cast<page_id_t>(parent_node->ValueAt(middle_val_index + 1));
    sib_page = buffer_pool_manager_->FetchPage(sib_page_id);
    sib_node = reinterpret_cast<BPlusTreePage*>(sib_page->GetData());
    *is_right = true;
  }
  // std::cout << "size: " << node->GetSize() << ", sib size = " << sib_node->GetSize() << std::endl;
  buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
  if (sib_node->GetSize() + node->GetSize() <= max_size) {
    buffer_pool_manager_->UnpinPage(sib_page_id, false);
    return nullptr;
  }
  return sib_node;
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage* BPLUSTREE_TYPE::FindSiblingCoalesce(BPlusTreePage* node,
        BPlusTreeInternalPage<INTERNAL_KVC>*& parent, bool* is_right) {
  if (node->IsRootPage()) {
    return nullptr;
  }
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  parent = reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(parent_page->GetData());

  int middle_val_index = parent->ValueIndex(node->GetPageId());
  page_id_t sib_page_id;
  Page* sib_page = nullptr;
  BPlusTreePage* sib_node =nullptr;
  // find sibling node and find left first
  if (middle_val_index > 0) {
    sib_page_id = static_cast<page_id_t>(parent->ValueAt(middle_val_index - 1));
    sib_page = buffer_pool_manager_->FetchPage(sib_page_id);
    sib_node = reinterpret_cast<BPlusTreePage*>(sib_page->GetData());
  } else {
    sib_page_id = static_cast<page_id_t>(parent->ValueAt(middle_val_index + 1));
    sib_page = buffer_pool_manager_->FetchPage(sib_page_id);
    sib_node = reinterpret_cast<BPlusTreePage*>(sib_page->GetData());
    *is_right = true;
  }

  return sib_node;
}

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 1, if tree is empty, return
  if (IsEmpty()) {
    return;
  }
  // 2, find the leaf page and remove the item
  Page* root = buffer_pool_manager_->FetchPage(root_page_id_);
  Page* leaf_page = Search(key, root);
  if (root->GetPageId() != leaf_page->GetPageId()) {
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
  }
  BPlusTreeLeafPage<KVC>* leaf_node = 
        reinterpret_cast<BPlusTreeLeafPage<KVC>*>(leaf_page->GetData());

  int idx = leaf_node->KeyIndex(key, comparator_);
  leaf_node->RemoveAndDeleteRecord(key, comparator_);
  // 3, The smallest key is deleted, should update parent node
  if (idx == 0 && leaf_node->GetParentPageId() != INVALID_PAGE_ID) {
    // We	don’t	care	about	this,	as long	as our node-fullness requirements are satisfied
    // Doesn’t affect	lookups	at all
  }
  // 4, coalesce Or redistribute
  CoalesceOrRedistribute(leaf_node);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, Transaction *transaction) {

  // std::cout << "-------  node_type = " << node->IsLeafPage() << ", size = ";
  // std::cout << node->GetSize() << "---------------" << std::endl;
  // 0, leaf node : at least ceil((max_size - 1)/ 2)
  //    internal node: at least ceil(max_size / 2) pointers
  if ((node->IsLeafPage() && node->GetSize() >= leaf_max_size_ / 2)
      || (node->GetSize() >= (internal_max_size_ + 1)/ 2)) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return;
  }

   // 1, if the size of sibling's node + node's size > max_size, sibling's node have surplus
  int max_size = node->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
  bool is_right = false; // sibling's node is right on node
  BPlusTreePage* sib_node = FindSiblingRedistribute(node, max_size, &is_right);
  if (sib_node != nullptr) {
    if (is_right) {
      // if index == 0, move right_node's first item into end of left_node
      Redistribute(node, sib_node, 0);
    } else {
      // else move left_node's last item into head of right_node
      Redistribute(sib_node, node, 1);
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sib_node->GetPageId(), true);
    return;
  }

  // 2, sibling's node don't have surplus , find sibling's node to coalesce
  BPlusTreeInternalPage<INTERNAL_KVC>* parent = nullptr;
  is_right = false;
  sib_node = FindSiblingCoalesce(node, parent, &is_right);
  // std::cout << "parent: page = " <<parent->GetPageId() << ", size =  " << parent->GetSize() << std::endl;
  // std::cout << "sibling: page = " <<sib_node->GetPageId();
  // std::cout <<  ", size =  " << sib_node->GetSize() << " sibling is right = " << is_right << std::endl;
  bool delete_parent = false;
  if (sib_node != nullptr) {
    if (is_right) {
      //std::cout << "sib is right" << std::endl;
      delete_parent = Coalesce(node, sib_node, parent, 0);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(sib_node->GetPageId());
    } else {
      //std::cout << "sib is left" << std::endl;
      delete_parent = Coalesce(sib_node, node, parent, 0);
      buffer_pool_manager_->UnpinPage(sib_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(node->GetPageId());
    }
  }
  // remove last key from b plus tree
  if(node->IsRootPage()) {
    AdjustRoot(node);
  } else if (delete_parent) {
    AdjustRoot(parent);
  } else if (parent != nullptr) {
    CoalesceOrRedistribute(parent);
  }
}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
// some assume: 
// neighbor_node is on node's right
// move all item from neighbor page to node page 
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Coalesce(BPlusTreePage *left_node, BPlusTreePage *right_node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  int val_idx = parent->ValueIndex(right_node->GetPageId());                              
  // leaf node
  if (left_node->IsLeafPage()) {
     BPlusTreeLeafPage<KVC>* left_leaf = static_cast<BPlusTreeLeafPage<KVC>*>(left_node);
     BPlusTreeLeafPage<KVC>* right_leaf = static_cast<BPlusTreeLeafPage<KVC>*>(right_node);
     right_leaf->MoveAllTo(left_leaf);
  } else {
    BPlusTreeInternalPage<INTERNAL_KVC>* left_internal = 
      static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(left_node);
    BPlusTreeInternalPage<INTERNAL_KVC>* right_internal = 
      static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(right_node);
    KeyType middle_key = parent->KeyAt(val_idx);
    right_internal->MoveAllTo(left_internal, middle_key, buffer_pool_manager_);
  }
  parent->Remove(val_idx);\
  // if parent size = 1 and is root page , should be delete;
  return parent->GetSize() == 1 && parent->IsRootPage();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
// some assume: 
// if index == 0, move right_node's first item into end of left_node
// else move left_node's last item into head of right_node  
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *left_node, BPlusTreePage *right_node, int index) {
  page_id_t parent_page_id = left_node->GetParentPageId();
  Page* parent = buffer_pool_manager_->FetchPage(parent_page_id);
  BPlusTreeInternalPage<INTERNAL_KVC>* parent_node = 
    reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(parent->GetData());
  
  int middle_key_idx = parent_node->ValueIndex(right_node->GetPageId());
  KeyType new_middle_key;

  if (left_node->IsLeafPage()) {
    BPlusTreeLeafPage<KVC>* left_leaf = static_cast<BPlusTreeLeafPage<KVC>*>(left_node);
    BPlusTreeLeafPage<KVC>* right_leaf = static_cast<BPlusTreeLeafPage<KVC>*>(right_node);
    if (index == 0) {
      right_leaf->MoveFirstToEndOf(left_leaf);
    } else {
      left_leaf->MoveLastToFrontOf(right_leaf);
    }
    new_middle_key = right_leaf->KeyAt(0);
  } else {
    BPlusTreeInternalPage<INTERNAL_KVC>* left_interval = 
        static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(left_node);
    BPlusTreeInternalPage<INTERNAL_KVC>* right_interval = 
        static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(right_node);
    
    // get middle key
    KeyType middle_key = parent_node->KeyAt(middle_key_idx); 
    if (index == 0) {
      // new mid key is head of neighbor
      new_middle_key = right_interval->KeyAt(1);
      right_interval->MoveFirstToEndOf(left_interval, middle_key, buffer_pool_manager_);
    } else {
      // new mid key is the last of node
      new_middle_key = left_interval->KeyAt(left_interval->GetSize() - 1);
      left_interval->MoveLastToFrontOf(right_interval, middle_key, buffer_pool_manager_);
    }  
  }

  parent_node->SetKeyAt(middle_key_idx, new_middle_key);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // root node is leaf node
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  } else if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // root node is internal node
    BPlusTreeInternalPage<INTERNAL_KVC>* internal_node =
        reinterpret_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(old_root_node);
    page_id_t child = internal_node->RemoveAndReturnOnlyChild();
    Page* new_root_page = buffer_pool_manager_->FetchPage(child);
    BPlusTreePage* new_root_node = reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child;
    UpdateRootPageId();
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  while (!node->IsLeafPage()) {
    BPlusTreeInternalPage<INTERNAL_KVC>* internal_node = 
        static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(node);
    page_id_t next_page_id = internal_node->ValueAt(0);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<BPlusTreeLeafPage<KVC>*>(node), 0, buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_); 
  Page* page = Search(key, root_page);
  BPlusTreeLeafPage<KVC>* node = reinterpret_cast<BPlusTreeLeafPage<KVC>*>(page->GetData());
  return INDEXITERATOR_TYPE(node, node->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  while (!node->IsLeafPage()) {
    BPlusTreeInternalPage<INTERNAL_KVC>* internal_node = 
        static_cast<BPlusTreeInternalPage<INTERNAL_KVC>*>(node);
    page_id_t next_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<BPlusTreeLeafPage<KVC>*>(node), node->GetSize(), buffer_pool_manager_); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
