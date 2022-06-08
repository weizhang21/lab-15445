//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

// deadlock prevention ==> wound - wait
// 1, older txn request newer txn lock: newer txn abort and older txn hold lock
// 2, newer txn reques older txn lock: wait

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // std::cout << "S-lock(" << txn->GetTransactionId() << ", " << rid << ")" << std::endl;
  latch_.lock();
  auto& req_q =  lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> lock(req_q.mutex);

  req_q.request_queue_.emplace_front(txn->GetTransactionId(), LockMode::SHARED);
  auto lock_req = req_q.request_queue_.begin();

  // if older txn request newer txn lock: newer txn abort
  for (auto iter = req_q.request_queue_.rbegin(); iter != req_q.request_queue_.rend(); ++iter) {
    if (txn->GetTransactionId() < iter->txn_id_ && iter->lock_mode_ == LockMode::EXCLUSIVE) {
      TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
    }
  }

  auto should_wait = [&]()-> bool {
    for (auto iter = req_q.request_queue_.rbegin(); iter != req_q.request_queue_.rend(); ++iter) {
      if (txn->GetState() != TransactionState::GROWING) return false;
      if (iter->txn_id_ == txn->GetTransactionId()) return false;
      if (iter->lock_mode_ == LockMode::EXCLUSIVE) return true;
    }
    return true;
  };

  while (should_wait()) {
    req_q.cv_.wait(lock);
  }

  if (txn->GetState() != TransactionState::GROWING) {
    req_q.request_queue_.erase(lock_req);
    req_q.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // std::cout << "S-lock(" << txn->GetTransactionId() << ", " << rid << "),  success" << std::endl;
  lock_req->granted_ = true;
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // std::cout << "X-lock(" << txn->GetTransactionId() << ", " << rid << ")" << std::endl;
  latch_.lock();

  auto& req_q =  lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> lock(req_q.mutex);

  req_q.request_queue_.emplace_front(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto lock_req = req_q.request_queue_.begin();
  
  // if older txn request newer txn lock: newer txn abort
  for (auto iter = req_q.request_queue_.begin(); iter != req_q.request_queue_.end(); ++iter) {
    if (txn->GetTransactionId() < iter->txn_id_) {
      TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
    }
  }
  

  auto should_wait = [&]()-> bool {
    if (txn->GetState() != TransactionState::GROWING) return false;
    return txn->GetTransactionId() != req_q.request_queue_.back().txn_id_;
  };

  while (should_wait()) {
    // std::cout << txn->GetTransactionId() << " X-lock waiting... ";
    // std::cout << "queue front is " << req_q.request_queue_.back().txn_id_ << std::endl;
    req_q.cv_.wait(lock);
  }

  if (txn->GetState() != TransactionState::GROWING) {
    req_q.request_queue_.erase(lock_req);
    req_q.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  
  // std::cout << "X-lock(" << txn->GetTransactionId() << ", " << rid << "),  success" << std::endl;
  lock_req->granted_ = true;
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  latch_.lock();
  auto& req_q =  lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> lock(req_q.mutex);
  bool has_s_lock = false;
  for (auto iter = req_q.request_queue_.begin(); iter != req_q.request_queue_.end(); ++iter) {
    if(iter->txn_id_ == txn->GetTransactionId()) {
      req_q.request_queue_.erase(iter);
      has_s_lock = true;
      txn->GetSharedLockSet()->erase(rid);
      break;
    }
  }
  if (!has_s_lock) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  
  // if older txn request newer txn lock: newer txn abort
  for (auto iter = req_q.request_queue_.begin(); iter != req_q.request_queue_.end(); ++iter) {
    if (txn->GetTransactionId() < iter->txn_id_) {
      TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
    }
  }

  req_q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto lock_req = req_q.request_queue_.begin();
  auto should_wait = [&]()-> bool {
    if (txn->GetState() != TransactionState::GROWING) return false;
    return txn->GetTransactionId() != req_q.request_queue_.back().txn_id_;
  };

  while (should_wait()) {
    // std::cout << txn->GetTransactionId() << " X-lock waiting... ";
    // std::cout << "queue front is " << req_q.request_queue_.front().txn_id_ << std::endl;
    req_q.cv_.wait(lock);
  }

  if (txn->GetState() != TransactionState::GROWING) {
    req_q.request_queue_.erase(lock_req);
    req_q.cv_.notify_all();
     txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  lock_req->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  //std::cout << "un-lock(" << txn->GetTransactionId() << ", " << rid << ")" << std::endl;
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto& req_q =  lock_table_.at(rid);

  std::unique_lock<std::mutex> lock(req_q.mutex);
  for(auto iter = req_q.request_queue_.begin(); iter != req_q.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      req_q.request_queue_.erase(iter);
      req_q.cv_.notify_all();
      if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (req_q.request_queue_.empty()) {
        lock_table_.erase(rid);
      }
      //std::cout << "un-lock(" << txn->GetTransactionId() << ", " << rid << "),  success" << std::endl;
      return true;
    }
  }
  req_q.cv_.notify_all();
  return false;
}

}  // namespace bustub
