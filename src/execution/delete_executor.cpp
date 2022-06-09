//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "include/execution/executor_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
  TableInfo* table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo*> index_infos = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto lock_ma = exec_ctx_->GetLockManager();
  while (child_executor_->Next(tuple, rid)) {
    if (table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      lock_ma->Lock(txn, *rid, false);
      for (auto& index : index_infos) {
        IndexWriteRecord iwr(*rid, plan_->TableOid(), WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog());
        txn->AppendTableWriteRecord(std::move(iwr));
        index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      }
    };
  }
  return false;
}

}  // namespace bustub
