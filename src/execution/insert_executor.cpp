//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "include/execution/executor_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableInfo* table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo*> index_infos = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  auto schema = &table_info->schema_;
  if (plan_->IsRawInsert()) {
    auto& rows = plan_->RawValues();
    for(auto& row : rows) {
      *tuple = Tuple(row, schema);
      if (table_info->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction())) {
        for (auto index : index_infos) {
        index->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction()); 
        }
      }
    }
  } else {
    while(child_executor_->Next(tuple, rid)) {
      if (table_info->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction())) {
        for (auto index : index_infos) {
          index->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction()); 
        }
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
