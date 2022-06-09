//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "include/execution/executor_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
}

void UpdateExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableInfo* table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo*> index_infos = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  
  const AbstractPlanNode* child = plan_->GetChildPlan();
  auto executor = ExecutorFactory::CreateExecutor(GetExecutorContext(), child);
  auto txn = exec_ctx_->GetTransaction();
  auto lock_ma = exec_ctx_->GetLockManager();
  while(child_executor_->Next(tuple, rid)) {
    auto update_tuple = GenerateUpdatedTuple(*tuple);
    if (table_info->table_->UpdateTuple(update_tuple, *rid, GetExecutorContext()->GetTransaction())) {
      lock_ma->Lock(txn, *rid, false);
      for(auto& index : index_infos) {
        IndexWriteRecord iwr(*rid, plan_->TableOid(), WType::UPDATE, *tuple, index->index_oid_, exec_ctx_->GetCatalog());
        txn->AppendTableWriteRecord(std::move(iwr));
        index->index_->DeleteEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());
        index->index_->InsertEntry(update_tuple, *rid, GetExecutorContext()->GetTransaction());
      }
    };
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
