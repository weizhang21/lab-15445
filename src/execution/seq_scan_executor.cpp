//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, 
                                 const SeqScanPlanNode *plan)
  : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())),
    table_iter_end_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
  auto out_schema = GetOutputSchema();
  while (table_iter_ != table_iter_end_) {
    auto cur_tuple = *table_iter_;
    ++table_iter_;
    auto predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(&cur_tuple, out_schema).GetAs<bool>()) {
      auto& cols = out_schema->GetColumns();
      std::vector<Value> values;
      uint32_t size = 0;
      for (auto& col : cols) {
        auto val = cur_tuple.GetValue(out_schema, out_schema->GetColIdx(col.GetName()));
        values.emplace_back(std::move(val));
        size += Type::GetTypeSize(col.GetType());
      }
      std::string storage;
      storage.reserve(sizeof(uint32_t) + size);
      storage.append((char*)(&size), sizeof(uint32_t));
      uint32_t offset = sizeof(uint32_t);
      for (auto& val : values) {
        val.SerializeTo(const_cast<char*>(storage.data()) + offset);
        offset += Type::GetTypeSize(val.GetTypeId());
      }
      tuple->DeserializeFrom(storage.data());
      rid->Set(cur_tuple.GetRid().GetPageId(),cur_tuple.GetRid().GetSlotNum());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
