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
      plan_(plan) {}

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableInfo* table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo*> index_infos = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  auto schema = &table_info->schema_;
  std::vector<Tuple> tuples{};
  if (plan_->IsRawInsert()) {
    auto& rows = plan_->RawValues();
    for(auto& row : rows) {
      tuples.emplace_back(row, schema);
    }
  } else {
    const AbstractPlanNode* child = plan_->GetChildPlan();
    auto executor = ExecutorFactory::CreateExecutor(GetExecutorContext(), child);
    Tuple tuple;
    RID rid;
    while(executor->Next(&tuple, &rid)) {
      tuples.emplace_back(tuple);
    }
  }
  for (auto& tuple : tuples) {
      RID rid;
      while(!table_info->table_->InsertTuple(tuple, &rid, GetExecutorContext()->GetTransaction()));     
      for (auto index : index_infos) {
        index->index_->InsertEntry(tuple, rid, GetExecutorContext()->GetTransaction()); 
      }
  }
  return false;
}

}  // namespace bustub
