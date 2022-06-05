//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
  : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_child_(std::move(left_child)),
    right_child_(std::move(right_child)) {}


HashJoinExecutor::~HashJoinExecutor() {
  for(auto& item : join_map) {
    delete item.second;
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple tuple;
  RID rid;
  auto col_expr = static_cast<ColumnValueExpression*>(const_cast<AbstractExpression*>(plan_->LeftJoinKeyExpression()));
  auto schema = const_cast<Schema*>(left_child_->GetOutputSchema());
  while(left_child_->Next(&tuple, &rid)) {
    Tuple* hash_tuple = new Tuple(tuple);
    HashJoinKey hash_key = {hash_tuple, schema, col_expr};
    join_map.emplace(hash_key, hash_tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) { 
  Tuple r_tuple;
  RID r_rid;
  auto schema = GetOutputSchema();
  auto right_schema = const_cast<Schema*>(right_child_->GetOutputSchema());
  auto col_expr = static_cast<ColumnValueExpression*>(const_cast<AbstractExpression*>(plan_->RightJoinKeyExpression()));
  while (right_child_->Next(&r_tuple, &r_rid)){
    HashJoinKey hash_key = {&r_tuple, right_schema, col_expr};
    if (join_map.find(hash_key) == join_map.end()) {
      continue;
    }

    std::vector<Value> values;
    values.reserve(schema->GetColumnCount());
    auto& cols = schema->GetColumns();
    for (auto& col : cols) {
      values.emplace_back(col.GetExpr()->EvaluateJoin(join_map.at(hash_key), 
          left_child_->GetOutputSchema(), &r_tuple, right_child_->GetOutputSchema()));
    }
    *tuple = Tuple(values, schema);
    return true;
  }
  return false; 
}

}  // namespace bustub
