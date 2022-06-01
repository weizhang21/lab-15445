//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
     plan_(plan),
     left_executor_(std::move(left_executor)),
     right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto schema = plan_->OutputSchema();
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  Tuple left_tuple, right_tuple;
  RID left_rid, right_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (!plan_->Predicate() ||
          plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
          std::vector<Value> values;
          values.reserve(schema->GetColumnCount());
          auto& cols = schema->GetColumns();
          for (auto& col : cols) {
            values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
          }
          *tuple = Tuple(values, schema);
          return true;
      };
    }   
  }
  return false;
}

}  // namespace bustub
