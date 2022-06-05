//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "include/execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)){
    auto key = MakeAggregateKey(&tuple);
    auto val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, val);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto out_schema = GetOutputSchema();
  AggregateValueExpression agg_expr(false, 0, TypeId::BIGINT);
  while (aht_iterator_ != aht_.End()) {
    AggregateKey key = aht_iterator_.Key();
    AggregateValue val = aht_iterator_.Val();
    std::vector<Value> values{};
    for (auto& col : out_schema->GetColumns()) {
      Value v;
      v = col.GetExpr()->EvaluateAggregate(key.group_bys_, val.aggregates_);
      values.emplace_back(std::move(v));
    }
    *tuple = Tuple(values, out_schema);
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr && 
       !plan_->GetHaving()->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      continue;
    }
    return true;
  }
   return false;
  }

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
