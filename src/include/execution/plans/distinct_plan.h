//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/plans/abstract_plan.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Distinct; }

  /** @return The child plan node */
  const AbstractPlanNode *GetChildPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

struct DistinctHashKey {
  const Tuple tuple;
  const Schema* schema;
  bool operator==(const DistinctHashKey &other) const {
    for (size_t idx = 0; idx < schema->GetColumnCount(); idx++) {
      auto val = tuple.GetValue(schema, idx);
      if (val.CompareEquals(other.tuple.GetValue(schema, idx)) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {
/** Implements std::hash on DistinctHashKey */
template <>
struct hash<bustub::DistinctHashKey> {
  std::size_t operator()(const bustub::DistinctHashKey &distinct_key) const {
    size_t curr_hash = 0;
    for (size_t idx = 0; idx < distinct_key.schema->GetColumnCount(); idx++) {
      auto val = distinct_key.tuple.GetValue(distinct_key.schema, idx);
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&val));
    }
    return curr_hash;
  }
};

} // namespace std
