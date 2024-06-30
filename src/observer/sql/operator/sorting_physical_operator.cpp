#include "sql/operator/sorting_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "storage/field/field.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

SortingPhysicalOperator::SortingPhysicalOperator(const Field& field):
  field_(field)   
{}

RC SortingPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (true) {
    ValueListTuple tuple;
    tuple.set_cells(child->current_tuple()->to_value_list());
    tuple_all_.push_back(tuple);
    rc = child->next();
    if (rc != RC::SUCCESS) break;
  }

  if (rc == RC::RECORD_EOF) {
    return child->close();
  }

  tuple_ = tuple_all_.begin();

  return rc;
}

RC SortingPhysicalOperator::next()
{
  ++tuple_;
  if (tuple_ == tuple_all_.end()) {
    return RC::RECORD_EOF;
  } else {
    return RC::SUCCESS;
  }
}

RC SortingPhysicalOperator::close()
{
  return RC::SUCCESS;
}

Tuple *SortingPhysicalOperator::current_tuple()
{
  if (tuple_ != tuple_all_.end()) {
    return &*tuple_;
  } else {
    return nullptr;
  }
}