#include "sql/operator/sorting_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "sql/expr/tuple_cell.h"
#include "storage/field/field.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include <algorithm>
#include <iostream>
#include <ostream>

struct StringException : public std::exception
{
   std::string s;
   StringException(std::string ss) : s(ss) {}
   ~StringException() throw () {} // Updated
   const char* what() const throw() { return s.c_str(); }
};

SortingPhysicalOperator::SortingPhysicalOperator(const Field& field):
  field_(field)   
{}

RC SortingPhysicalOperator::open(Trx *trx)
{
  LOG_DEBUG("physical sorting operator, child open");
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
    rc = child->next();
    if (rc != RC::SUCCESS) break;
    auto tuple = child->current_tuple();
    tuple_all_.push_back(tuple);
  }

  std::sort(tuple_all_.begin(), tuple_all_.end(), [&](Tuple* a, Tuple* b) {
    auto spec = TupleCellSpec(field_.table_name(), field_.field_name());
    Value cell_a, cell_b;
    a->find_cell(spec, cell_a);
    b->find_cell(spec, cell_b);
    return cell_a.compare(cell_b) <= 0;
  });

  tuple_iter_.reset();
  if (rc == RC::RECORD_EOF) { rc = RC::SUCCESS; }
  
  return rc;
}

RC SortingPhysicalOperator::next()
{
  if (!tuple_iter_.has_value()) {
    tuple_iter_ = tuple_all_.begin();
    return RC::SUCCESS;
  }
  if (tuple_iter_ == --tuple_all_.end()) {
    return RC::RECORD_EOF;
  } else {
    ++tuple_iter_.value();
    return RC::SUCCESS;
  }
}

RC SortingPhysicalOperator::close()
{
  std::cout << "close sorting operator" << std::endl;
  PhysicalOperator *child = children_[0].get();
  return child->close();
}

Tuple *SortingPhysicalOperator::current_tuple()
{
  if (tuple_iter_ != tuple_all_.end()) {
    std::cout << "current tuple: " << (**tuple_iter_)->to_string() << std::endl;
    return **tuple_iter_;
  } else {
    return nullptr;
  }
}