#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() {
  Assert(_input_left == nullptr || _input_left->_output != nullptr,
         "You need to execute the left input operator before executing this one.");
  Assert(_input_right == nullptr || _input_right->_output != nullptr,
         "You need to execute the right input operator before executing this one.");
  _output = _on_execute();
}

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  Assert(_output != nullptr, "You need to execute this operator before requesting its output.");
  return _output;
}

std::shared_ptr<const Table> AbstractOperator::_input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_right() const { return _input_right->get_output(); }

}  // namespace opossum
