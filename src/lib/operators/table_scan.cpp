#include "table_scan.hpp"
#include "resolve_type.hpp"
#include "../storage/table.hpp"
#include "../storage/chunk.hpp"
#include "../storage/value_segment.hpp"
#include "../storage/dictionary_segment.hpp"
#include "../storage/reference_segment.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in) {
  const auto table = in->get_output();
  _table_scan_impl = make_unique_by_data_type<BaseTableScanImpl, TableScanImpl>(table->column_type(column_id));
}

TableScan::TableScanImpl::TableScanImpl(
  const std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
  const AllTypeVariant search_value) : _table(table), _column_id(column_id), _scan_type(scan_type),
                _search_value(type_cast<T>(search_value)) {

};

const std::shared_ptr<Table> TableScan::TableScanImpl::execute() const {
  const auto result_table = std::make_shared<Table>();
  Chunk result_chunk;

  for (auto chunk_index = ChunkID(0); chunk_index < _table->chunk_count(); ++chunk_index) {
    const Chunk& chunk = _table->get_chunk(chunk_index); // TODO: auto

    const auto& segment = chunk.get_segment(_column_id);

    const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    if (value_segment != nullptr) {
      _scan_segment(result_table, value_segment);
      continue;
    }
    /*
    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dict_segment != nullptr) {
      // TODO: Smartly iterate the dictionary.

      continue;
    }
    const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment<T>>(segment);
    if (reference_segment != nullptr) {

      continue;
    }
    */

    Fail("Type mismatch: Cannot cast table segments to type of search_value.");
  }
}

void TableScan::TableScanImpl::_scan_segment(std::shared_ptr<opossum::Table> table,
                                             std::shared_ptr<ValueSegment<T>> segment) const {
  for (const auto& value : segment->values()) {
    if (_matches_search_value(value)) {

    }
  }
}

bool TableScan::TableScanImpl::_matches_search_value(const T &value) const {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      return value == _search_value;
    }
    case ScanType::OpNotEquals: {
      return value != _search_value;
    }
    case ScanType::OpGreaterThan: {
      return value > _search_value;
    }
    case ScanType::OpGreaterThanEquals: {
      return value >= _search_value;
    }
    case ScanType::OpLessThan: {
      return value < _search_value;
    }
    case ScanType::OpLessThanEquals: {
      return value <= _search_value;
    }
    default: {
      Fail("Unknown scan type operator");
    }
  }
}

}  // namespace opossum
