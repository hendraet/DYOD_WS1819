#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  std::shared_ptr<const Table> _on_execute() override;

  class BaseTableScanImpl {
   public:
    virtual const std::shared_ptr<const Table> execute() const = 0;
  };

  template <typename T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    TableScanImpl(const std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
                  const AllTypeVariant search_value);

    const std::shared_ptr<const Table> execute() const override;

   protected:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;
    const ScanType _scan_type;
    const T _search_value;

    void _add_chunk(const std::shared_ptr<Table>& result_table, std::shared_ptr<PosList>& result_pos_list,
                    const std::shared_ptr<const Table>& referenced_table) const;

    void _scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                       const std::shared_ptr<ValueSegment<T>> segment) const;
    void _scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                       std::shared_ptr<DictionarySegment<T>> segment) const;
    void _scan_segment(std::shared_ptr<PosList> pos_list, const std::shared_ptr<ReferenceSegment> segment) const;

    bool _matches_search_value(const T& value) const;

    std::pair<ValueID, ValueID> _get_value_ids(const std::shared_ptr<DictionarySegment<T>> segment) const;
  };
};

}  // namespace opossum
