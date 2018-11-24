#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../storage/dictionary_segment.hpp"
#include "../storage/reference_segment.hpp"
#include "../storage/value_segment.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

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
    BaseTableScanImpl() = default;

    virtual std::shared_ptr<const Table> execute() = 0;
  };

  template <typename T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    TableScanImpl(const std::shared_ptr<const Table> table, ColumnID column_id, const ScanType scan_type,
                  const AllTypeVariant search_value);

    // TODO: Mark return and method as const?
    std::shared_ptr<const Table> execute() override;

   protected:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;
    const ScanType _scan_type;
    const AllTypeVariant _search_value;

    void _scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                       const std::shared_ptr<ValueSegment<T>> segment) const;
    void _scan_segment(const ChunkID current_chunk_id, std::shared_ptr<PosList> pos_list,
                       std::shared_ptr<DictionarySegment<T>> segment) const;
    void _scan_segment(std::shared_ptr<PosList> pos_list, const std::shared_ptr<ReferenceSegment> segment) const;

    bool _matches_search_value(const T& value) const;
  };
};

}  // namespace opossum
