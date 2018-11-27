#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/value_segment.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.get_chunk(ChunkID{q}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

TEST_F(StorageTableTest, ColumnNames) { EXPECT_EQ(t.column_names(), std::vector<std::string>({"col_1", "col_2"})); }

TEST_F(StorageTableTest, EmplaceChunk) {
  {
    Chunk chunk;
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(t.column_type(ColumnID(0))));
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(t.column_type(ColumnID(1))));
    t.emplace_chunk(std::move(chunk));
    EXPECT_EQ(t.chunk_count(), 1u);
  }

  t.append({1, "Hallo"});
  EXPECT_EQ(t.chunk_count(), 1u);

  {
    Chunk chunk;
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(t.column_type(ColumnID(0))));
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(t.column_type(ColumnID(1))));
    t.emplace_chunk(std::move(chunk));
    EXPECT_EQ(t.chunk_count(), 2u);
  }
}
}  // namespace opossum
