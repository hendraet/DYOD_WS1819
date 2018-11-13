#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/storage/fitted_attribute_vector.hpp"

namespace opossum {

class StorageFittedAttributeVectorTest : public ::testing::Test {};

TEST_F(StorageFittedAttributeVectorTest, GetSet) {
  auto vec = FittedAttributeVector<uint8_t>(5, ValueID(50));
  vec.set(0, ValueID(4));
  vec.set(3, ValueID(2));

  EXPECT_EQ(vec.get(0), 4u);
  EXPECT_EQ(vec.get(3), 2u);
}

TEST_F(StorageFittedAttributeVectorTest, Size) {
  const auto vec = FittedAttributeVector<uint8_t>(5, ValueID(50));

  EXPECT_EQ(vec.size(), 5u);
}

TEST_F(StorageFittedAttributeVectorTest, Width) {
  auto vec1 = FittedAttributeVector<uint8_t>(1, ValueID(50));
  EXPECT_EQ(vec1.width(), 1);

  auto vec2 = FittedAttributeVector<uint16_t>(1, ValueID(50));
  EXPECT_EQ(vec2.width(), 2);

  auto vec3 = FittedAttributeVector<uint32_t>(1, ValueID(50));
  EXPECT_EQ(vec3.width(), 4);
}

TEST_F(StorageFittedAttributeVectorTest, MakeSharedAttributeVector) {
  auto vec1 = make_shared_attribute_vector(5, ValueID(100));
  EXPECT_EQ(vec1->width(), 1);

  auto vec2 = make_shared_attribute_vector(5, ValueID(300));
  EXPECT_EQ(vec2->width(), 2);

  auto vec3 = make_shared_attribute_vector(5, ValueID(100000));
  EXPECT_EQ(vec3->width(), 4);
}

}  // namespace opossum
