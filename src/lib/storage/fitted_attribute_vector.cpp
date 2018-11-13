#include "fitted_attribute_vector.hpp"

#include <limits>
#include <memory>

namespace opossum {

template <typename T>
FittedAttributeVector<T>::FittedAttributeVector(const size_t size, const ValueID max_value) : _values(size) {
  Assert(ValueID{static_cast<uint32_t>(std::numeric_limits<T>::max())} >= max_value,
         "too many unique values for VariableAttributeVector");
}

template <typename T>
ValueID FittedAttributeVector<T>::get(const size_t i) const {
  return ValueID(static_cast<uint32_t>(_values.at(i)));
}

template <typename T>
void FittedAttributeVector<T>::set(const size_t i, const ValueID value_id) {
  Assert(ValueID{static_cast<uint32_t>(std::numeric_limits<T>::max())} >= value_id, "value_id can not be stored");

  _values.at(i) = static_cast<T>(value_id);
}

template <typename T>
size_t FittedAttributeVector<T>::size() const {
  return _values.size();
}

template <typename T>
AttributeVectorWidth FittedAttributeVector<T>::width() const {
  return static_cast<AttributeVectorWidth>(std::numeric_limits<T>::digits / 8);
}

std::shared_ptr<BaseAttributeVector> make_shared_attribute_vector(const size_t size, const ValueID max_value) {
  Assert(ValueID{static_cast<uint32_t>(std::numeric_limits<uint32_t>::max())} >= max_value,
         "too many unique values for AttributeVector");

  if (ValueID{static_cast<uint32_t>(std::numeric_limits<uint8_t>::max())} >= max_value) {
    return std::make_shared<FittedAttributeVector<uint8_t>>(size, max_value);
  } else if (ValueID{static_cast<uint32_t>(std::numeric_limits<uint16_t>::max())} >= max_value) {
    return std::make_shared<FittedAttributeVector<uint16_t>>(size, max_value);
  }

  return std::make_shared<FittedAttributeVector<uint32_t>>(size, max_value);
}

}  // namespace opossum
