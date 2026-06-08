# GH-45946: Variant Decoding — Exhaustive Line-by-Line Breakdown

> **Branch**: `variant-decoding` (commit `e980fd0867`)
> **PR**: GH-45946: [C++][Parquet] Variant decoding
> **Spec**: [VariantEncoding.md](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
> **Tests**: 165/165 PASSED (`BUILD_WARNING_LEVEL=CHECKIN`)

---

## Table of Contents

1. [Formal Model](#1-formal-model)
2. [Architecture Overview](#2-architecture-overview)
3. [File: `variant_internal.h` — Public API Header](#3-file-variant_internalh--public-api-header)
4. [File: `variant_internal.cc` — Decoder Implementation](#4-file-variant_internalcc--decoder-implementation)
5. [File: `variant_test_util.h` — Test Infrastructure](#5-file-variant_test_utilh--test-infrastructure)
6. [File: `variant_internal_test.cc` — Test Suite](#6-file-variant_internal_testcc--test-suite)
7. [Build System Changes](#7-build-system-changes)
8. [Invariant Summary](#8-invariant-summary)
9. [Security Properties](#9-security-properties)

---

## 1. Formal Model

### 1.1 Variant Binary Encoding as a Grammar

The Variant binary format can be expressed as a context-free grammar over byte sequences:

```
Variant      ::= Metadata × Value
Metadata     ::= Header DictSize Offsets[DictSize+1] StringData
Value        ::= Primitive | ShortString | Object | Array

Header_meta  ::= byte  where  bits[0:3] = version ∈ {1}
                               bit[4] = is_sorted ∈ {0,1}
                               bit[5] = reserved = 0
                               bits[6:7] = offset_size - 1 ∈ {0,1,2,3}

Primitive    ::= header:byte × payload
                 where  header[0:1] = 0b00 (basic_type = 0)
                        header[2:7] = primitive_type_id ∈ [0..20]

ShortString  ::= header:byte × data[len]
                 where  header[0:1] = 0b01 (basic_type = 1)
                        header[2:7] = len ∈ [0..63]

Object       ::= header:byte × NumFields × FieldIDs[n] × Offsets[n+1] × Data
                 where  header[0:1] = 0b10 (basic_type = 2)
                        header[2:3] = field_offset_size - 1
                        header[4:5] = field_id_size - 1
                        header[6]   = is_large
                        header[7]   = unused

Array        ::= header:byte × NumElements × Offsets[n+1] × Data
                 where  header[0:1] = 0b11 (basic_type = 3)
                        header[2:3] = field_offset_size - 1
                        header[4]   = is_large
                        header[5:7] = unused
```

### 1.2 Type System (21 Primitive Types)

Let `T` be the primitive type domain:

```
T = { Null, True, False, Int8, Int16, Int32, Int64, Double,
      Decimal4, Decimal8, Decimal16, Date, TimestampMicros,
      TimestampMicrosNTZ, Float, Binary, String, TimeNTZ,
      TimestampNanos, TimestampNanosNTZ, UUID }
```

Define the payload size function `σ: T → ℤ ∪ {⊥}`:

```
σ(Null) = σ(True) = σ(False) = 0
σ(Int8) = 1
σ(Int16) = 2
σ(Int32) = σ(Float) = σ(Date) = 4
σ(Int64) = σ(Double) = σ(Timestamp*) = σ(TimeNTZ) = 8
σ(Decimal4) = 5  (1 scale + 4 value)
σ(Decimal8) = 9  (1 scale + 8 value)
σ(Decimal16) = 17 (1 scale + 16 value)
σ(UUID) = 16
σ(Binary) = σ(String) = ⊥  (variable: 4-byte length prefix + data)
```

Total byte size for a primitive value: `1 + σ(type)` for fixed-length; `1 + 4 + len` for variable-length.

### 1.3 Recursion as a Bounded Tree Walk

The decoding process is a depth-first traversal of a tree `T = (V, E)` where:
- Leaf nodes ∈ {Primitive, ShortString}
- Interior nodes ∈ {Object, Array}
- `depth(v) ≤ kMaxNestingDepth = 128` ∀ v ∈ V

The traversal generates a sequence of visitor events (SAX model):

```
Events = { Null, Bool(b), Int8(v), ..., UUID(bytes),
           StartObject(n), FieldName(s), EndObject,
           StartArray(n), EndArray }
```

**Termination guarantee**: The traversal terminates because:
1. Each recursive call increments `depth` by 1
2. At `depth > 128`, the function returns `Status::Invalid`
3. Each container moves forward through the buffer (positive offset advances)
4. The buffer length is finite

### 1.4 Offset Validation Invariants

For **metadata**: String offsets `o₀, o₁, ..., oₙ` must satisfy:
- Monotonicity: `oᵢ ≤ oᵢ₊₁` ∀ i ∈ [0, n-1]
- Bounds: `oₙ ≤ |string_data|`

For **arrays**: Element offsets `o₀, o₁, ..., oₙ` must satisfy:
- Monotonicity: `oᵢ ≤ oᵢ₊₁` ∀ i ∈ [0, n-1]
- Bounds: `data_start + oₙ ≤ |buffer|`

For **objects**: Field offsets `o₀, o₁, ..., oₙ` must satisfy:
- **NOT** required to be monotonic (spec allows out-of-order value storage)
- Per-field bounds: `oᵢ ≤ oₙ` ∀ i ∈ [0, n-1]  (where `oₙ` = total_data_size)
- Total bounds: `data_start + oₙ ≤ |buffer|`

### 1.5 Bit Extraction Formulas

Given a full header byte `h`:

```
basic_type = h & 0x03                    (bits 0-1)
type_info  = (h >> 2) & 0x3F            (bits 2-7, the 6-bit "value_header")
```

For **objects** (basic_type = 2):
```
field_offset_size = (type_info & 0x03) + 1
field_id_size     = ((type_info >> 2) & 0x03) + 1
is_large          = ((type_info >> 4) & 0x01) ≠ 0
```

For **arrays** (basic_type = 3):
```
field_offset_size = (type_info & 0x03) + 1
is_large          = ((type_info >> 2) & 0x01) ≠ 0
```

**Critical distinction**: Array `is_large` is at `type_info` bit 2 (full byte bit 4), while Object `is_large` is at `type_info` bit 4 (full byte bit 6). The Go implementation had a bug reading the wrong bit for arrays (apache/arrow-go#839).

---

## 2. Architecture Overview

### 2.1 Design Pattern: SAX/Visitor (not DOM/Tree)

```
┌─────────────┐    events    ┌──────────────┐
│ DecodeValue │ ─────────── → │ VariantVisitor │
│ (recursive) │              │ (user impl)   │
└─────────────┘              └──────────────┘
       │
       │ reads
       ↓
┌─────────────────────────┐
│ Raw byte buffer (const) │
│ + VariantMetadata dict   │
└─────────────────────────┘
```

**Why visitor, not tree?**
- Zero heap allocation per-value (only containers allocate for offset vectors)
- Matches Arrow C++ idioms: `TypeVisitor`, `ArrayVisitor`, `ScalarVisitor`
- Streaming: can abort early without decoding remaining data
- Suitable for column-scan workloads (millions of rows)

### 2.2 Memory Model

- **Zero-copy strings**: `string_view` into the raw metadata/value buffer
- **Buffer lifetime**: Caller must ensure buffer outlives returned metadata/string_views
- **No UTF-8 validation**: Raw bytes passed through (documented design choice)
- **Heap allocation**: Only in `DecodeObject`/`DecodeArray` for `std::vector<uint32_t>` offset/ID vectors (TODO: `SmallVector` optimization)

### 2.3 Error Handling Strategy

Uses Arrow's `Status`/`Result<T>` pattern:
- `Status::Invalid(...)` for malformed input (spec violations, truncations, bounds errors)
- `Status::Cancelled(...)` for visitor-initiated abort
- `ARROW_RETURN_NOT_OK(...)` macro for early return on error (eliminates explicit `if (!s.ok())` checks)
- `ARROW_ASSIGN_OR_RAISE(var, expr)` for unwrapping `Result<T>`

---

## 3. File: `variant_internal.h` — Public API Header

### 3.1 File Header & Include Guard

```cpp
#pragma once
```
Modern include guard (preferred over `#ifndef`/`#define` in Arrow codebase).

### 3.2 Includes

```cpp
#include <cstdint>          // uint8_t, int32_t, int64_t
#include <string>           // std::string (for VariantBuilder dict)
#include <string_view>      // std::string_view (zero-copy)
#include <unordered_map>    // for VariantBuilder dictionary
#include <vector>           // for metadata strings, offset arrays

#include "arrow/result.h"   // Result<T> monad
#include "arrow/status.h"   // Status error type
#include "arrow/util/visibility.h"  // ARROW_EXPORT macro for shared library
```

Note: `<string>`, `<unordered_map>` are present because the header also declares `VariantBuilder` (encoder), which lives on the encoding branch but the header is shared.

### 3.3 Namespace

```cpp
namespace arrow::extension::variant_internal {
```

**Why `variant_internal`?** Originally `variant`, but Unity builds (which compile multiple `.cc` files into a single translation unit) collided with the factory function `arrow::extension::variant(std::shared_ptr<DataType>)` in `parquet_variant.cc`. The `_internal` suffix resolves the name collision while accurately describing the content: it implements the *internal* binary encoding format.

### 3.4 Constants

```cpp
constexpr uint8_t kVariantVersion = 1;
constexpr int32_t kMaxNestingDepth = 128;
```

- `kVariantVersion`: The only supported spec version. Forward-compatible: future versions will be rejected with a clear error.
- `kMaxNestingDepth`: Security guard. C++ default thread stack is 1-8 MB; each recursion level uses ~100-200 bytes of stack. At 128 levels × ~200 bytes = ~25 KB — well within limits. Prevents stack overflow from malicious input. Go doesn't need this because goroutine stacks grow dynamically (up to 1 GB).

### 3.5 Enumerations

```cpp
enum class BasicType : uint8_t {
  kPrimitive = 0,    // bits 0-1 = 00
  kShortString = 1,  // bits 0-1 = 01
  kObject = 2,       // bits 0-1 = 10
  kArray = 3,        // bits 0-1 = 11
};
```

Direct mapping from the 2-bit field in the header byte. Using `enum class` for type safety (no implicit integer conversion).

```cpp
enum class PrimitiveType : uint8_t {
  kNull = 0,               // 0 payload bytes
  kTrue = 1,               // 0 payload bytes
  kFalse = 2,              // 0 payload bytes
  kInt8 = 3,               // 1 byte
  kInt16 = 4,              // 2 bytes
  kInt32 = 5,              // 4 bytes
  kInt64 = 6,              // 8 bytes
  kDouble = 7,             // 8 bytes (IEEE 754)
  kDecimal4 = 8,           // 1 + 4 = 5 bytes
  kDecimal8 = 9,           // 1 + 8 = 9 bytes
  kDecimal16 = 10,         // 1 + 16 = 17 bytes
  kDate = 11,              // 4 bytes (days since epoch)
  kTimestampMicros = 12,   // 8 bytes (µs since epoch, UTC)
  kTimestampMicrosNTZ = 13,// 8 bytes (µs since epoch, no timezone)
  kFloat = 14,             // 4 bytes (IEEE 754 single)
  kBinary = 15,            // 4-byte length + data (variable)
  kString = 16,            // 4-byte length + data (variable)
  kTimeNTZ = 17,           // 8 bytes (µs since midnight)
  kTimestampNanos = 18,    // 8 bytes (ns since epoch, UTC)
  kTimestampNanosNTZ = 19, // 8 bytes (ns since epoch, no timezone)
  kUUID = 20,              // 16 bytes (big-endian)
};
```

21 types covering the full spec. Note that `kFloat` (14) comes *after* `kDouble` (7) — this is the spec's enumeration order, not sorted by size.

### 3.6 `VariantMetadata` Struct

```cpp
struct ARROW_EXPORT VariantMetadata {
  uint8_t version = 0;
  bool is_sorted = false;
  int32_t offset_size = 0;
  std::vector<std::string_view> strings;
};
```

This is the *parsed* representation of the metadata buffer. Key points:
- `strings` contains zero-copy `string_view`s into the original buffer
- `is_sorted` enables binary search in `FindMetadataKey`
- `offset_size` is recorded for debugging/inspection but not needed after parsing
- Lifetime: the `VariantMetadata` is only valid as long as the source buffer is alive

### 3.7 `DecodeMetadata` Function Signature

```cpp
ARROW_EXPORT Result<VariantMetadata> DecodeMetadata(const uint8_t* data, int64_t length);
```

Returns `Result<T>` (either a value or an error status). The Arrow convention is to use `Result<T>` when the function produces a value on success, and `Status` when it only reports success/failure.

### 3.8 Inline Header Utilities

```cpp
inline BasicType GetBasicType(uint8_t header) {
  return static_cast<BasicType>(header & 0x03);
}

inline PrimitiveType GetPrimitiveType(uint8_t header) {
  return static_cast<PrimitiveType>((header >> 2) & 0x3F);
}
```

These are `inline` because they're trivial (single bitwise operation), called in hot paths (per-value), and defined in the header for zero overhead.

The mask `0x3F` = `0b00111111` extracts 6 bits. For `BasicType`, `0x03` = `0b00000011` extracts 2 bits.

### 3.9 `PrimitiveValueSize` Signature

```cpp
ARROW_EXPORT int32_t PrimitiveValueSize(PrimitiveType primitive_type);
```

Returns the fixed payload size for a given primitive type, or `-1` for variable-length types (Binary, String). This enables O(1) size computation without reading the value data.

### 3.10 `VariantVisitor` Abstract Class

```cpp
class ARROW_EXPORT VariantVisitor {
 public:
  virtual ~VariantVisitor() = default;

  // 21 primitive callbacks
  virtual Status Null() = 0;
  virtual Status Bool(bool value) = 0;
  virtual Status Int8(int8_t value) = 0;
  // ... (all 21 types)
  virtual Status UUID(const uint8_t* bytes) = 0;

  // Container callbacks
  virtual Status StartObject(int32_t num_fields) = 0;
  virtual Status FieldName(std::string_view name) = 0;
  virtual Status EndObject() = 0;
  virtual Status StartArray(int32_t num_elements) = 0;
  virtual Status EndArray() = 0;
};
```

**Design decisions**:
- Pure virtual (`= 0`) — forces implementors to handle all types (compile-time completeness check)
- Returns `Status` — allows early termination (visitor can return error to abort traversal)
- `Decimal*` pass raw bytes + scale rather than constructing a `Decimal128` object (zero-copy, no Arrow dependency in the visitor interface beyond Status)
- `UUID` passes raw 16 bytes (big-endian per spec)
- No `ElementIndex` callback for arrays (sequential; use `GetArrayElement` for random access)

### 3.11 `DecodeVariantValue` Signature

```cpp
ARROW_EXPORT Status DecodeVariantValue(const VariantMetadata& metadata,
                                       const uint8_t* data, int64_t length,
                                       VariantVisitor* visitor);
```

The main entry point for decoding. Takes pre-parsed metadata (for string dictionary lookups) and drives the visitor through the value tree.

### 3.12 Random Access Utilities

```cpp
ARROW_EXPORT Result<BasicType> GetValueBasicType(const uint8_t* data, int64_t length);
ARROW_EXPORT Result<int32_t> GetObjectFieldCount(const uint8_t* data, int64_t length);
ARROW_EXPORT Result<int32_t> GetArrayElementCount(const uint8_t* data, int64_t length);
ARROW_EXPORT Result<int64_t> ValueSize(const uint8_t* data, int64_t length);

ARROW_EXPORT Status FindObjectField(const VariantMetadata& metadata,
                                    const uint8_t* data, int64_t length,
                                    std::string_view field_name,
                                    int64_t* field_offset, int64_t* field_size);

ARROW_EXPORT Status GetArrayElement(const uint8_t* data, int64_t length,
                                    int32_t index,
                                    int64_t* element_offset, int64_t* element_size);

ARROW_EXPORT Status GetObjectFieldAt(const VariantMetadata& metadata,
                                     const uint8_t* data, int64_t length,
                                     int32_t index, std::string_view* field_name,
                                     int64_t* field_offset, int64_t* field_size);

ARROW_EXPORT int32_t FindMetadataKey(const VariantMetadata& metadata,
                                     std::string_view key);
```

These provide O(1) or O(log n) access without full traversal — essential for the Parquet reader which needs to access specific shredded fields without decoding entire variant values.

- `FindObjectField`: O(n) for <32 fields, O(log n) for ≥32 fields (binary search)
- `GetArrayElement`: O(1) — direct offset table lookup
- `GetObjectFieldAt`: O(1) — positional field access
- `FindMetadataKey`: O(log n) if sorted, O(n) otherwise

---

## 4. File: `variant_internal.cc` — Decoder Implementation

### 4.1 Includes

```cpp
#include "arrow/extension/variant_internal.h"  // Own header (first, per convention)
#include <cstring>                              // std::memcpy for LE reads
#include "arrow/util/endian.h"                  // bit_util::FromLittleEndian
#include "arrow/util/logging_internal.h"        // DCHECK macros (debug assertions)
```

Note: `logging_internal.h` (not `logging.h`) is the convention for extension `.cc` files in Arrow. This avoids pulling in the full logging infrastructure in the public header.

### 4.2 Anonymous Namespace

```cpp
namespace {
// ... helpers ...
}  // namespace
```

All internal helper functions (not part of the public API) are in an anonymous namespace for internal linkage. This prevents symbol conflicts in Unity builds and allows the compiler to inline aggressively.

### 4.3 `ReadUnsignedLE` — Little-Endian Integer Reader

```cpp
inline uint32_t ReadUnsignedLE(const uint8_t* data, int32_t num_bytes) {
  uint32_t result = 0;
  std::memcpy(&result, data, num_bytes);
  result = bit_util::FromLittleEndian(result);
  if (num_bytes < 4) {
    result &= (static_cast<uint32_t>(1) << (num_bytes * 8)) - 1;
  }
  return result;
}
```

**Line-by-line analysis**:

1. `uint32_t result = 0;` — Zero-initialize to clear upper bytes (critical for `memcpy` of fewer than 4 bytes)
2. `std::memcpy(&result, data, num_bytes);` — Type-punning safe copy. On little-endian platforms, this directly loads the desired value. On big-endian, it loads bytes in the wrong order.
3. `result = bit_util::FromLittleEndian(result);` — No-op on little-endian. On big-endian, byte-swaps the full 32-bit word, placing the meaningful bytes in the correct positions.
4. `if (num_bytes < 4)` — When reading 1, 2, or 3 bytes, the upper bits may contain garbage from the byte-swap.
5. `result &= (1 << (num_bytes * 8)) - 1;` — Masks off upper bytes. For `num_bytes=1`: mask = `0xFF`. For `num_bytes=2`: mask = `0xFFFF`. For `num_bytes=3`: mask = `0xFFFFFF`.

**Correctness proof**: Let `b[0..n-1]` be the input bytes in memory (little-endian encoding).
- The desired value is `V = Σᵢ b[i] × 256ⁱ` for i ∈ [0, n-1].
- After `memcpy`, on LE: `result = V + garbage_in_upper_bytes` → mask removes garbage.
- After `memcpy`, on BE: `result` has bytes in reversed order → `FromLittleEndian` swaps them → mask removes garbage.

### 4.4 `ValidateOffsets` — Metadata String Offset Validation

```cpp
Status ValidateOffsets(const std::vector<uint32_t>& offsets, int64_t data_length) {
  for (size_t i = 1; i < offsets.size(); ++i) {
    if (offsets[i] < offsets[i - 1]) {
      return Status::Invalid("..non-decreasing..", i);
    }
  }
  if (!offsets.empty() && offsets.back() > static_cast<uint32_t>(data_length)) {
    return Status::Invalid("..exceeds data length..", offsets.back(), data_length);
  }
  return Status::OK();
}
```

Validates the two invariants from §1.4:
1. **Monotonicity check**: Iterates pairwise, O(n). Short-circuits on first violation.
2. **Bounds check**: Only checks the last offset (since monotonicity implies all prior offsets are ≤ last).

### 4.5 `DecodePrimitive` — The Primitive Type Switch

```cpp
Status DecodePrimitive(const uint8_t* data, int64_t length, int64_t offset,
                       uint8_t header, VariantVisitor* visitor, int64_t* bytes_consumed) {
  auto primitive_type = GetPrimitiveType(header);
  int64_t pos = offset + 1;  // skip header byte

  auto check_remaining = [&](int64_t needed) -> Status {
    if (pos + needed > length) {
      return Status::Invalid("Variant value: truncated primitive...");
    }
    return Status::OK();
  };

  switch (primitive_type) {
    case PrimitiveType::kNull:
      ARROW_RETURN_NOT_OK(visitor->Null());
      *bytes_consumed = 1;
      return Status::OK();
    // ... 20 more cases ...
  }
}
```

**Structure for each fixed-size type** (e.g., Int32):
```cpp
case PrimitiveType::kInt32: {
  ARROW_RETURN_NOT_OK(check_remaining(4));    // bounds check
  int32_t value;
  std::memcpy(&value, data + pos, 4);         // type-punning safe load
  value = bit_util::FromLittleEndian(value);  // endian conversion
  ARROW_RETURN_NOT_OK(visitor->Int32(value)); // visitor callback
  *bytes_consumed = 5;                        // 1 header + 4 payload
  return Status::OK();
}
```

**Pattern for variable-length types** (Binary, String):
```cpp
case PrimitiveType::kString: {
  ARROW_RETURN_NOT_OK(check_remaining(4));                    // need length field
  uint32_t str_length;
  std::memcpy(&str_length, data + pos, 4);
  str_length = bit_util::FromLittleEndian(str_length);
  ARROW_RETURN_NOT_OK(check_remaining(4 + (int64_t)str_length)); // need data too
  auto view = std::string_view(reinterpret_cast<const char*>(data + pos + 4), str_length);
  ARROW_RETURN_NOT_OK(visitor->String(view));                    // zero-copy!
  *bytes_consumed = 1 + 4 + (int64_t)str_length;
  return Status::OK();
}
```

**Decimal special case**:
```cpp
case PrimitiveType::kDecimal4: {
  ARROW_RETURN_NOT_OK(check_remaining(5));     // 1 scale + 4 value
  auto scale = static_cast<int32_t>(data[pos]);// scale byte
  // Note: scale not validated (lenient decoder; encoder validates ≤ 38)
  ARROW_RETURN_NOT_OK(visitor->Decimal4(data + pos + 1, scale));
  *bytes_consumed = 6;                         // 1 header + 1 scale + 4 value
  return Status::OK();
}
```

The comment about lenient scale validation is deliberate: the decoder accepts any scale value (0-255) to remain forward-compatible. The encoder enforces the spec constraint (scale ≤ 38).

### 4.6 `DecodeShortString` — Inline String (≤63 bytes)

```cpp
Status DecodeShortString(const uint8_t* data, int64_t length, int64_t offset,
                         uint8_t header, VariantVisitor* visitor,
                         int64_t* bytes_consumed) {
  int32_t str_len = (header >> 2) & 0x3F;  // bits 2-7 = length (max 63)
  int64_t pos = offset + 1;
  if (pos + str_len > length) {
    return Status::Invalid("...truncated short string...");
  }
  auto view = std::string_view(reinterpret_cast<const char*>(data + pos), str_len);
  ARROW_RETURN_NOT_OK(visitor->String(view));
  *bytes_consumed = 1 + str_len;
  return Status::OK();
}
```

Short strings encode the length in the header itself (6 bits = max 63). This avoids the 4-byte length prefix overhead for common short field values. The same `visitor->String()` callback is used for both short and long strings — the caller doesn't need to distinguish.

### 4.7 `DecodeObject` — Object Container Decoding

This is the most complex function (~80 lines). Let's trace through it:

#### 4.7.1 Header Parsing

```cpp
uint8_t type_info = (header >> 2) & 0x3F;
int32_t field_offset_size = (type_info & 0x03) + 1;        // 1-4 bytes
int32_t field_id_size = ((type_info >> 2) & 0x03) + 1;     // 1-4 bytes
bool is_large = ((type_info >> 4) & 0x01) != 0;            // bit 4 of type_info
int32_t num_fields_size = is_large ? 4 : 1;
```

Maps directly to the formal bit layout in §1.5. The `+ 1` converts from "size minus one" encoding to actual size.

#### 4.7.2 Read `num_fields`

```cpp
int64_t pos = offset + 1;
if (pos + num_fields_size > length) {
  return Status::Invalid("...truncated object num_fields...");
}
auto num_fields = static_cast<int32_t>(ReadUnsignedLE(data + pos, num_fields_size));
pos += num_fields_size;
```

When `is_large=false`, reads 1 byte (max 255 fields). When `is_large=true`, reads 4 bytes (max ~4 billion fields).

#### 4.7.3 Read Field IDs

```cpp
int64_t field_ids_size = static_cast<int64_t>(num_fields) * field_id_size;
if (pos + field_ids_size > length) { return Status::Invalid(...); }

std::vector<uint32_t> field_ids(num_fields);
for (int32_t i = 0; i < num_fields; ++i) {
  field_ids[i] = ReadUnsignedLE(data + pos, field_id_size);
  pos += field_id_size;
}
```

**NOTE comment**: "Per spec, field IDs must be in lexicographic order of corresponding key names. We do not validate this ordering here for performance."

This is a deliberate trade-off: validation would require O(n) string comparisons per object. Instead, `FindObjectField` relies on this invariant for binary search, with a documented assumption.

**TODO comment**: SmallVector optimization for heap allocation avoidance.

#### 4.7.4 Read Value Offsets

```cpp
int64_t offsets_size = (static_cast<int64_t>(num_fields) + 1) * field_offset_size;
if (pos + offsets_size > length) { return Status::Invalid(...); }

std::vector<uint32_t> value_offsets(num_fields + 1);
for (int32_t i = 0; i <= num_fields; ++i) {
  value_offsets[i] = ReadUnsignedLE(data + pos, field_offset_size);
  pos += field_offset_size;
}
```

Note: `num_fields + 1` offsets — the last offset gives the total data size (end sentinel).

#### 4.7.5 Non-Monotonic Offset Handling

```cpp
// Note: per spec, object field offsets are NOT required to be
// monotonically increasing because field values may be stored
// in a different order than field IDs.
```

This is a critical difference from arrays. The spec allows object field values to be stored in any physical order within the data region — the offset table provides random access regardless of physical layout.

#### 4.7.6 Per-Field Offset Bounds Validation

```cpp
int64_t total_data_size = static_cast<int64_t>(value_offsets[num_fields]);
if (data_start + total_data_size > length) { return Status::Invalid(...); }

for (int32_t i = 0; i < num_fields; ++i) {
  if (value_offsets[i] > static_cast<uint32_t>(total_data_size)) {
    return Status::Invalid("...field offset exceeds data size...");
  }
}
```

This is **defense-in-depth** that Go does not perform. Even though the total data fits in the buffer, individual field offsets could point beyond the valid data region if the variant is malformed. This prevents out-of-bounds reads during field decoding.

#### 4.7.7 Field Iteration

```cpp
ARROW_RETURN_NOT_OK(visitor->StartObject(num_fields));

for (int32_t i = 0; i < num_fields; ++i) {
  auto field_id = field_ids[i];
  if (field_id >= metadata.strings.size()) {
    return Status::Invalid("...field_id exceeds metadata dictionary...");
  }
  ARROW_RETURN_NOT_OK(visitor->FieldName(metadata.strings[field_id]));

  int64_t field_offset = data_start + value_offsets[i];
  int64_t consumed = 0;
  ARROW_RETURN_NOT_OK(DecodeValueAt(metadata, data, data_start + total_data_size,
                                    field_offset, visitor, &consumed, depth));
}

ARROW_RETURN_NOT_OK(visitor->EndObject());
*bytes_consumed = (data_start - offset) + total_data_size;
```

Key insight: `DecodeValueAt` is passed `data_start + total_data_size` as the effective `length`, **not** the full buffer length. This restricts recursive decoding to the object's data region, preventing reads beyond it.

The NOTE comment documents that consumed bytes are NOT validated against expected field size because non-monotonic offsets make per-field size inference unreliable.

### 4.8 `DecodeArray` — Array Container Decoding

Structurally similar to `DecodeObject` but simpler:

#### 4.8.1 Header Parsing

```cpp
uint8_t type_info = (header >> 2) & 0x3F;
int32_t field_offset_size = (type_info & 0x03) + 1;
bool is_large = ((type_info >> 2) & 0x01) != 0;  // bit 2, NOT bit 4
int32_t num_elements_size = is_large ? 4 : 1;
```

**The critical difference from objects**: `is_large` is at bit 2 of `type_info` (bit 4 of full byte), not bit 4. This is where the Go bug was.

#### 4.8.2 Monotonicity Validation

```cpp
for (int32_t i = 1; i <= num_elements; ++i) {
  if (value_offsets[i] < value_offsets[i - 1]) {
    return Status::Invalid("...not monotonically non-decreasing...");
  }
}
```

Unlike objects, arrays REQUIRE monotonic offsets because elements are stored sequentially. This is explicitly validated.

#### 4.8.3 Element Iteration

```cpp
ARROW_RETURN_NOT_OK(visitor->StartArray(num_elements));
for (int32_t i = 0; i < num_elements; ++i) {
  int64_t elem_offset = data_start + value_offsets[i];
  int64_t consumed = 0;
  ARROW_RETURN_NOT_OK(DecodeValueAt(metadata, data, data_start + total_data_size,
                                    elem_offset, visitor, &consumed, depth));
}
ARROW_RETURN_NOT_OK(visitor->EndArray());
```

Same pattern as objects: restricts recursive decoding to the array's data region.

### 4.9 `DecodeValueAt` — The Recursive Dispatcher

```cpp
Status DecodeValueAt(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
                     int64_t offset, VariantVisitor* visitor, int64_t* bytes_consumed,
                     int32_t depth) {
  if (offset >= length) {
    return Status::Invalid("...offset beyond buffer...");
  }
  if (depth > kMaxNestingDepth) {
    return Status::Invalid("...nesting depth exceeds maximum...");
  }

  uint8_t header = data[offset];
  auto basic_type = GetBasicType(header);

  switch (basic_type) {
    case BasicType::kPrimitive:
      return DecodePrimitive(data, length, offset, header, visitor, bytes_consumed);
    case BasicType::kShortString:
      return DecodeShortString(data, length, offset, header, visitor, bytes_consumed);
    case BasicType::kObject:
      return DecodeObject(metadata, data, length, offset, header, visitor,
                          bytes_consumed, depth + 1);
    case BasicType::kArray:
      return DecodeArray(metadata, data, length, offset, header, visitor,
                         bytes_consumed, depth + 1);
    default:
      return Status::Invalid("...unknown basic type...");
  }
}
```

This is the core recursion point. Note:
- `depth + 1` for containers (each nested container increases depth)
- Primitives and short strings don't increment depth (they're leaves)
- The `default` case handles any future basic_type values (forward-compatible rejection)

### 4.10 `PrimitiveValueSize` — Fixed-Size Lookup Table

```cpp
int32_t PrimitiveValueSize(PrimitiveType primitive_type) {
  switch (primitive_type) {
    case PrimitiveType::kNull:
    case PrimitiveType::kTrue:
    case PrimitiveType::kFalse:
      return 0;
    case PrimitiveType::kInt8:
      return 1;
    // ... etc ...
    case PrimitiveType::kBinary:
    case PrimitiveType::kString:
      return -1;  // variable length
    default:
      return -1;
  }
}
```

Returns `-1` for variable-length types and unknown types. This signals `ValueSize()` to read the 4-byte length prefix instead.

### 4.11 `DecodeMetadata` — Metadata Buffer Parser

```cpp
Result<VariantMetadata> DecodeMetadata(const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("buffer is null or empty");
  }

  uint8_t header = data[0];
  uint8_t version = header & 0x0F;
  if (version != kVariantVersion) {
    return Status::Invalid("unsupported version...");
  }

  // Bit 5 is reserved — MUST be zero in v1
  if ((header >> 5) & 0x01) {
    return Status::Invalid("reserved bit 5 is set");
  }

  bool is_sorted = ((header >> 4) & 0x01) != 0;
  int32_t offset_size = ((header >> 6) & 0x03) + 1;
  // ... read dict_size, offsets, validate, extract string_views ...
}
```

**Reserved bit 5 enforcement**: This is stricter than Go. The rationale: if a future spec version uses bit 5, older decoders should explicitly reject the data (clean failure) rather than silently misinterpreting it.

### 4.12 `DecodeVariantValue` — Public Entry Point

```cpp
Status DecodeVariantValue(const VariantMetadata& metadata, const uint8_t* data,
                          int64_t length, VariantVisitor* visitor) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("buffer is null or empty");
  }
  DCHECK_NE(visitor, nullptr);
  int64_t bytes_consumed = 0;
  return DecodeValueAt(metadata, data, length, 0, visitor, &bytes_consumed, /*depth=*/0);
}
```

`DCHECK_NE(visitor, nullptr)` is a debug-only assertion (no-op in release builds). It documents the precondition without imposing runtime cost. This is an Arrow convention: use `DCHECK` for programmer errors, `Status::Invalid` for user input errors.

### 4.13 `ValueSize` — Total Value Byte Count

```cpp
Result<int64_t> ValueSize(const uint8_t* data, int64_t length) {
  uint8_t header = data[0];
  auto basic_type = GetBasicType(header);
  uint8_t type_info = (header >> 2) & 0x3F;

  switch (basic_type) {
    case BasicType::kShortString:
      return 1 + static_cast<int64_t>(type_info);  // header + inline length

    case BasicType::kObject: { /* parse header to compute total */ }
    case BasicType::kArray:  { /* parse header to compute total */ }
    case BasicType::kPrimitive: {
      auto ptype = static_cast<PrimitiveType>(type_info);
      int32_t payload_size = PrimitiveValueSize(ptype);
      if (payload_size >= 0) return 1 + payload_size;
      // Variable-length: read 4-byte length prefix
      uint32_t var_len;
      std::memcpy(&var_len, data + 1, 4);
      var_len = bit_util::FromLittleEndian(var_len);
      return 1 + 4 + static_cast<int64_t>(var_len);
    }
  }
}
```

This function does NOT recursively validate contents — it only reads the header and offset table to compute the total byte footprint. O(1) for primitives and short strings, O(1) for containers (reads only the last offset).

**Go bug reference** (in the Array case):
```cpp
// Note: Go's valueSize() in arrow-go (prior to fix PR) incorrectly
// used (typeInfo >> 4) for arrays, which reads bit 6 — the object's
// is_large position.
bool is_large = ((type_info >> 2) & 0x01) != 0;
```

### 4.14 `FindObjectField` — Name-Based Field Lookup

```cpp
Status FindObjectField(const VariantMetadata& metadata, const uint8_t* data,
                       int64_t length, std::string_view field_name,
                       int64_t* field_offset, int64_t* field_size) {
  // ... parse header ...

  constexpr int32_t kBinarySearchThreshold = 32;

  auto get_key_at = [&](int32_t i) -> std::string_view { /* lookup from dict */ };
  auto get_value_offset = [&](int32_t i) -> int64_t { /* compute data offset */ };

  int32_t found_index = -1;

  if (num_fields < kBinarySearchThreshold) {
    // Linear scan
    for (int32_t i = 0; i < num_fields; ++i) {
      if (get_key_at(i) == field_name) { found_index = i; break; }
    }
  } else {
    // Binary search (keys in lex order per spec)
    int32_t lo = 0, hi = num_fields - 1;
    while (lo <= hi) {
      int32_t mid = lo + (hi - lo) / 2;
      auto key = get_key_at(mid);
      if (key == field_name) { found_index = mid; break; }
      else if (key < field_name) { lo = mid + 1; }
      else { hi = mid - 1; }
    }
  }

  if (found_index >= 0) {
    *field_offset = get_value_offset(found_index);
    ARROW_ASSIGN_OR_RAISE(auto size, ValueSize(data + *field_offset, length - *field_offset));
    *field_size = size;
  }
  return Status::OK();
}
```

**Key design decisions**:

1. **Threshold of 32**: Below this, linear scan is faster (no branch misprediction, better cache locality for small N). Above 32, binary search's O(log n) wins.

2. **`int32_t` for `lo`/`hi`**: Deliberate choice. The Go implementation uses `uint32` which wraps to `MaxUint32` when `mid == 0` and `hi = mid - 1`. Using signed integers avoids this class of bugs entirely.

3. **Not-found semantics**: Returns `Status::OK()` with `field_offset = -1` (not an error to search for a missing field).

4. **Out-of-range field ID handling**: `get_key_at` returns empty `string_view` for invalid IDs — safe degradation.

### 4.15 `GetArrayElement` — O(1) Array Random Access

```cpp
Status GetArrayElement(const uint8_t* data, int64_t length, int32_t index,
                       int64_t* element_offset, int64_t* element_size) {
  // ... parse header, validate index bounds ...
  auto elem_offset = ReadUnsignedLE(data + offset_start + index * field_offset_size,
                                    field_offset_size);
  *element_offset = data_start + elem_offset;
  ARROW_ASSIGN_OR_RAISE(auto size, ValueSize(data + *element_offset, length - *element_offset));
  *element_size = size;
  return Status::OK();
}
```

Direct offset table lookup — no iteration over preceding elements. This is why arrays have monotonic offsets: it enables O(1) random access.

### 4.16 `FindMetadataKey` — Dictionary Lookup

```cpp
int32_t FindMetadataKey(const VariantMetadata& metadata, std::string_view key) {
  if (metadata.is_sorted) {
    // Binary search
    int32_t lo = 0, hi = static_cast<int32_t>(metadata.strings.size()) - 1;
    while (lo <= hi) { /* standard binary search */ }
    return -1;
  }
  // Linear scan
  for (int32_t i = 0; i < metadata.strings.size(); ++i) {
    if (metadata.strings[i] == key) return i;
  }
  return -1;
}
```

The `is_sorted` flag in the metadata header enables O(log n) dictionary lookups. This is relevant when a producer knows the dictionary is sorted (e.g., because it sorted keys during encoding).

---

## 5. File: `variant_test_util.h` — Test Infrastructure

### 5.1 `RecordingVisitor`

```cpp
class RecordingVisitor : public VariantVisitor {
 public:
  std::vector<std::string> events;

  Status Null() override { events.push_back("Null"); return Status::OK(); }
  Status Bool(bool value) override {
    events.push_back(std::string("Bool(") + (value ? "true" : "false") + ")");
    return Status::OK();
  }
  // ... all 21 primitive types + containers ...
};
```

**Design**: Records every visitor callback as a human-readable string. Tests assert against the exact event sequence, making test failures immediately obvious.

**Why a separate header?**: Shared between decoder tests and builder (encoder) tests. Marked as test-only (not installed with the library).

---

## 6. File: `variant_internal_test.cc` — Test Suite

### 6.1 Test Helpers

#### `BuildMetadataBuffer`

```cpp
std::vector<uint8_t> BuildMetadataBuffer(const std::vector<std::string>& strings,
                                         bool sorted = false, int32_t offset_size = 1) {
  // Constructs a valid metadata buffer from a list of strings
  // Header → dict_size → offsets → string_data
}
```

Manually constructs the binary metadata format for testing. This is the "known-good encoder" used to verify the decoder.

#### `PrimitiveHeader`

```cpp
uint8_t PrimitiveHeader(PrimitiveType type) {
  return static_cast<uint8_t>(BasicType::kPrimitive) | (static_cast<uint8_t>(type) << 2);
}
```

Computes the header byte: `(type_id << 2) | 0b00`.

#### `BuildShortString`, `BuildObject`, `BuildArray`

Manually construct binary representations of each value type. These serve as reference encoders for decoder testing.

### 6.2 Test Categories (165 tests total)

| Category | Count | Coverage |
|----------|-------|----------|
| Metadata parsing | 15 | All offset sizes, sorted flag, error cases, non-monotonic |
| Primitive types | 21 | All types + boundary values + truncation |
| Short strings | 4 | Empty, simple, max (63), truncated |
| Objects | 5 | Empty, single, multiple, invalid ID, 3-byte offset |
| Arrays | 4 | Empty, single, heterogeneous, is_large (256 elements) |
| Nesting | 3 | Object-in-object, array-in-object, array-of-objects |
| Depth limit | 2 | Exceeds 128, succeeds at 50 |
| Visitor abort | 2 | Early abort, immediate abort |
| Spec bytes | 6 | Handcrafted byte sequences |
| ValueSize | 6 | All basic types + regression |
| Random access | 8 | Find field, array element, positional access |
| FindMetadataKey | 4 | Sorted/unsorted × found/not-found |
| Non-monotonic offsets | 2 | Spec-compliant out-of-order object values |
| Binary search | 4 | 40-field objects: middle, first, last, not-found |
| Variable-length ValueSize | 3 | Long string, binary, truncated |
| Unknown types | 2 | Unknown primitive, unknown in ValueSize |
| Array non-monotonic | 1 | Rejection of invalid offsets |
| Object offset bounds | 1 | Per-field offset > total_data_size |
| Empty metadata offset sizes | 1 | Empty dict with offset_size=4 |
| Error cases | 8 | Version 0, type mismatches, negative index, etc. |
| **Total** | **~165** | |

### 6.3 Notable Test Designs

#### Regression Test for Go Bug (apache/arrow-go#839)

```cpp
TEST_F(VariantValueSizeRegressionTest, LargeArrayIsLargeBit) {
  // 300 elements → triggers is_large=true
  // Verifies is_large at bit 4 of full byte, NOT bit 6
  std::vector<std::vector<uint8_t>> elements(300, {PrimitiveHeader(PrimitiveType::kNull)});
  auto data = BuildArray(elements, /*field_offset_size=*/2);

  uint8_t header = data[0];
  ASSERT_TRUE(((header >> 4) & 0x01) != 0);  // bit 4 = is_large for arrays
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(data.data(), data.size()));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}
```

This directly exercises the bit that Go got wrong.

#### Non-Monotonic Object Offsets (Spec-Compliant)

```cpp
TEST_F(VariantObjectNonMonotonicTest, NonMonotonicObjectOffsets) {
  // Values stored as [c, a, b] but offsets point correctly
  // Offsets: a->2, b->4, c->0, end->6
  // ...
}
```

Proves the decoder handles out-of-order value storage correctly.

#### Depth Limit

```cpp
TEST_F(VariantDepthTest, ExceedsMaxNestingDepth) {
  std::vector<uint8_t> inner = {PrimitiveHeader(PrimitiveType::kNull)};
  for (int i = 0; i < 130; ++i) {
    inner = BuildArray({inner}, /*field_offset_size=*/2);
  }
  ASSERT_RAISES(Invalid, DecodeVariantValue(...));
}
```

Wraps 130 nested arrays to exceed the 128 limit. Uses `field_offset_size=2` because each nesting level adds >4 bytes, exceeding 1-byte offset capacity.

#### Visitor Early Abort

```cpp
TEST_F(VariantAbortTest, VisitorAbortsEarly) {
  AbortingVisitor visitor(3);  // abort after 3rd event
  auto status = DecodeVariantValue(...);
  ASSERT_TRUE(status.IsCancelled());
  ASSERT_EQ(visitor.count, 3);  // exactly 3 events received
}
```

Verifies that `Status::Cancelled` propagates cleanly through recursion without UB.

---

## 7. Build System Changes

### 7.1 CMake (`cpp/src/arrow/CMakeLists.txt`)

```cmake
extension/variant_internal.cc
```

Added to the `arrow_srcs` list. This compiles `variant_internal.cc` into the `libarrow` shared library, making the decoder available to all Arrow consumers.

### 7.2 CMake (`cpp/src/arrow/extension/CMakeLists.txt`)

```cmake
set(CANONICAL_EXTENSION_TESTS bool8_test.cc json_test.cc uuid_test.cc
                              variant_internal_test.cc variant_builder_test.cc)
```

Added test file to the canonical extensions test binary (`arrow-canonical-extensions-test`).

### 7.3 Meson (`cpp/src/arrow/meson.build`)

```meson
'extension/variant_internal.cc',
```

Mirror of the CMake source addition.

### 7.4 Meson (`cpp/src/arrow/extension/meson.build`)

```meson
canonical_extension_tests = ['bool8_test.cc', 'json_test.cc', 'uuid_test.cc',
                             'variant_internal_test.cc', 'variant_builder_test.cc']
```

Mirror of the CMake test addition. Also includes:

```meson
# variant_internal.h: public API for variant binary encoding/decoding.
# "internal" refers to the binary encoding internals, not visibility.
'variant_internal.h',
```

Installs the header with a clarifying comment about the naming.

---

## 8. Invariant Summary

### Pre-conditions (caller must ensure)

| Invariant | Where enforced |
|-----------|----------------|
| Metadata buffer outlives `VariantMetadata` | Documented in header |
| Value buffer outlives visitor callbacks | Documented in header |
| `visitor != nullptr` | `DCHECK_NE` in `DecodeVariantValue` |
| Metadata parsed before value decode | API signature (takes `VariantMetadata`) |

### Post-conditions (decoder guarantees)

| Invariant | Mechanism |
|-----------|-----------|
| No reads beyond buffer bounds | `pos + needed > length` checks everywhere |
| No stack overflow from deep nesting | `kMaxNestingDepth = 128` guard |
| Metadata offsets are monotonically non-decreasing | `ValidateOffsets` |
| Array element offsets are monotonically non-decreasing | Loop in `DecodeArray` |
| Object field offsets are within data bounds | Per-field check in `DecodeObject` |
| Field IDs resolve to valid dictionary entries | `field_id >= metadata.strings.size()` check |
| Reserved bits in v1 metadata are zero | Bit 5 check in `DecodeMetadata` |
| Error propagation from visitor is clean | `ARROW_RETURN_NOT_OK` on every callback |

### Deliberately NOT validated (documented)

| Property | Reason |
|----------|--------|
| UTF-8 validity of strings | Performance; validated at higher level |
| Field ID lexicographic ordering | Performance; documented assumption |
| Decimal scale ≤ 38 | Forward-compatibility (lenient decoder) |
| Consumed bytes = expected field size (objects) | Non-monotonic offsets make this unreliable |
| Consumed bytes = expected element size (arrays) | Documented TODO for optional strict mode |

---

## 9. Security Properties

### 9.1 Threat Model

The decoder operates on **untrusted input** (Parquet files from arbitrary sources). The primary threats are:

1. **Stack overflow**: Mitigated by `kMaxNestingDepth = 128`
2. **Buffer over-read**: Every offset/size is bounds-checked before use
3. **Integer overflow**: Sizes stored as `int64_t` (max ~9.2 × 10¹⁸); offsets as `uint32_t` → `int64_t` for arithmetic
4. **Infinite loops**: Not possible (each recursion advances through finite buffer with positive offset)
5. **Denial of service via large allocations**: `std::vector<uint32_t>` allocations bounded by `num_fields` from buffer (TODO: SmallVector)

### 9.2 Comparison with Go

| Security Property | C++ Implementation | Go Implementation |
|-------------------|:------------------:|:-----------------:|
| Recursion depth limit | ✅ (128) | ❌ (unlimited, relies on goroutine stack growth) |
| Per-field offset bounds check | ✅ | ❌ |
| Reserved bit validation | ✅ | ❌ |
| Array monotonicity check | ✅ | ✅ |
| Binary search signed arithmetic | ✅ (int32_t) | ❌ (uint32 underflow risk) |
| Metadata offset validation | ✅ | ✅ |

### 9.3 Complexity Analysis

| Operation | Time Complexity | Space Complexity |
|-----------|:--------------:|:----------------:|
| `DecodeMetadata` | O(n) where n = dict_size | O(n) for string_view vector |
| `DecodeVariantValue` (full traversal) | O(V) where V = total values in tree | O(D + F_max) where D = depth, F_max = max fields |
| `ValueSize` | O(1) | O(1) |
| `FindObjectField` (< 32 fields) | O(n) | O(1) |
| `FindObjectField` (≥ 32 fields) | O(log n) | O(1) |
| `GetArrayElement` | O(1) | O(1) |
| `GetObjectFieldAt` | O(1) | O(1) |
| `FindMetadataKey` (sorted) | O(log n) | O(1) |
| `FindMetadataKey` (unsorted) | O(n) | O(1) |

---

*End of exhaustive breakdown. For the encoding branch, see `arrow_issue_45947_solution_breakdown.md`.*
