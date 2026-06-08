# GH-45946 Solution: Variant Binary Decoding

> **Status**: Implementation complete, PR submitted (#50121), CI passing after namespace fix
> **Branch**: `variant-decoding` on `qzyu999/arrow` (commit `e980fd0867`)
> **Namespace**: `arrow::extension::variant_internal`
> **Date**: 2026-06-05 (last updated 2026-06-11)
> **Tests**: 165/165 passing with `BUILD_WARNING_LEVEL=CHECKIN`

---

## 1. What Was Built and Why

### The Problem

Issue #45946 asks: given a Variant's two binary buffers (`metadata` + `value`), parse them according to the [Variant Encoding Spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) and make the contents accessible to C++ code.

Without this, Arrow knows a column *is* a Variant (thanks to #46104's schema plumbing) but has no way to *read* what's inside. The bytes are opaque.

### What Was Delivered

A standalone decoding library: `variant_internal.h` (public API) and `variant_internal.cc` (implementation). It:

1. Parses the metadata buffer → produces a string dictionary
2. Recursively traverses the value buffer → invokes typed callbacks via a visitor interface
3. Provides random-access utilities for field/element lookup without full traversal
4. Validates all inputs defensively → returns `Status::Invalid` on malformed data, never crashes
5. Includes `ValueSize()` for computing value byte sizes without decoding
6. Includes `FindMetadataKey()` for O(log n) dictionary lookup on sorted metadata

This is **not** wired into the Parquet read pipeline (that's a future step). It's a utility library that other code can call — matching how the Go reference implementation structured things.

---

## 2. Architectural Approach

### 2.1 Design Philosophy: SAX vs DOM

The fundamental choice for a recursive binary decoder:

| Approach | Analogy | Memory | Use Case |
|----------|---------|--------|----------|
| **DOM-style** (tree) | `json.loads()` → Python dict | O(n) — allocates tree | When you need random access |
| **SAX-style** (visitor) | XML SAX parser, event stream | O(1) — callbacks only | When scanning/streaming |

**We chose SAX/visitor** because:
- Arrow processes millions of rows — allocating a tree per variant value is wasteful
- The caller decides what to materialize (a JSON printer only needs strings, an aggregator only needs numbers)
- Arrow already uses this pattern extensively (`TypeVisitor`, `ArrayVisitor`, `ScalarVisitor`)
- The Go reference implementation also uses a callback/accessor pattern, not materialization

**Additionally**, random-access utilities (`FindObjectField`, `GetArrayElement`, `GetObjectFieldAt`) provide DOM-style navigation for cases where full traversal is unnecessary.

### 2.2 System Design

```
Public API (variant_internal.h):
├── DecodeMetadata()         — metadata bytes → VariantMetadata
├── DecodeVariantValue()     — value bytes + visitor → callbacks
├── GetValueBasicType()      — peek at type without full decode
├── GetObjectFieldCount()    — peek at object size
├── GetArrayElementCount()   — peek at array size
├── ValueSize()              — compute total byte size of a value
├── FindObjectField()        — O(1)/O(log n) field lookup by name
├── GetArrayElement()        — O(1) array element by index
├── GetObjectFieldAt()       — O(1) field by position
└── FindMetadataKey()        — dictionary key lookup

Internal Implementation (variant_internal.cc):
├── ReadUnsignedLE()         — safe 1-4 byte LE read
├── ValidateOffsets()        — monotonicity + bounds check
├── DecodeValueAt()          — recursive dispatch (core loop)
├── DecodePrimitive()        — 21 primitive types
├── DecodeShortString()      — inline strings ≤63 bytes
├── DecodeObject()           — field_ids + offsets + values
└── DecodeArray()            — offsets + elements
```

### 2.3 Data Flow: How Bytes Become Callbacks

```
Caller                          DecodeMetadata        DecodeVariantValue    Visitor
  |                                   |                       |               |
  |-- metadata_bytes, length -------->|                       |               |
  |<-- VariantMetadata{strings} ------|                       |               |
  |                                                           |               |
  |-- metadata, value_bytes, len, &visitor ------------------>|               |
  |                                                           |-- header=0x02 |
  |                                                           |   (Object)    |
  |                                                           |   StartObject(2) -->|
  |                                                           |   FieldName("name")->|
  |                                                           |   String("Alice") -->|
  |                                                           |   FieldName("age") ->|
  |                                                           |   Int32(30) -------->|
  |                                                           |   EndObject() ------>|
  |<-- Status::OK() ------------------------------------------|               |
```

---

## 3. Comparison with Go Reference Implementation

### 3.1 What the Go Implementation Does

| Go Component | Our C++ Equivalent | Notes |
|-------------|-------------------|-------|
| `Metadata` struct | `VariantMetadata` struct | Both: version, sorted, string dictionary |
| `Metadata.DecodeMetadata()` | `DecodeMetadata()` | Both: parse header + offsets + strings |
| `Value` interface | `VariantVisitor` abstract class | Different pattern (see below) |
| `GetBasicType()` | `GetBasicType()` | Identical logic |
| `GetPrimitiveType()` | `GetPrimitiveType()` | Identical logic |
| `Value.NumFields()` | `GetObjectFieldCount()` | Same purpose |
| `Value.NumElements()` | `GetArrayElementCount()` | Same purpose |
| `ObjectValue.ValueByKey()` | `FindObjectField()` | Binary search for ≥32 fields |
| `ArrayValue.Value(i)` | `GetArrayElement()` | O(1) index access |
| `ObjectValue.FieldAt(i)` | `GetObjectFieldAt()` | Positional access |
| `Metadata.IdFor()` | `FindMetadataKey()` | Binary/linear search |
| `valueSize()` | `ValueSize()` | Fixed bug in Go version |

### 3.2 Key Difference: Access Pattern

**Go**: Returns a `Value` interface with methods like `.AsInt32()`, `.Field(name)`, `.Element(i)`. The caller navigates lazily — each call parses on demand.

**C++ (ours)**: Uses a visitor that receives **all** values during a single traversal pass. Random access is available separately via standalone utility functions.

**Why we diverge here:**

1. **C++ doesn't have GC** — a Go `Value` can hold a slice reference cheaply. In C++ the equivalent would need `shared_ptr` or careful lifetime documentation, adding complexity.
2. **Arrow's existing pattern** — Arrow C++ consistently uses visitors for type dispatch.
3. **Performance for bulk decode** — when converting an entire Variant column to JSON or materializing into an Arrow struct array, you need to visit every value anyway. The visitor processes everything in a single pass with zero allocation.
4. **Both patterns are provided** — visitor for bulk traversal, standalone functions for random access. This gives callers flexibility without forcing either approach.

### 3.3 Deliberate Divergences from Go

| # | Divergence | Rationale |
|---|-----------|-----------|
| 1 | Recursion depth limit (`kMaxNestingDepth=128`) | C++ stack is 1-8 MB vs Go's 1 GB growable goroutine stack |
| 2 | Reserved bit 5 validation | We fail cleanly on future versions; Go silently accepts |
| 3 | Object field offset bounds validation | Defense against malformed/malicious input; Go does not check |
| 4 | `FindObjectField` uses `int32_t` for lo/hi | Avoids Go's unsigned underflow bug when `mid == 0` |
| 5 | No UTF-8 validation | Same as Go — responsibility of higher-level consumer |

### 3.4 Bugs Found in Go Implementation

During development, we found and fixed two bugs in `apache/arrow-go`:

1. **`valueSize()` array `is_large` bit position** — used `(typeInfo >> 4)` (reads object's is_large) instead of `(typeInfo >> 2)`. Fixed in apache/arrow-go#839.
2. **`ObjectValue.ValueByKey()` unsigned underflow** — binary search uses `uint32` for `j = mid - 1`, wraps to `MaxUint32` when `mid == 0`. Separate issue TBD.

---

## 4. File-by-File Explanation

### 4.1 `variant_internal.h` — The Contract

**Purpose**: Public header defining the types, enums, visitor interface, and function signatures that constitute the decoding API.

**Namespace**: `arrow::extension::variant_internal` (renamed from `variant` to avoid Unity build collision with the `arrow::extension::variant()` factory function in `parquet_variant.cc`).

**Key design decisions:**

1. **`ARROW_EXPORT` on public symbols** — required for shared library visibility on Windows/Linux.
2. **Enums are `enum class` (strongly typed)** — prevents accidental integer conversions. Values match the spec exactly.
3. **`VariantMetadata` uses `std::string_view`** — zero-copy references into the raw buffer. The caller must ensure the buffer outlives the metadata.
4. **`VariantVisitor` is pure virtual** — forces implementers to handle all types. No default "do nothing" methods that could silently swallow data.
5. **Random access via standalone functions** — `FindObjectField`, `GetArrayElement`, `GetObjectFieldAt` complement the visitor for cases where you need to navigate directly without full traversal.

### 4.2 `variant_internal.cc` — The Machinery

**Purpose**: All parsing logic. This file never allocates Arrow arrays — it's pure byte parsing + visitor dispatch.

**Structure:**

```
Anonymous namespace (internal helpers):
├── ReadUnsignedLE()       — read 1-4 byte LE integer
├── ValidateOffsets()      — check offset monotonicity
├── DecodeValueAt()        — recursive dispatch (forward-declared)
├── DecodePrimitive()      — 21 primitive types
├── DecodeShortString()    — inline strings
├── DecodeObject()         — parse field_ids + offsets + recurse
└── DecodeArray()          — parse offsets + recurse

Public API (outside anonymous namespace):
├── PrimitiveValueSize()   — size lookup table
├── DecodeMetadata()       — metadata buffer parser
├── DecodeVariantValue()   — entry point for value decode
├── GetValueBasicType()    — peek helper
├── GetObjectFieldCount()  — peek helper
├── GetArrayElementCount() — peek helper
├── ValueSize()            — total value size calculator
├── FindObjectField()      — field lookup by name (linear/binary search)
├── GetArrayElement()      — element by index (O(1))
├── GetObjectFieldAt()     — field by position (O(1))
└── FindMetadataKey()      — dictionary key lookup
```

### 4.3 `variant_internal_test.cc` — Proof of Correctness (165 tests)

**Test architecture**: Uses a `RecordingVisitor` (in `variant_test_util.h`) that captures all callbacks as strings, then asserts the exact event sequence.

**Test categories:**
- Metadata parsing (15 tests — incl. non-monotonic string offsets, all offset sizes, reserved bit 5)
- All primitive types + boundaries (21 tests)
- Short strings (4 tests)
- Objects (5 tests — incl. 3-byte offset_size, non-monotonic field offsets)
- Arrays + is_large (4 tests)
- Nesting (3 tests)
- Recursion depth limit (2 tests)
- Utility functions (8 tests)
- Integration (1 test)
- Visitor abort propagation (2 tests)
- Spec-conformance hardcoded bytes (6+ tests)
- ValueSize (6 tests incl. regression for Go bug)
- Random access: FindObjectField, GetArrayElement, GetObjectFieldAt (8 tests)
- FindMetadataKey (4 tests)
- Binary search path for large objects (4 tests)
- Variable-length ValueSize (3 tests)
- Unknown/invalid type handling (2 tests)
- Array non-monotonic offset rejection (1 test)
- Object field offset bounds validation (1 test)
- Empty metadata with various offset sizes (1 test)
- Error cases: type mismatches, version 0, offset overflows, negative index (8 tests)

### 4.4 `variant_test_util.h` — Shared Test Infrastructure

`RecordingVisitor` — a `VariantVisitor` implementation that stores all callbacks as strings in a vector. Shared between decoder tests and encoder round-trip tests. Test-only (not installed).

### 4.5 Build Files

- `cpp/src/arrow/CMakeLists.txt` — Added `extension/variant_internal.cc` to `arrow_objlib` (compiled into `libarrow.so`)
- `cpp/src/arrow/extension/CMakeLists.txt` — Added test to `CANONICAL_EXTENSION_TESTS`
- `cpp/src/arrow/meson.build` — Mirror of CMake source addition
- `cpp/src/arrow/extension/meson.build` — Mirror of CMake test addition

---

## 5. Byte-Level Walkthrough: A Concrete Example

Let's trace exactly what happens when decoding `{"name": "Alice", "age": 30}`:

### Metadata Buffer (hex):
```
01 02 00 04 07 6E 61 6D 65 61 67 65
```

Parsing:
```
Byte 0:  0x01 = header
         version = 0x01 & 0x0F = 1 ✓
         sorted  = (0x01 >> 4) & 0x01 = 0 (false)
         reserved bit 5 = (0x01 >> 5) & 0x01 = 0 ✓
         offset_size = ((0x01 >> 6) & 0x03) + 1 = 1

Byte 1:  0x02 = dict_size = 2 strings
Bytes 2-4: offsets = [0x00, 0x04, 0x07] (3 offsets for 2 strings)
Bytes 5-11: string data = "nameage" (7 bytes)

Result: strings = ["name" (bytes 0-3), "age" (bytes 4-6)]
```

### Value Buffer (hex):
```
02 02 00 01 00 06 0B 15 41 6C 69 63 65 14 1E 00 00 00
```

Parsing:
```
Byte 0:  0x02 = header
         basic_type = 0x02 & 0x03 = 2 (Object)
         type_info = (0x02 >> 2) & 0x3F = 0
         field_offset_size = (0 & 0x03) + 1 = 1
         field_id_size = ((0 >> 2) & 0x03) + 1 = 1
         is_large = ((0 >> 4) & 0x01) = 0 → num_fields_size = 1

Byte 1:  0x02 = num_fields = 2

Bytes 2-3: field_ids = [0x00, 0x01] → ["name", "age"]

Bytes 4-6: offsets = [0x00, 0x06, 0x0B]
           (field 0 starts at +0, field 1 starts at +6, total size = 11)

Bytes 7-17: field values (11 bytes)
  Field 0 (offset 0): 0x15 = short string, len=(0x15>>2)&0x3F = 5
                       "Alice" (bytes 8-12)
  Field 1 (offset 6): 0x14 = primitive, type=(0x14>>2)&0x3F = 5 (Int32)
                       30 as LE int32 = [0x1E, 0x00, 0x00, 0x00]
```

Visitor receives:
```
StartObject(2)
FieldName("name")
String("Alice")
FieldName("age")
Int32(30)
EndObject()
```

---

## 6. Security & Robustness

| Threat | Mitigation |
|--------|-----------|
| Stack overflow from deep nesting | `kMaxNestingDepth = 128` with clean error |
| Out-of-bounds read | Every buffer access preceded by bounds check |
| Malformed offset tables | Monotonicity validation (arrays), per-field bounds (objects) |
| Future spec versions | Reserved bit 5 enforcement → clean rejection |
| Out-of-range field IDs | Checked against `metadata.strings.size()` |
| Truncated buffers | Length checks before every read |
| Integer overflow | Uses `int64_t` for sizes, `static_cast` for promotions |

---

## 7. Build & CI Notes

### Unity Builds

Arrow CI uses `CMAKE_UNITY_BUILD=ON` which combines multiple `.cc` files into single translation units. This exposed a namespace collision: our original `namespace arrow::extension::variant` conflicted with the `arrow::extension::variant()` factory function in `parquet_variant.cc`. Fixed by renaming to `variant_internal`.

**Lesson**: Never reuse a name that exists as a function in the same namespace scope. Our local Docker tests don't use Unity builds, so this only appeared in CI.

### Test Commands

```bash
# Docker (lightweight, ~5 min):
docker run --rm -v "${PWD}:/arrow" -w /arrow/cpp arrow-ext-test:latest bash -c \
  "cmake -S . -B /build -GNinja -DARROW_BUILD_TESTS=ON -DARROW_JSON=ON \
    -DCMAKE_BUILD_TYPE=Debug -DBUILD_WARNING_LEVEL=CHECKIN >/dev/null 2>&1 && \
  ninja -C /build arrow-canonical-extensions-test 2>&1 && \
  /build/debug/arrow-canonical-extensions-test --gtest_filter='Variant*'"
```

---

## 8. What Comes Next

1. **Encoder (#45947)** — `VariantBuilder` class, uses the same namespace/enums, round-trip tests call `DecodeVariantValue` to verify `decode(encode(v)) == v`. PR ready.
2. **Shredding (#45948)** — Parquet reader/writer integration, requires both encoder and decoder.
3. **VariantArray accessors** — `metadata_bytes(i)`, `value_bytes(i)`, `Visit(i, visitor)` on existing `VariantArray` class.

---

## 9. Summary

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| Pattern | Visitor (SAX-style) + random access utilities | Arrow convention + flexibility |
| File location | `arrow/extension/` | Alongside `parquet_variant.h/cc` |
| Namespace | `arrow::extension::variant_internal` | Avoids Unity build collision |
| Error handling | `Result<T>` / `Status` | Arrow standard, never exceptions |
| Memory | Zero-copy string_view | Buffer must outlive metadata |
| Endianness | `bit_util::FromLittleEndian` | Portable across architectures |
| Validation | Bounds-check before every read | Malformed input → Status::Invalid, never crash |
| Testing | RecordingVisitor + exact event assertions | Deterministic, comprehensive, 165 tests |
| Scope | Decode + random access, no pipeline wiring | Clean separation of concerns |
