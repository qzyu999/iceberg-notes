# PR Plan: #45946 — Variant Binary Decoding

> Branch: `variant-decoding` (created, on this branch now)
> Target: `apache/arrow:main` via fork `qzyu999/arrow`
> PR Title: `GH-45946: [C++][Parquet] Variant decoding`

---

## Scope: What "Decoding" Means Here

The decoder does NOT integrate into the Parquet read pipeline (that's a later step). It's a **standalone utility** that takes raw variant binary buffers and provides structured access to the contents. Think of it like shipping a library (`VariantDecoder`) that other code can call — the wiring into `reader.cc` happens later.

This is how the Go reference implementation did it: standalone decode utilities first, integration second.

---

## File Plan

| File | Action | Purpose |
|------|--------|---------|
| `cpp/src/arrow/extension/variant_internal.h` | **Create** | Shared constants, enums (basic types, primitive types, offset sizes) |
| `cpp/src/arrow/extension/variant_internal.cc` | **Create** | Metadata decoder, value decoder implementations |
| `cpp/src/arrow/extension/variant_internal_test.cc` | **Create** | Unit tests for decoding |
| `cpp/src/arrow/extension/parquet_variant.h` | **Modify** | Add decoder API to `VariantArray` (accessor methods) |
| `cpp/src/arrow/extension/parquet_variant.cc` | **Modify** | Hook up VariantArray to decoder |
| `cpp/src/arrow/extension/CMakeLists.txt` | **Modify** | Add new source files and test target |

**Why `variant_internal.h/cc`?** Following Arrow convention — internal implementation details go in `*_internal.h`. The encoder (#45947) will also use these constants and structures, so factoring them out now avoids churn later.

---

## Implementation Steps (ordered)

### Step 1: Define shared constants and types

`variant_internal.h` — the format vocabulary:

```cpp
namespace arrow::extension::variant {

// Variant encoding spec v1
constexpr uint8_t kVariantVersion = 1;

// Basic type codes (bits 0-1 of value header)
enum class BasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
  kObject = 2,
  kArray = 3,
};

// Primitive type codes (bits 2-7 when basic_type == kPrimitive)
enum class PrimitiveType : uint8_t {
  kNull = 0,
  kTrue = 1,
  kFalse = 2,
  kInt8 = 3,
  kInt16 = 4,
  kInt32 = 5,
  kInt64 = 6,
  kDouble = 7,
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  kDate = 11,
  kTimestampMicros = 12,     // UTC
  kTimestampMicrosNTZ = 13,  // No timezone
  kFloat = 14,
  kBinary = 15,
  kString = 16,
};

// Parsed metadata (string dictionary)
struct VariantMetadata {
  uint8_t version;
  bool is_sorted;
  std::vector<std::string_view> strings;  // views into the raw buffer
};

// Decoded variant value — a tagged union
class VariantValue { ... };  // see step 3

}  // namespace arrow::extension::variant
```

### Step 2: Metadata decoder

Parse the metadata buffer into `VariantMetadata`:

```
Input:  const uint8_t* data, int64_t length
Output: Result<VariantMetadata>
```

Algorithm:
1. Read header byte → extract version, sorted flag, offset_size
2. Validate version == 1
3. Read dictionary_size (offset_size bytes, little-endian)
4. Read (dictionary_size + 1) offsets
5. Validate offsets are monotonically increasing and within bounds
6. Create string_views pointing into the raw buffer (zero-copy)

Validation is critical here — malformed metadata should return `Status::Invalid`, not crash.

### Step 3: Value decoder (recursive)

Design choice: **visitor pattern** vs **materialized tree**.

**Recommendation: Visitor pattern** (like Arrow's `ArrayVisitor`). Avoids allocating a tree of objects for every variant value — important for scanning millions of rows.

```cpp
// Visitor interface
class VariantVisitor {
 public:
  virtual ~VariantVisitor() = default;
  virtual Status Null() = 0;
  virtual Status Bool(bool value) = 0;
  virtual Status Int8(int8_t value) = 0;
  virtual Status Int16(int16_t value) = 0;
  virtual Status Int32(int32_t value) = 0;
  virtual Status Int64(int64_t value) = 0;
  virtual Status Float(float value) = 0;
  virtual Status Double(double value) = 0;
  virtual Status Decimal4(const Decimal128& value, int32_t precision, int32_t scale) = 0;
  virtual Status Decimal8(const Decimal128& value, int32_t precision, int32_t scale) = 0;
  virtual Status Decimal16(const Decimal128& value, int32_t precision, int32_t scale) = 0;
  virtual Status Date(int32_t days) = 0;
  virtual Status TimestampMicros(int64_t micros) = 0;
  virtual Status TimestampMicrosNTZ(int64_t micros) = 0;
  virtual Status Binary(std::string_view data) = 0;
  virtual Status String(std::string_view data) = 0;

  virtual Status StartObject(int32_t num_fields) = 0;
  virtual Status FieldName(std::string_view name) = 0;
  virtual Status EndObject() = 0;

  virtual Status StartArray(int32_t num_elements) = 0;
  virtual Status EndArray() = 0;
};

// Main decode function
Status DecodeVariantValue(const VariantMetadata& metadata,
                          const uint8_t* data, int64_t length,
                          VariantVisitor* visitor);
```

PLUS a simpler "get type at offset" and "get scalar at offset" API for random access without full traversal:

```cpp
// Get the type of the value at the given offset without fully decoding
Result<BasicType> GetVariantBasicType(const uint8_t* data, int64_t length);

// For object access by field name (the key use case)
Result<std::optional<int64_t>> FindObjectField(
    const VariantMetadata& metadata,
    const uint8_t* data, int64_t length,
    std::string_view field_name);
```

### Step 4: VariantArray accessor methods

Add methods to `VariantArray` that make it easy to access decoded data:

```cpp
class VariantArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;

  // Get raw metadata buffer for row i
  std::string_view metadata_bytes(int64_t i) const;

  // Get raw value buffer for row i
  std::string_view value_bytes(int64_t i) const;

  // Decode metadata for row i
  Result<VariantMetadata> GetMetadata(int64_t i) const;

  // Get the basic type of row i's value
  Result<BasicType> GetBasicType(int64_t i) const;

  // Full decode via visitor
  Status Visit(int64_t i, VariantVisitor* visitor) const;
};
```

### Step 5: Tests

Test categories:

1. **Metadata decoding**
   - Empty dictionary (header only)
   - Single string
   - Multiple strings, sorted and unsorted
   - All offset sizes (1, 2, 3, 4 bytes)
   - Invalid: bad version, truncated buffer, offsets out of range

2. **Primitive value decoding**
   - Every primitive type (null, bool, int8/16/32/64, float, double, decimal, date, timestamps, binary, string)
   - Short strings (basic_type=1, length in header)
   - Long strings (basic_type=0, primitive_type=kString)

3. **Object decoding**
   - Empty object
   - Single field
   - Multiple fields
   - Nested objects
   - Field lookup by name (sorted and unsorted dictionaries)

4. **Array decoding**
   - Empty array
   - Homogeneous array (all same type)
   - Heterogeneous array (mixed types)
   - Nested arrays
   - Array of objects

5. **Edge cases & error handling**
   - Zero-length buffers
   - Truncated values
   - Maximum nesting depth
   - Offset size boundaries (values near 255, 65535, etc.)

6. **Round-trip with Go reference data** (if test fixtures exist in `testing/` submodule)

---

## Build & Test Commands

```bash
# Interactive development loop in container
docker compose run --rm conda-cpp bash

# Inside container — build only what's needed
cd /build
cmake /arrow/cpp -DARROW_PARQUET=ON -DARROW_BUILD_TESTS=ON -GNinja
ninja -j$(nproc) parquet-arrow-reader-writer-test

# Run just the variant tests
ctest -R "arrow-reader-writer-test" --output-on-failure \
  --tests-regex "TestVariant"

# Or if you add a separate test target:
ninja -j$(nproc) arrow-extension-variant-test
ctest -R "extension-variant-test" --output-on-failure
```

---

## Reference Materials

| Resource | What to study |
|----------|---------------|
| [Variant Encoding Spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) | The authoritative byte-level format definition |
| [arrow-go variant](https://github.com/apache/arrow-go) (commit 5240503) | Full Go decoder — closest architectural reference |
| [Spark VariantUtil.java](https://github.com/apache/spark/tree/master/common/variant) | Original Java implementation, battle-tested |
| `cpp/src/arrow/extension/parquet_variant.cc` | Existing code you're extending |
| `cpp/src/parquet/arrow/variant_test.cc` | Existing tests (schema-only so far) |

---

## Key Design Decisions to Make

1. **Visitor vs materialized tree?**
   - Recommend: Visitor for performance + a small `VariantValue` sum type for simple scalar access
   - Precedent: Arrow uses visitors extensively (`TypeVisitor`, `ArrayVisitor`)

2. **Where do the files live?**
   - Option A: `cpp/src/arrow/extension/variant_internal.h` (next to parquet_variant.h)
   - Option B: `cpp/src/parquet/variant/` (new subdirectory in parquet)
   - Recommend: Option A — it's logically part of the extension type, and the encoding spec is Arrow-level (not Parquet-specific). The Go impl also keeps it with the extension type.

3. **Zero-copy vs copying string access?**
   - Metadata strings should be `string_view` into the raw buffer (zero-copy)
   - This means the metadata buffer must outlive the `VariantMetadata` struct — document this clearly

4. **How much validation?**
   - Full validation on public API boundaries (returns `Status::Invalid`)
   - DCHECKs internally for performance-critical paths
   - Follow Arrow's "validate" vs "ValidateFull" pattern if needed

5. **Thread safety?**
   - Decoding should be stateless/const — multiple threads can decode different rows concurrently
   - No shared mutable state in the decoder

---

## PR Checklist (before submitting)

- [ ] Code compiles with no warnings (`-Werror` is on in CI)
- [ ] All new code has Apache license headers
- [ ] Follows `.clang-format` (run `clang-format -i` on new files)
- [ ] Tests pass: `ctest -R "variant" --output-on-failure`
- [ ] No memory leaks (run with valgrind or ASAN at least once)
- [ ] PR description links to #45946 and umbrella #45937
- [ ] PR title: `GH-45946: [C++][Parquet] Variant decoding`
- [ ] Mention that encoder PR (#45947) will follow and will reuse `variant_internal.h`

---

## Estimated Timeline

| Phase | Effort | What |
|-------|--------|------|
| Step 1: Constants & types | 1-2 days | Header file with enums, structs |
| Step 2: Metadata decoder | 2-3 days | Parse + validate metadata buffer |
| Step 3: Value decoder | 5-7 days | Recursive decoder, visitor, field access |
| Step 4: VariantArray API | 1-2 days | Wire decoder into array class |
| Step 5: Tests | 3-5 days | Comprehensive test coverage |
| Polish & review prep | 2-3 days | Docs, format, address self-review |
| **Total** | **~2-3 weeks** | |

Review cycles will add more calendar time, but the coding effort is roughly this.


---

## Quality Gates: Meeting Arrow's Standards

Arrow is a high-bar Apache project. PRs from new contributors get scrutinized heavily. Below is a comprehensive checklist for validating that any code (vibe-coded or otherwise) meets their standards before submitting.

---

### QG-1: Code Style & Formatting (Automated)

Arrow enforces these via pre-commit hooks and CI. Your code will be auto-rejected if it fails any.

| Check | Tool | Rule |
|-------|------|------|
| Line length | clang-format | **90 columns max** (not 80, not 100) |
| Style | clang-format | Google style base with Arrow overrides |
| Pointer alignment | clang-format | `DerivePointerAlignment: false` (left-aligned: `int* ptr`) |
| Qualifier order | clang-format | `const` on left: `const int x`, not `int const x` |
| Linting | clang-tidy | google-*, modernize-* rules enabled |
| cpplint | cpplint | Google C++ style (with exceptions in CPPLINT.cfg) |
| Include order | Preserve existing blocks (not reordered by formatter) |
| Namespace comments | Required for namespaces > 10 lines: `}  // namespace foo` |

**How to validate locally (inside Docker container):**

```bash
# Format check — will modify files in-place
clang-format -i cpp/src/arrow/extension/variant_internal.h
clang-format -i cpp/src/arrow/extension/variant_internal.cc
clang-format -i cpp/src/arrow/extension/variant_internal_test.cc

# Lint check
clang-tidy cpp/src/arrow/extension/variant_internal.cc \
  -p /build -- -I/arrow/cpp/src

# cpplint
cpplint --quiet --verbose=2 cpp/src/arrow/extension/variant_internal.h
```

**Common vibe-coding pitfalls that fail formatting:**
- Lines over 90 chars (AI loves long lines)
- Using `auto` excessively (clang-tidy warns on non-obvious auto)
- Missing `// namespace` closing comments
- Wrong include ordering
- Using `std::move` on trivial types (modernize warnings)
- Trailing whitespace

---

### QG-2: Arrow C++ Idioms & Patterns

These are conventions not caught by linters but will be flagged in review:

#### Error Handling

```cpp
// ✅ CORRECT: Use Result<T> or Status
Result<VariantMetadata> DecodeMetadata(const uint8_t* data, int64_t len);
Status Validate(const uint8_t* data, int64_t len);

// ❌ WRONG: Never use exceptions
throw std::invalid_argument("bad data");  // NEVER

// ✅ CORRECT: Use macros for propagation
ARROW_ASSIGN_OR_RAISE(auto metadata, DecodeMetadata(data, len));
ARROW_RETURN_NOT_OK(Validate(data, len));

// ✅ CORRECT: Status construction
return Status::Invalid("Unsupported variant version: ", version);
return Status::IOError("Truncated metadata buffer at offset ", offset);
```

#### Memory Management

```cpp
// ✅ CORRECT: shared_ptr for shared ownership
std::shared_ptr<Buffer> buffer;

// ✅ CORRECT: unique_ptr for sole ownership
std::unique_ptr<VariantDecoder> decoder;

// ❌ WRONG: Raw new/delete
auto* ptr = new Foo();  // NEVER

// ✅ CORRECT: String views for non-owning references
std::string_view GetString(int32_t index) const;
// Document lifetime: "view is valid as long as metadata buffer is alive"
```

#### Naming Conventions

```cpp
// Classes: PascalCase
class VariantMetadata {};

// Methods: PascalCase  
Status DecodeValue(const uint8_t* data);

// Local variables: snake_case
int64_t offset_size = 0;
auto dict_size = ReadLittleEndian(buf);

// Constants: kPascalCase
constexpr uint8_t kVariantVersion = 1;

// Enums: PascalCase with k-prefix values
enum class BasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
};

// Namespace: snake_case
namespace arrow::extension::variant {}

// Private members: trailing underscore
std::shared_ptr<Field> metadata_;
```

#### Include Guards

```cpp
// Arrow uses #pragma once (not traditional guards)
#pragma once
```

#### Documentation

```cpp
/// \brief One-line summary of the function.
///
/// Longer description if needed. Explain the contract:
/// what the caller must guarantee (preconditions) and what
/// the function guarantees (postconditions).
///
/// \param[in] data Pointer to variant value buffer (must not be null)
/// \param[in] length Length of the buffer in bytes
/// \param[out] visitor Callback interface for decoded values
/// \return Status::OK on success, Status::Invalid on malformed input
///
/// \note The metadata buffer must outlive any string_views returned.
Status DecodeVariantValue(const VariantMetadata& metadata,
                          const uint8_t* data, int64_t length,
                          VariantVisitor* visitor);
```

---

### QG-3: Mirroring the Go Reference Implementation

The Go implementation by @zeroshade is the architectural reference the Arrow community expects you to align with. Key things to study and mirror:

#### What to Replicate

1. **API surface**: The Go impl exposes metadata decoding, value type inspection, and field access separately. Your C++ should have the same logical boundaries.

2. **Validation strategy**: The Go impl validates eagerly on construction (metadata parse) and lazily on access (value decode). Match this — don't validate the entire value tree upfront.

3. **Zero-copy string access**: Go uses slices into the raw buffer for strings. C++ should use `std::string_view` similarly.

4. **Type enum mapping**: Ensure your `PrimitiveType` enum values match the spec exactly (0=null, 1=true, 2=false, 3=int8, ..., 16=string). Cross-reference with Go's constants.

5. **Object field lookup**: The Go impl supports both linear scan (unsorted) and binary search (sorted dictionary). Implement both.

#### What to Adapt for C++

1. **Error handling**: Go uses `error` returns; C++ uses `Status`/`Result<T>`. Don't return sentinel values.

2. **Memory model**: Go has GC; C++ needs explicit lifetime management. Document that `VariantMetadata` borrows from the buffer.

3. **Generics**: Go uses interfaces; C++ should use the visitor pattern or templates.

4. **Testing**: Go uses table-driven tests; C++ uses Google Test with parameterized tests (`TEST_P`).

#### Where to Find the Go Code

```
apache/arrow-go/
└── parquet/variant/
    ├── metadata.go       # Metadata parsing
    ├── value.go          # Value decoding  
    ├── variant.go        # Top-level API
    ├── metadata_test.go  # Metadata tests
    └── value_test.go     # Value decoding tests
```

Study the test cases especially — they cover spec edge cases the Arrow reviewers will expect you to handle.

---

### QG-4: Testing Standards

Arrow expects **production-grade test coverage**. A PR with thin tests will be sent back.

#### Test Structure (Google Test conventions)

```cpp
// Test fixture class for shared setup
class VariantDecodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Common setup
  }

  // Helper: build metadata buffer from a list of strings
  std::vector<uint8_t> BuildMetadata(
      const std::vector<std::string>& strings,
      bool sorted = false,
      uint8_t offset_size = 1);

  // Helper: build a primitive value buffer
  std::vector<uint8_t> BuildPrimitive(PrimitiveType type,
                                       const void* data, 
                                       int64_t len);
};

// Individual test — descriptive name
TEST_F(VariantDecodingTest, DecodeInt32Primitive) {
  auto metadata = BuildMetadata({});
  int32_t expected = 42;
  auto value = BuildPrimitive(PrimitiveType::kInt32, &expected, 4);
  
  ASSERT_OK_AND_ASSIGN(auto result, DecodeMetadata(metadata.data(), metadata.size()));
  // ... assertions
}

// Parameterized tests for type coverage
class PrimitiveTypeTest : public ::testing::TestWithParam<PrimitiveType> {};

TEST_P(PrimitiveTypeTest, RoundTrip) {
  // Test that each primitive type decodes correctly
}

INSTANTIATE_TEST_SUITE_P(
    AllPrimitiveTypes, PrimitiveTypeTest,
    ::testing::Values(
        PrimitiveType::kNull, PrimitiveType::kTrue, PrimitiveType::kFalse,
        PrimitiveType::kInt8, PrimitiveType::kInt16, PrimitiveType::kInt32,
        // ... all types
    ));
```

#### Required Test Categories (Arrow reviewers WILL ask for these)

1. **Happy path**: Every type decodes correctly
2. **Boundary values**: INT32_MAX, INT64_MIN, empty string, max-length short string (63 bytes), offset_size transitions (255→256)
3. **Error cases**: Truncated buffers, invalid version, bad basic_type, offset out of range, dict_size exceeding buffer
4. **Fuzz resistance**: Malformed inputs should return `Status::Invalid`, never crash/segfault
5. **Spec compliance**: Test vectors from the encoding spec (if any exist in `testing/` submodule)
6. **Nesting depth**: Objects containing arrays containing objects (3+ levels)

#### Test Helpers Pattern (match existing Arrow style)

```cpp
// Arrow uses ASSERT_OK, ASSERT_OK_AND_ASSIGN, ASSERT_RAISES extensively
ASSERT_OK(status);                              // Status must be OK
ASSERT_OK_AND_ASSIGN(auto val, result);         // Result must be OK, unwrap
ASSERT_RAISES(Invalid, bad_call());             // Must fail with Invalid
ASSERT_RAISES_WITH_MESSAGE(                     // Must fail with specific message
    Invalid, "Invalid: Unsupported variant version: 99",
    DecodeMetadata(bad_data, len));

// Array validation
ASSERT_OK(array->ValidateFull());               // Full validation pass
```

#### How to Run Tests

```bash
# Inside container — build test binary
ninja -j$(nproc) arrow-canonical-extensions-test

# Run all variant-related tests
ctest -R "canonical-extensions" --output-on-failure

# Run a single test
./debug/arrow-canonical-extensions-test --gtest_filter="VariantDecoding*"

# Run with verbose output for debugging
./debug/arrow-canonical-extensions-test --gtest_filter="VariantDecoding*" --gtest_print_time=1

# Memory checking (valgrind)
valgrind --leak-check=full ./debug/arrow-canonical-extensions-test --gtest_filter="VariantDecoding*"

# ASAN build (use the sanitizer container)
docker compose run --rm ubuntu-cpp-sanitizer
```

---

### QG-5: CI Pipeline Expectations

When you push a PR, these CI jobs will run automatically on your C++ changes:

| Job | What It Checks | Typical Failure |
|-----|---------------|----------------|
| `conda-cpp` | Full build + test on Linux (conda deps) | Compilation errors, test failures |
| `ubuntu-cpp` | Build on Ubuntu with system deps | Different compiler warnings |
| `debian-cpp` | Build on Debian | Library version differences |
| `fedora-cpp` | Build on Fedora | GCC version-specific warnings |
| `alpine-linux-cpp` | Build on Alpine (musl libc) | Portability issues |
| `clang-format` pre-commit | Style formatting | Any formatting deviation |
| `cpplint` pre-commit | C++ lint | Style violations |
| `ubuntu-cpp-sanitizer` | ASAN + UBSAN | Memory errors, undefined behavior |
| `ubuntu-cpp-thread-sanitizer` | TSAN | Data races |

**Key insight**: Your code must compile cleanly with BOTH GCC and Clang, on multiple Linux distros. Avoid compiler-specific extensions.

**Common CI-only failures:**
- Warning-as-error on unused variables (`-Werror`)
- Implicit conversions between signed/unsigned integers
- Missing `#include` that happened to be transitively included on one platform
- ASAN detecting use-after-free in string_view usage (lifetime bugs)

---

### QG-6: PR Description & Communication Standards

Arrow reviewers expect professional, thorough PR descriptions. Template:

```markdown
### Rationale

Implements variant binary decoding per the [Variant Encoding Spec](link).
This is step 2 of the variant support umbrella (#45937), following the
logical type definition (#46104/PR #45375).

### What changes are included in this PR?

- New files: `variant_internal.h/cc` with metadata and value decoders
- Extended `VariantArray` with accessor methods for decoded data  
- Test coverage for all primitive types, objects, arrays, and error cases

### Are these changes tested?

Yes — new test file `variant_internal_test.cc` with:
- N test cases covering all primitive types
- Object/array decoding with nesting
- Error handling for malformed inputs
- Parameterized tests for boundary values

### Are there any user-facing changes?

No (C++ internal API only, not yet exposed through PyArrow).

### Design Notes

- Uses visitor pattern (matching Arrow's ArrayVisitor) for value traversal
- Zero-copy string access via string_view into raw metadata buffer
- Shared constants in `variant_internal.h` will be reused by encoder (#45947)
- Aligned with the Go reference implementation architecture
```

---

### QG-7: Validation Checklist for Vibe-Coded Output

When using AI to generate code, run through this checklist before committing:

#### Correctness

- [ ] Does the byte parsing match the spec exactly? (bit positions, endianness, sizes)
- [ ] Are offset calculations correct? (off-by-one errors are the #1 bug in binary parsers)
- [ ] Does it handle the "short string" vs "long string" distinction correctly?
- [ ] Are all 17 primitive types handled (null, true, false, int8-64, float, double, decimal4/8/16, date, timestamp, timestampNTZ, binary, string)?
- [ ] Is the object field_id lookup correct for both sorted and unsorted dictionaries?
- [ ] Does array decoding handle the num_elements header correctly for all offset sizes?

#### Safety

- [ ] Does every buffer access check bounds BEFORE reading? (never read past `length`)
- [ ] Are all integer conversions safe? (no implicit narrowing, no signed/unsigned confusion)
- [ ] Does it validate metadata version before proceeding?
- [ ] Does it cap recursion depth for nested objects/arrays? (prevent stack overflow on malicious input)
- [ ] Are all `string_view` lifetimes valid? (do they outlive the buffer they point into?)

#### Style

- [ ] Is every line ≤ 90 characters?
- [ ] Are all method names PascalCase?
- [ ] Are all local variables snake_case?
- [ ] Are enum values kPascalCase?
- [ ] Do all files have the Apache 2.0 license header?
- [ ] Are `#pragma once` guards used (not `#ifndef`)?
- [ ] Are namespace closing comments present?
- [ ] Is `const` used correctly and consistently?

#### Arrow-Specific

- [ ] Does it use `Result<T>` / `Status` (never exceptions)?
- [ ] Does it use `ARROW_ASSIGN_OR_RAISE` / `ARROW_RETURN_NOT_OK` macros?
- [ ] Does it use `int64_t` for sizes/offsets (not `size_t` or `int`)?
- [ ] Does it use `std::shared_ptr` where Arrow expects it?
- [ ] Are test assertions using `ASSERT_OK`, `ASSERT_OK_AND_ASSIGN`, `ASSERT_RAISES`?
- [ ] Does the CMakeLists.txt correctly register new source files and tests?

#### Spec Compliance

- [ ] Does metadata parsing handle all 4 offset sizes (1, 2, 3, 4 bytes)?
- [ ] Does it validate the sorted flag and use binary search when applicable?
- [ ] Does decimal decoding handle precision/scale bytes correctly?
- [ ] Does timestamp distinguish UTC vs NTZ?
- [ ] Does short string only allow up to 63 bytes (6-bit length)?

---

### QG-8: What Arrow Reviewers Specifically Look For

Based on patterns from merged PRs (#45375 and others):

1. **Incremental, reviewable diffs** — Don't dump 1500 lines in one commit. Structure commits logically:
   - Commit 1: Header with types/enums/interfaces
   - Commit 2: Metadata decoder + tests
   - Commit 3: Value decoder + tests
   - Commit 4: VariantArray integration + tests

2. **No dead code** — Don't add functions/methods that aren't called or tested yet. If the encoder will use something, leave a `// TODO GH-45947` comment but don't ship unused code.

3. **Conservative API surface** — Start minimal. It's easy to add methods later, hard to remove them. Reviewers will push back on "might be useful someday" APIs.

4. **Spec references** — Comment non-obvious byte parsing with references to the spec section:
   ```cpp
   // Variant Encoding Spec §4.1: Value header byte layout
   // Bits 0-1: basic_type, Bits 2-7: type-specific
   uint8_t basic_type = header & 0x03;
   ```

5. **Test names that tell a story** — `DecodeInt32Primitive` not `Test1`. Reviewers skim test names to understand coverage.

6. **No warnings** — `-Werror` is on. Zero warnings on GCC and Clang.

7. **No UB** — Undefined behavior will be caught by ASAN/UBSAN in CI. Common traps:
   - Signed integer overflow
   - Unaligned memory access (casting `uint8_t*` to `int32_t*`)
   - Reading uninitialized memory

8. **Portability** — Must work on x86_64 and ARM64. Don't assume endianness without `arrow::bit_util` helpers.

---

### QG-9: Pre-Submission Sanity Checks

Run these inside the Docker container before pushing:

```bash
# 1. Format (must produce zero diff)
find cpp/src/arrow/extension -name "variant_internal*" -exec clang-format -i {} \;
git diff --exit-code  # should show nothing

# 2. Build with warnings as errors
cmake /arrow/cpp -GNinja \
  -DARROW_PARQUET=ON \
  -DARROW_BUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_WARNING_LEVEL=CHECKIN
ninja -j$(nproc)

# 3. Run tests
ctest -R "canonical-extensions" --output-on-failure

# 4. Run ASAN (if in sanitizer container)
ctest -R "canonical-extensions" --output-on-failure

# 5. Valgrind spot check
valgrind --leak-check=full --error-exitcode=1 \
  ./debug/arrow-canonical-extensions-test --gtest_filter="VariantDecoding*"

# 6. Check for TODO/FIXME (make sure none are accidental)
grep -rn "TODO\|FIXME\|HACK\|XXX" cpp/src/arrow/extension/variant_internal*
# Only intentional TODOs (like "TODO GH-45947") should appear
```

---

### QG-10: Common Reviewer Feedback on First-Time Arrow PRs

Anticipate and avoid these:

| Feedback | Prevention |
|----------|-----------|
| "Please use `int64_t` not `size_t`" | Arrow convention: all sizes/lengths are `int64_t` |
| "Can you add a test for the error case?" | Every `Status::Invalid` return needs a corresponding `ASSERT_RAISES` test |
| "This needs bounds checking" | Every buffer read needs `if (offset + N > length) return Invalid(...)` |
| "Please use the `ARROW_ASSIGN_OR_RAISE` macro" | Never do `auto result = Foo(); if (!result.ok()) return result.status();` |
| "Can you split this into smaller commits?" | See QG-8 point 1 |
| "Why is this public?" | Default to private/internal; only expose what's needed |
| "Please add a brief doc comment" | Every public method needs `/// \brief ...` |
| "This could overflow on 32-bit" | Use `int64_t` arithmetic, not `int` |
| "Missing include" | Include what you use directly, don't rely on transitive includes |
| "Please benchmark this" | If the reviewer asks, add a micro-benchmark (only if asked) |
