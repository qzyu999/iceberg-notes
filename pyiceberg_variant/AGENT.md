# Variant Encode/Decode — Agent Context

> Last updated: 2026-06-11
> Owner: @qzyu999
> Umbrella issue: GH-45937 [C++][Parquet] Add variant support

---

## Repository Layout

| Repo | Local Path | Remote |
|------|-----------|--------|
| Apache Arrow (C++) | `C:\...\arrow` | `origin` = `qzyu999/arrow`, `upstream` = `apache/arrow` |
| Apache Arrow Go | `C:\...\arrow-go` | `origin` = `qzyu999/arrow-go`, `upstream` = `apache/arrow-go` |
| Notes/Context (this repo) | `C:\...\iceberg-notes` | — |

---

## Branch Structure (C++ — `apache/arrow`)

```
main (e16067a78c)
  └── variant-decoding (e980fd0867) — GH-45946: [C++][Parquet] Variant decoding
       └── variant-encoding (7f51026fb8) — GH-45947: [C++][Parquet] Variant encoding
```

- **Linear history**: encoding sits directly on top of decoding.
- **Single commit per branch** — clean for squash-merge or rebase by reviewers.
- **Ready for force-push** to `origin/variant-decoding` and `origin/variant-encoding`.
- Merge order: **45946 first, then 45947**. The encoding PR targets the decoding branch (or main after 45946 merges).
- **Docker tests pass**: 238/238 tests (encoding), 165/165 tests (decoding standalone), `BUILD_WARNING_LEVEL=CHECKIN` (warnings-as-errors), verified 2026-06-09. Comment-only changes in 5th review pass (2026-06-10) do not affect test outcomes.
- **Namespace**: `arrow::extension::variant_internal` (renamed from `variant` to avoid Unity build collision with the `arrow::extension::variant()` factory function in `parquet_variant.cc`). See sixth review pass below.

### Branch: `variant-decoding`

**Scope**: Full Variant binary decoding per the [VariantEncoding.md](VariantEncoding.md) spec.

**Files** (8 changed, +3637 lines):
- `cpp/src/arrow/extension/variant_internal.h` — Public API: enums, structs, decoder functions, random-access utilities
- `cpp/src/arrow/extension/variant_internal.cc` — All decoder logic (~1015 lines)
- `cpp/src/arrow/extension/variant_internal_test.cc` — 108+ tests (~2125 lines)
- `cpp/src/arrow/extension/variant_test_util.h` — Shared `RecordingVisitor` for tests (test-only, not installed)
- `cpp/src/arrow/CMakeLists.txt` — Added `variant_internal.cc` to build
- `cpp/src/arrow/extension/CMakeLists.txt` — Added test file
- `cpp/src/arrow/meson.build` — Mirror of CMake addition
- `cpp/src/arrow/extension/meson.build` — Mirror of CMake test addition + install header comment

**Key design decisions**:
- SAX/visitor pattern (not DOM/tree materialization) — matches Arrow convention
- Zero-copy `string_view` into raw buffer
- Recursion depth limit (`kMaxNestingDepth = 128`)
- Includes random-access utilities: `ValueSize`, `FindObjectField`, `GetArrayElement`, `GetObjectFieldAt`, `FindMetadataKey`
- UTF-8 validation is NOT performed during decode (documented in header); responsibility of higher-level consumer
- Per-field offset bounds validation in object decoding (rejects offsets > total_data_size)
- `ReadUnsignedLE` concise big-endian correctness comment (trimmed from verbose version)
- `DCHECK_NE(visitor, nullptr)` in `DecodeVariantValue` for null visitor safety
- `FindObjectField` binary search includes comment noting it assumes spec-compliant field ID ordering
- `FindObjectField` binary search uses `int32_t` for `lo`/`hi` with comment explaining this avoids Go's unsigned underflow bug
- Field ID ordering NOT validated in `DecodeObject` for performance (documented with NOTE comment)
- Decimal decoder is lenient on scale (no validation), documented with comment; encoder validates scale ≤ 38
- SmallVector TODO includes performance rationale: "correctness-first; optimize if profiling shows pressure"

### Branch: `variant-encoding`

**Scope**: `VariantBuilder` class for encoding Variant binary values. Validated by round-trip tests against the decoder.

**Files** (7 changed, +1775 lines on top of decoder):
- `cpp/src/arrow/extension/variant_internal.h` — Added `VariantBuilder` class + `<string>`, `<unordered_map>` includes
- `cpp/src/arrow/extension/variant_builder.cc` — Builder implementation (~463 lines)
- `cpp/src/arrow/extension/variant_builder_test.cc` — 75+ round-trip + encoder tests (~1180 lines)
- Build files (CMake + Meson) — Added builder source and test

**Key improvements over initial draft**:
- `VariantBuilder` is move-only (non-copyable, noexcept movable)
- `AddKey()` has TODO for transparent hasher optimization
- `FinishArray()` validates offsets are non-negative
- `FinishObject()` doc explicitly states "sorts in-place" in the brief
- `Finish()` validates total dictionary size fits in 4-byte offsets
- `IntSize()` has `DCHECK_LE(value, UINT32_MAX)` guard
- NaN/±Inf float/double tests included
- `RoundTrip()` test helper documented re: `.ValueOrDie()` usage (non-void function)
- `Finish()` has TODO for incremental sorted-state caching (O(n) rescan per call matches Go)

**TODOs left for GH-45948 (shredding)**:
```cpp
// TODO GH-45948: Add BuildWithoutMeta() — raw value bytes without metadata
// TODO GH-45948: Add UnsafeAppendEncoded() — append pre-encoded bytes
// TODO GH-45948: Add SetAllowDuplicates(bool) — last-value-wins semantics
```

---

## Go Bug Fix (`apache/arrow-go`)

### Issue
`valueSize()` in `parquet/variant/utils.go` used `(typeInfo >> 4) & 0x1` for array `is_large` flag — should be `(typeInfo >> 2) & 0x1` per spec. This is bit 2 of the 6-bit value_header (bit 4 of full byte), not bit 4 (bit 6 of full byte).

### Status
- **Branch**: `fix-valuesize-array-islarge` on `qzyu999/arrow-go`
- **Commit**: `ba1e4b4` — single commit with fix + regression test (`valuesize_test.go`)
- **Pushed**: Yes, to `origin/fix-valuesize-array-islarge`
- **PR submitted**: Yes, to `apache/arrow-go`
- **Local checkout**: The fix branch (HEAD = `ba1e4b4`)

### How it relates to C++
The C++ `ValueSize()` implementation correctly uses `(type_info >> 2) & 0x01` for arrays, with a comment referencing the Go bug:
```cpp
// Note: Go's valueSize() in arrow-go (prior to fix PR) incorrectly
// used (typeInfo >> 4) for arrays, which reads bit 6 — the object's
// is_large position. The spec places array is_large at bit 4 of the
// full header byte. See: apache/arrow-go#839.
```

---

## Spec Conformance Notes

### Bit Layout Reference

**Object header** (full byte):
```
bit 0-1: basic_type = 2
bit 2-3: field_offset_size_minus_one
bit 4-5: field_id_size_minus_one
bit 6:   is_large
bit 7:   unused
```

**Array header** (full byte):
```
bit 0-1: basic_type = 3
bit 2-3: field_offset_size_minus_one
bit 4:   is_large
bit 5-7: unused
```

The `type_info` = `(header >> 2) & 0x3F` (the 6-bit value_header after stripping basic_type):
- Object `is_large`: `(type_info >> 4) & 0x01` ✓
- Array `is_large`: `(type_info >> 2) & 0x01` ✓
- Object `field_id_size`: `((type_info >> 2) & 0x03) + 1`
- Object `field_offset_size`: `(type_info & 0x03) + 1`
- Array `field_offset_size`: `(type_info & 0x03) + 1`

### Metadata header byte:
```
bit 0-3: version (must be 1)
bit 4:   sorted_strings
bit 5:   reserved (must be 0 in v1)
bit 6-7: offset_size_minus_one
```

---

## Go Parity Summary

| Feature | C++ Decoder | C++ Encoder | Go Equivalent |
|---------|:-----------:|:-----------:|---------------|
| Metadata decode | ✅ | — | `Metadata.DecodeMetadata()` |
| All 21 primitive types | ✅ | ✅ | `Value.Value()` / `Builder.Append*()` |
| Short string ≤63 bytes | ✅ | ✅ | Short string encoding in `AppendString` |
| Object decode + field ordering | ✅ | — | `ObjectValue` |
| Array decode + is_large | ✅ | — | `ArrayValue` |
| ValueSize | ✅ | — | `valueSize()` (now fixed) |
| FindObjectField (by name) | ✅ | — | `ObjectValue.ValueByKey()` |
| GetArrayElement (by index) | ✅ | — | `ArrayValue.Value(i)` |
| GetObjectFieldAt (positional) | ✅ | — | `ObjectValue.FieldAt(i)` |
| FindMetadataKey (dict lookup) | ✅ | — | `Metadata.IdFor()` |
| Builder: all primitives | — | ✅ | `Builder.Append*()` |
| Int auto-sizing | — | ✅ | `Builder.AppendInt()` |
| FinishArray/FinishObject | — | ✅ | `Builder.FinishArray/Object()` |
| Duplicate key rejection | — | ✅ (always error) | `Builder.FinishObject()` (configurable) |
| Reset/Reuse | — | ✅ | `Builder.Reset()` |
| BuildFromExistingMetadata | — | ✅ | `NewBuilderFromMeta()` |
| BuildWithoutMeta | — | TODO 45948 | `Builder.BuildWithoutMeta()` |
| UnsafeAppendEncoded | — | TODO 45948 | `Builder.UnsafeAppendEncoded()` |
| SetAllowDuplicates | — | TODO 45948 | `Builder.SetAllowDuplicates()` |
| JSON parse/serialize | — | N/A | `ParseJSON/MarshalJSON` |

---

## Testing

### C++ Test Suites

Both test files compile into `arrow-canonical-extensions-test`.

**Decoder tests** (`variant_internal_test.cc`):
- Metadata parsing (15 tests — incl. non-monotonic string offsets)
- All primitive types + boundaries (21 tests)
- Short strings (4 tests)
- Objects (5 tests — incl. 3-byte offset_size)
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
- Non-monotonic object offsets (2 tests)
- Binary search path for large objects (4 tests)
- Variable-length ValueSize (3 tests)
- Unknown/invalid type handling (2 tests)
- Array non-monotonic offset rejection (1 test)
- Object field offset bounds validation (1 test)
- Empty metadata with various offset sizes (1 test)
- Error cases: type mismatches, version 0, offset overflows, negative index (8 tests)

**Encoder tests** (`variant_builder_test.cc`):
- Primitive round-trip (14 tests — incl. short/long boundary)
- Int boundary auto-sizing (8 tests)
- Array round-trip (3 tests)
- Object round-trip + sorting + duplicates (5 tests)
- Builder features: reset, from-metadata, sorted flag (4 tests)
- Integration: complex object, large metadata, offset-size-from-key-count, invalid start, negative offsets (5 tests)
- Special floats: NaN, ±Inf for float and double (6 tests)
- Large container is_large (2 tests — array + object)
- Decimal scale validation (1 test)
- Decoder utility round-trips through builder output (4 tests)
- Direct int type methods: Int8, Int16, Int32, Int64 (4 tests)
- Builder reuse: multiple Finish() calls with dictionary preservation (2 tests)
- Pre-existing buffer: FinishObject/FinishArray with start > 0 (2 tests)

### Go Test (`valuesize_test.go`)
- `TestValueSizeLargeArray` — regression test for the is_large bit fix
- `TestValueSizeLargeObject` — ensures object path still works

---

## Build & Test Commands

### Docker-based testing (required for CI parity)

Arrow C++ tests MUST be run via Docker to match the CI environment. The local Windows environment cannot build Arrow C++ natively due to Linux-specific dependencies.

**Lightweight approach** (uses cached `arrow-ext-test` image, ~5 min):
```bash
# From the arrow repo root, with the desired branch checked out:

# Build + run extension tests (includes variant decode + encode tests):
docker run --rm -v "${PWD}:/arrow" -w /arrow/cpp arrow-ext-test:latest bash -c \
  "cmake -S . -B /build -GNinja \
    -DARROW_BUILD_TESTS=ON \
    -DARROW_JSON=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_WARNING_LEVEL=CHECKIN \
    >/dev/null 2>&1 && \
  ninja -C /build arrow-canonical-extensions-test 2>&1 && \
  /build/debug/arrow-canonical-extensions-test"
```

**Full CI approach** (builds the full conda-cpp image, ~30-60 min first time):
```bash
# Build base image (one-time):
docker compose build conda
docker compose build conda-cpp

# Run full C++ build + all tests:
docker compose run --rm conda-cpp
```

**Running only specific test suites**:
```bash
# Run only variant tests (by gtest filter):
docker run --rm -v "${PWD}:/arrow" -w /arrow/cpp arrow-ext-test:latest bash -c \
  "cmake -S . -B /build -GNinja -DARROW_BUILD_TESTS=ON -DARROW_JSON=ON -DCMAKE_BUILD_TYPE=Debug >/dev/null 2>&1 && \
  ninja -C /build arrow-canonical-extensions-test >/dev/null 2>&1 && \
  /build/debug/arrow-canonical-extensions-test --gtest_filter='Variant*'"
```

### Docker image: `arrow-ext-test`

A lightweight Ubuntu 24.04 image with:
- build-essential, cmake, ninja-build, git
- libgtest-dev, libgmock-dev, nlohmann-json3-dev

If the image doesn't exist, rebuild from this Dockerfile:
```dockerfile
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build git \
    libgtest-dev libgmock-dev nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
```
```bash
docker build -t arrow-ext-test:latest -f- . <<'EOF'
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build git \
    libgtest-dev libgmock-dev nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
EOF
```

### Docker troubleshooting (Windows)

Docker Desktop on Windows occasionally hangs (daemon unresponsive, `docker info` never returns). Fix:
```powershell
# Kill all Docker processes
Stop-Process -Name "Docker Desktop" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "com.docker.backend" -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

# Restart Docker Desktop
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
Start-Sleep -Seconds 20

# Verify daemon is responding
docker info 2>&1 | Select-String "Server Version"
```

### Important CMake flags

| Flag | Purpose |
|------|---------|
| `DARROW_BUILD_TESTS=ON` | Build test binaries |
| `DARROW_JSON=ON` | Required for opaque/tensor extension tests (they depend on JSON) |
| `DCMAKE_BUILD_TYPE=Debug` | Debug build for test assertions |
| `DBUILD_WARNING_LEVEL=CHECKIN` | Treats warnings as errors (CI mode) |

### Unity builds (CI caveat)

Arrow CI uses **Unity builds** (`CMAKE_UNITY_BUILD=ON`) which combine multiple `.cc` files into single translation units. This means symbols from different `.cc` files in the same CMake target can collide. Our Docker test image does NOT use Unity builds, so namespace/function name collisions only surface in CI. Key rule: **never reuse a name that exists as a function in the same namespace** (e.g., don't create `namespace foo` if `foo()` already exists at the same scope).

### Linting / Formatting

Arrow uses `clang-format` (version 18) and `clang-tidy` for C++ code style. The CI runs these checks:

```bash
# Check formatting (from arrow repo root):
# Must use clang-format-18 specifically
docker run --rm -v "${PWD}:/arrow" -w /arrow ubuntu:24.04 bash -c \
  "apt-get update && apt-get install -y clang-format-18 >/dev/null 2>&1 && \
  find cpp/src/arrow/extension/variant* -name '*.cc' -o -name '*.h' | \
  xargs clang-format-18 --dry-run --Werror"
```

Style rules are defined in `.clang-format` at the repo root. Key conventions:
- 2-space indent
- 90 character line limit
- `BreakBeforeBraces: Attach`
- Arrow-specific namespace style

### Test results (verified 2026-06-08)

| Branch | Tests | Result | Warning Level |
|--------|-------|--------|---------------|
| `variant-decoding` | 165 (standalone) | ✅ PASSED | CHECKIN (werror) |
| `variant-encoding` | 238 (full suite) | ✅ PASSED | CHECKIN (werror) |

> The encoding branch includes all extension tests (bool8, json, uuid, opaque, tensor,
> variant decoder, variant builder). The decoding branch alone runs 165 tests (no builder tests).

### Go tests

```bash
# From arrow-go directory:
go test ./parquet/variant/... -v -run TestValueSize
```

---

## PR Reviewer Notes (Deliberate Divergences from Go)

These are design choices that reviewers may question. Pre-emptive explanations:

### 1. Recursion depth limit (`kMaxNestingDepth = 128`)

**C++ has it, Go does not.**

The Go implementation has no recursion depth guard — it will recurse until the goroutine stack grows (default 1GB limit, effectively unlimited). In C++ the default thread stack is typically 1-8 MB, so a maliciously crafted variant with hundreds of nested arrays/objects can cause a stack overflow.

We set `kMaxNestingDepth = 128` which is generous for real-world data (JSON rarely nests >20 levels) while protecting against adversarial input. This is consistent with other Arrow C++ parsers (e.g., the JSON parser has configurable max nesting).

**Reviewer callout**: Mention this in the PR description. It's a security hardening measure appropriate for C++ that Go doesn't need due to different stack semantics.

### 2. No UTF-8 validation during decode

**Neither C++ nor Go validate UTF-8 during variant decode.**

The spec mandates all strings be UTF-8, but both implementations pass through raw bytes without validation. This is a deliberate design choice:
- Validation adds overhead per string (the spec already guarantees UTF-8 from the writer)
- The decoder operates on untrusted data in practice, but validation responsibility sits at the boundary where data enters the system (e.g., Parquet reader validation, or when materializing to Arrow StringArray)
- A future `ValidateVariant()` utility could be added if needed

**Reviewer callout**: Point out in the VariantVisitor doc comment (already there) and mention in PR description that this matches Go's approach. Suggest a follow-up for optional validation if reviewers want it.

### 3. Duplicate keys: always reject vs configurable

**C++ always errors on duplicate keys. Go allows configuring tolerance.**

Go has `Builder.SetAllowDuplicates(true)` which keeps the last value via compaction using `valueSize()`. Our implementation strictly rejects duplicates with `Status::Invalid`, which is spec-compliant ("An object may not contain duplicate keys").

**Rationale**: The spec says duplicates are an error. Tolerating them is a convenience for producers handling dirty data, not a requirement. Adding it later (via TODO for GH-45948) is straightforward and doesn't break any API contract.

**Reviewer callout**: Note this is spec-strict behavior. The TODO is documented in the header for GH-45948 if needed.

### 4. Visitor pattern vs random-access Value type

**C++ uses a visitor (SAX-style). Go uses a `Value` interface (DOM-style).**

Go returns a `Value` struct that you navigate lazily — `obj.ValueByKey("name")` returns another `Value`. This works well in Go due to GC-managed slices and interface dispatch.

C++ uses a visitor that receives all values during a single traversal pass. This is idiomatic Arrow C++ (`TypeVisitor`, `ArrayVisitor`, `ScalarVisitor` all use this pattern) and avoids heap allocation per value. Random access is still possible via the standalone utility functions (`FindObjectField`, `GetArrayElement`).

**Reviewer callout**: Both patterns are provided — visitor for bulk traversal, standalone functions for random access. This gives callers flexibility without forcing either approach.

### 5. `std::vector` heap allocations in container decoding

`DecodeObject` and `DecodeArray` allocate `std::vector<uint32_t>` for field IDs and offsets. TODOs in the code note this could be optimized with stack-allocated `SmallVector` for the common case.

**Reviewer callout**: Acknowledge this is a first implementation. Performance optimization with `SmallVector` can follow if profiling shows allocation pressure during bulk variant column scans.

### 6. Reserved bit 5 validation in metadata header

**C++ rejects metadata where bit 5 is set. Go does not check this.**

The spec says bit 5 is "reserved" in version 1. We enforce it must be zero, which means future spec versions that use this bit will cause older decoders to explicitly reject the data (clean failure) rather than silently misinterpret it. This is the defensive choice for forward compatibility.

### 7. Object field offset bounds validation

The decoder validates that each field offset is `< total_data_size` (with a special case for empty objects). Go does not perform this check — it will simply read out-of-bounds data if the offsets are malformed.

**Reviewer callout**: This is additional safety checking for defense against malformed/malicious input.

### 8. Binary search correctness assumption in FindObjectField

`FindObjectField` uses binary search for objects with ≥32 fields, relying on the spec invariant that field IDs are listed in lexicographic order of their corresponding key names. If the input violates this invariant (malformed data), binary search may return incorrect results. A comment documents this assumption. The Go implementation has the same assumption in `ObjectValue.ValueByKey()`.

### 9. Go `ObjectValue.ValueByKey()` unsigned underflow in binary search

The Go binary search uses `j = mid - 1` where `j` is `uint32`. If `mid == 0`, this wraps around to `MaxUint32`. The C++ implementation correctly uses `int32_t` for `lo`/`hi`, avoiding this issue. A comment in `FindObjectField` explicitly documents this choice. A separate issue/PR has been filed for this Go bug.

### 10. `Finish()` recomputes `is_sorted` on every call

`Finish()` calls `std::is_sorted(dict_keys_.begin(), dict_keys_.end())` which is O(n) in the dictionary size. The Go implementation does the same (`slices.IsSortedFunc` in `Build()`). For column-scan workloads where `Finish()` is called per-row with a large shared dictionary, this could be a bottleneck. A TODO in the code suggests caching the sorted state incrementally (check only new keys vs. previous last key). This is acceptable for a first implementation and matches Go behavior.

---

## Additional Issues Filed

| Issue/PR | Repo | Description |
|----------|------|-------------|
| apache/arrow-go#839 (PR) | arrow-go | `valueSize()` array `is_large` bit position fix |
| TBD | arrow-go | `ObjectValue.ValueByKey()` unsigned underflow in binary search |

---

## What's Next

### GH-45948: Variant Shredding (not started)
- Branch: `variant-shredding` (to be created after 45946+45947 merge)
- Depends on both encoder and decoder
- Requires: `BuildWithoutMeta`, `UnsafeAppendEncoded`, `SetAllowDuplicates`
- Involves: Parquet reader/writer integration, schema changes to `VariantExtensionType`
- See `arrow_issue_45946_45947_45948_solution.md` for detailed plan

### PR Review Checklist
When creating PRs on `apache/arrow`:
1. **45946 PR** targets `main`, titled: `GH-45946: [C++][Parquet] Variant decoding`
2. **45947 PR** targets `main` (after 45946 merges) OR targets the 45946 branch if using stacked PRs, titled: `GH-45947: [C++][Parquet] Variant encoding`
3. Both PRs should reference the umbrella issue GH-45937
4. Mention the Go bug discovery + fix PR in the 45946 PR description
5. Call out the deliberate divergences from Go (section above) in the PR description
6. **Before pushing**: Run Docker tests on both branches to verify all tests pass with CHECKIN warning level

### PR Description Notes (what to mention)

#### GH-45946 PR (Variant Decoding):
- **Summary**: Full Variant binary decoding per the [VariantEncoding.md](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) spec. Adds `variant_internal.h/.cc` with decoder, random-access utilities, and 165 tests.
- **Key points to mention**:
  - Part of GH-45937 umbrella (Add variant support)
  - Visitor pattern (SAX-style) — idiomatic for Arrow C++
  - Recursion depth limit (kMaxNestingDepth=128) — security hardening for C++ stack semantics
  - Random-access utilities (FindObjectField, GetArrayElement, etc.) for future Parquet reader integration
  - No UTF-8 validation during decode (matches Go; documented for future follow-up)
  - Discovered and fixed a bug in arrow-go `valueSize()` (apache/arrow-go#839) — array `is_large` bit position
  - Also identified unsigned underflow bug in Go's `ObjectValue.ValueByKey()` binary search (separate issue TBD)
  - Reserved bit 5 enforcement (Go does not check; we fail cleanly on future versions)
  - Object field offset bounds validation (Go does not check; defense-in-depth)
- **What was tested**: 165 tests pass with `BUILD_WARNING_LEVEL=CHECKIN`

#### GH-45947 PR (Variant Encoding):
- **Summary**: `VariantBuilder` class for encoding Variant binary values. Validated by round-trip tests against the decoder from GH-45946.
- **Key points to mention**:
  - Part of GH-45937 umbrella (Add variant support)
  - Depends on GH-45946 (decoding) — merge order matters
  - Mirrors Go's `Builder` pattern: start/offset + finish for containers
  - Strict duplicate key rejection (spec says "must not contain duplicate keys"); configurable tolerance deferred to GH-45948 with TODO
  - `FinishObject` sorts fields in-place — spec requires field IDs in lexicographic key order
  - Builder is move-only, dictionary preserved across `Finish()` calls
  - TODOs documented for GH-45948 shredding: `BuildWithoutMeta`, `UnsafeAppendEncoded`, `SetAllowDuplicates`
  - 4GB size limit comment — spec's 4-byte offset maximum (Go enforces stricter 128MB)
- **What was tested**: 238 total tests (73 encoder + 165 decoder) pass with `BUILD_WARNING_LEVEL=CHECKIN`

### Push Commands (already pushed 2026-06-11)
```bash
# Force-push updated decoder branch:
git push origin variant-decoding --force-with-lease  # done → e980fd0867

# Force-push updated encoding branch:
git push origin variant-encoding --force-with-lease  # done → 7f51026fb8
```

---

## Code Review Changelog

All review changes have been committed and tested. Both branches are single-commit, clean, and ready for force-push.

### First review pass (2026-06-07):

**Decoding branch** (`variant-decoding`):
1. **Header docblock**: Updated to clarify `_internal` naming — the file IS installed and public; "internal" refers to "binary encoding internals" not visibility
2. **`DecodeVariantValue`**: Added `DCHECK_NE(visitor, nullptr)` per Arrow convention
3. **`DecodePrimitive` (kDecimal4)**: Added comment documenting that scale is not validated during decode (lenient decoder, strict encoder)
4. **`FindObjectField`**: Added comment explaining binary search correctness assumption (relies on spec-mandated field ID ordering)
5. **`variant_internal.cc`**: Changed `#include "arrow/util/logging.h"` → `"arrow/util/logging_internal.h"` to match repo convention for extension .cc files
6. **`variant_internal_test.cc`**: Removed `#include <cstdio>`, replaced `std::snprintf` with string concatenation

**Encoding branch** (`variant-encoding`):
1. **`variant_builder.cc`**: Uses `"arrow/util/logging_internal.h"` (repo convention)
2. **`variant_builder_test.cc`**: Added 4 new direct int type method tests (`ExplicitInt8/16/32/64`) verifying they produce the specified type without auto-sizing
3. **`variant_builder_test.cc`**: Added 2 new builder reuse tests (`MultipleFinishPreservesDictionary`, `DictionaryGrowsAcrossFinishCalls`) verifying dictionary persistence across Finish() calls

### Second review pass (2026-06-08):

**Decoding branch** (`variant-decoding`):
1. Trimmed `ReadUnsignedLE` doc comment from 15 lines to concise 4-line version
2. Removed dead-code redundant `num_fields > 0` check inside object field offset bounds validation loop
3. Updated SmallVector TODO with perf rationale: "correctness-first; optimize if profiling shows pressure"
4. Added `meson.build` comment explaining `variant_internal.h` install naming
5. Added lifetime safety comment on `key_storage_` in `VariantFindFieldBinarySearchTest`
6. Added NOTE comment documenting that field ID ordering is not validated in `DecodeObject` (for performance)
7. Added `NonMonotonicStringOffsets` test — exercises `ValidateOffsets` rejection path
8. Added `ThreeByteOffsetSize` test — exercises 3-byte field_offset_size + field_id_size in value decoding

**Encoding branch** (`variant-encoding`):
1. Added `DCHECK_LE(value, UINT32_MAX)` to `IntSize()` function for explicit invariant
2. Rewrote `FinishObject` header doc to prominently state "sorts in-place"
3. Added clarifying comment on `RoundTrip()` helper about `.ValueOrDie()` usage
4. Replaced `.ValueOrDie()` with `ASSERT_OK_AND_ASSIGN` in `ShortStringBoundary63`, `LongStringBoundary64`, `FloatNaN`, and `DoubleNaN` tests
5. Added `LargeObjectIsLarge` test — mirrors `LargeArrayIsLarge`, builds 300-field object
6. Added `MetadataOffsetSizeFromKeyCount` test — verifies offset_size computation from `max(total_string_size, num_keys)`

### Third review pass (2026-06-08, principal engineer review):

**Decoding branch** (`variant-decoding`, c5720bc9f7):
1. Added documentation comments in `DecodeObject` explaining that consumed bytes are not validated against expected field size (non-monotonic offsets make per-field size inference unreliable); added TODO for optional strict validation
2. Added matching documentation comments in `DecodeArray` explaining consumed bytes are not validated; offsets are monotonically validated above, but element exact-fill is not checked
3. Added 8 new tests in `VariantErrorCaseTest`:
   - `MetadataVersionZero` — version 0 rejection
   - `GetObjectFieldCountOnArray` — type mismatch error
   - `GetArrayElementCountOnObject` — type mismatch error
   - `GetObjectFieldCountOnPrimitive` — type mismatch error
   - `GetArrayElementCountOnPrimitive` — type mismatch error
   - `MetadataStringOffsetExceedsBuffer` — offset exceeds available data (issue 4.4)
   - `GetArrayElementNegativeIndex` — negative index rejection
   - `FindObjectFieldOnNonObject` — calling FindObjectField on non-object
4. Added fuzz TODO comment at end of decoder test file referencing GH-45948 and Arrow's fuzzing infrastructure

**Encoding branch** (`variant-encoding`, a5c6f42f1e):
1. Added 2 new tests in `VariantBuilderPreExistingBufferTest`:
   - `ObjectAfterPrimitive` — FinishObject with start > 0 when buffer has pre-existing data
   - `ArrayAfterPrimitive` — FinishArray with start > 0 when buffer has pre-existing data

**Verification**:
- Encoding branch: 238/238 tests PASSED (BUILD_WARNING_LEVEL=CHECKIN)
- Decoding branch (standalone): 165/165 tests PASSED (BUILD_WARNING_LEVEL=CHECKIN)
- Confirmed decoding branch builds independently with no linker errors
- Confirmed branch split is correct: no encoder references exist on decoding branch

### Fourth review pass (2026-06-09, final nits):

**Decoding branch** (`variant-decoding`, e15ecc8f00):
1. Added comment in `FindObjectField` binary search path explaining that `int32_t` is used deliberately for `lo`/`hi` to avoid the unsigned underflow pattern present in Go's `ObjectValue.ValueByKey()` (which uses `uint32` and wraps to `MaxUint32` when `mid == 0`)

**Encoding branch** (`variant-encoding`, ebb9629b1b):
1. Added TODO in `Finish()` suggesting incremental sorted-state caching to avoid O(n) rescan on every call (matches Go's `Build()` which also rescans; acceptable for first implementation)

### Fifth review pass (2026-06-10, principal engineer final sign-off):

**Encoding branch** (`variant-encoding`, ce4befffe6):
1. Added comment in `FinishArray` noting the implicit ~4GB size limit from 4-byte offsets (not validated at runtime; Parquet row group sizes are bounded well below)
2. Added comment in `Finish()` noting that Go's `metadataMaxSizeLimit` is 128MB while C++ only enforces the spec's ~4GB offset maximum — a deliberate choice for spec-correctness over arbitrary defensive limits

**Review outcome**: LGTM. No blocking issues found. Implementation is spec-conformant, Go-parity is strong with documented deliberate divergences, test coverage is thorough (165 decoder + 73 encoder tests), and code matches Arrow C++ conventions. All recommended improvements are comment-only (no logic changes).

### Sixth review pass (2026-06-11, CI failure fix):

**Both branches** (`variant-decoding` e980fd0867, `variant-encoding` 7f51026fb8):
1. Renamed namespace `arrow::extension::variant` → `arrow::extension::variant_internal` across all 6 files (header, .cc, tests, test util, builder .cc, builder test)
2. **Root cause**: Unity builds (used by CI) compile multiple `.cc` files into one translation unit. `parquet_variant.cc` defines a function `arrow::extension::variant(std::shared_ptr<DataType>)`. Our namespace `arrow::extension::variant {}` collided with that function — same fully-qualified name, different entity types. Non-Unity builds (our Docker tests) never saw both in the same TU.
3. This was caught by 5 CI checks failing on PR #50121 (C GLib/Ruby MinGW, C++ Windows AVX2, Lint, Integration, Python Windows)

**Lesson learned**: Always check for name collisions with existing symbols in the `arrow::extension` namespace before choosing a sub-namespace name. Unity builds expose collisions that separate compilation hides.

### Final state:
- Both branches amended into single commits, clean working tree
- All 238 tests pass with `BUILD_WARNING_LEVEL=CHECKIN` in Docker (encoding branch)
- All 165 tests pass with `BUILD_WARNING_LEVEL=CHECKIN` in Docker (decoding branch alone)
- Namespace: `arrow::extension::variant_internal` (avoids Unity build collision)
- Pushed: `variant-decoding` (e980fd0867), `variant-encoding` (7f51026fb8)

---

## Key Files in This Notes Repo

| File | Purpose |
|------|---------|
| `AGENT.md` | This file — agent context for continuing work |
| `arrow_issue_45946_solution.md` | Detailed decoder design doc |
| `arrow_issue_45947_solution.md` | Encoder design doc |
| `arrow_issue_45946_45947_solution.md` | Combined plan with Go parity analysis |
| `arrow_issue_45946_45947_45948_solution.md` | Full roadmap including shredding |
| `arrow_go_bug.md` | Go `valueSize()` bug analysis + reproducer |
| `VariantEncoding.md` | Variant binary encoding spec (from parquet-format) |
| `VariantShredding.md` | Variant shredding spec (for future GH-45948) |
| `decoding_pr_plan.md` | Original PR plan |
| `development_strategy.md` | Overall strategy doc |
