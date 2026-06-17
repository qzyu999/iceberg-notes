# Variant Encode/Decode — Agent Context

> Last updated: 2026-06-16 (24th review pass applied)
> Owner: @qzyu999
> Umbrella issue: GH-45937 [C++][Parquet] Add variant support

---

## ⚡ QUICK START — Shredding Complete (principal engineer review fixes applied, uncommitted)

**Status:** Branch `variant-shredding-impl` (base commit `c5971e293c` + uncommitted review fixes) needs **Docker re-verification** after twenty-fourth review pass (ExtractDoubleOrFloat rename, DECIMAL256 TODO, lifetime comment, PERF TODO, header doc).

**What's done:**
- Full Rust parity for primitive/object/array shredding with native typed_value extraction
- **Object sub-field native extraction (Rust parity MAJOR)** — Primitive sub-schemas now recursively shred field values into native typed columns (Int64Array, StringArray, etc.) via `ShredVariantColumn` re-use. Enables Parquet statistics-based predicate pushdown on nested object fields. Reconstruction pre-computes per-field arrays (O(n) per field).
- Decimal128 support with width-preserving reconstruction (Decimal4/8/16)
- Strict timestamp type/unit/timezone matching in `IsVariantCompatibleWithType()`
- **Strict Time64 unit matching** — Only accepts `time64(MICRO)` targets (Rust parity; prevents micros→nanos misinterpretation)
- Strict decimal scale matching in `IsVariantCompatibleWithType()`
- **Variant::Null semantics fixed** — matches Rust: Null goes to value column, NOT typed_value (distinguishes variant-null from SQL NULL)
- Performance: `BuildWithoutMeta()` used in primitive reconstruction (avoids per-row metadata allocation)
- **Performance: Object shredding O(s+k) refactored** — Replaced O(s×k) inner marking loop with single-pass `unordered_map` name→index lookup
- **Performance: Object reconstruction builder reuse** — Cached `VariantBuilder` and metadata across rows with identical metadata bytes (common case: all rows share same metadata → O(1) amortized builder construction instead of O(n×k))
- **INT8/INT16 shredding targets added** — Rust parity for all integer widths
- **LARGE_STRING/LARGE_BINARY shredding targets added** — Rust parity for all string/binary types
- **STRING_VIEW/BINARY_VIEW shredding targets added** — Full Rust parity for view-based string/binary types (shred + reconstruct)
- **LargeList reconstruction support added** — Accepts both LIST and LARGE_LIST typed_value arrays in array reconstruction (64-bit offset support)
- **Typed_value field type correctness** — TIMESTAMP/TIME64 output fields now declare `int64()` matching physical storage (prevents downstream schema validation issues)
- **Big-endian safety** — `ReadLE` and Decimal128 reconstruction now endian-safe (s390x CI)
- **Template refactor** — primitive shred loop deduplicated via `ShredPrimitiveLoop<>` / `ShredBinaryLoop<>` templates (-214 lines)
- **Defensive defaults** — `VariantShreddingSchema::kind_` initialized, `GetBinaryValue` DCHECK on unsupported types
- **Comprehensive Rust parity TODOs** — Uint8/16/32/64, Float16, Decimal32/64, TimestampSecond/Millisecond documented as cast-mode gaps
- **Explicit standard library includes** — `<cstdint>` in header, `<string_view>` in .cc (C++ standard compliance)
- All compilation warnings resolved (werror mode)
- Tests verified in Docker (`arrow-ext-test:latest`) — **need re-run after 20th review pass**
- **Review fixes applied** (uncommitted): 
  - 7th pass: timestamp reconstruction bug, error handling, TODOs, 7 new tests
  - 8th pass: timestamp/decimal compatibility hardening, width-preserving decimal, BuildWithoutMeta optimization, meson install fix, DCHECK additions, 6 new type-compatibility tests
  - 9th pass: Variant::Null Rust-parity fix, C++20→C++17 designated initializer fix, int auto-sizing reconstruction comment
  - 10th pass: INT8/INT16 shred/reconstruct, C++20 test fix, Float→Double test, reconstruction ambiguity comments, builder reuse comment
  - 11th pass: LARGE_STRING/LARGE_BINARY shred/reconstruct targets, 2 new tests
  - 12th pass: kShortString→BINARY/LARGE_BINARY compat removed (dead path, Rust mismatch), Rust cast-based divergence TODO, ReadLE LE-only caveat, StringView/BinaryView TODOs, header doc enhancement, test helper comment fix
  - 13th pass: typed_value field type fix (TIMESTAMP/TIME64 → int64()), SetAllowDuplicates comment enhancement, LargeList/FixedSizeList/ListView TODOs, header Rust parity doc, 2 new error tests
  - 14th pass: input type validation in ShredVariantColumn/ReconstructVariantColumn, array length consistency checks, DCHECK for value_ in VariantExtensionType, narrowing-cast comments in ExtractInt64
  - 15th pass: big-endian `ReadLE` fix (byte-shift instead of memcpy+mask), endian-safe Decimal128 reconstruction (high_bits/low_bits accessors), type validation in ReconstructVariantColumnArray/Object, `default: return false` in IsVariantCompatibleWithType, input validation in ReconstructVariantColumn entry point, 4 new error tests
  - 16th pass: template refactor (`ShredPrimitiveLoop<>` / `ShredBinaryLoop<>` — eliminates ~360 lines of copy-paste), `GetBinaryValue` DCHECK on unsupported type, `kind_` default init, `#include <cstdint>` in header, expanded Rust parity TODOs (Uint/Float16/Decimal32/64/TimestampSecond/Milli), `parquet_variant.cc` comment on value-absent schema rejection
  - 17th pass: **object sub-field native extraction implemented** (recursive `ShredVariantColumn` for Primitive sub-schemas), pre-computed per-field reconstruction in `ReconstructVariantColumnObject`, explicit sign-extension casts, Go-reference comment removed, 1 new test + 1 enhanced test
  - 18th pass: **principal engineer hardening** — `#include <cstdint>` in `variant_shredding.cc`, `ReconstructVariantColumnObject` bounds-check on typed_struct field count vs schema (prevents UB on mismatched schemas), `UnsafeAppendEncoded` defensive no-op for size≤0 (DCHECK retained for debug, graceful in release), `BuildWithoutMeta` post-move clear() comment, 1 new error test (`ReconstructObjectFieldCountMismatch`)
  - 19th pass: **C++ standards compliance + documentation** — `#include <string_view>` in `variant_shredding.cc` (the file uses `std::string_view` extensively in `GetBinaryValue`, extraction helpers, and `ShredPrimitiveLoop`; was relying on transitive inclusion via `variant_internal.h` which is fragile under unity builds and include-order changes), `ToArrowType()` doc enhancement (logical vs physical type distinction for TIMESTAMP/TIME64 documented in header), `ReconstructVariantColumnObject` per-row builder creation optimization TODO added (notes O(n×k) cost and suggests builder pooling follow-up)
  - 20th pass: **principal engineer final review — test correctness + defensive hardening** — renamed `FloatNotCompatibleWithFloat64` → `FloatCompatibleWithFloat64ViaWidening` (test name contradicted assertion), added `FloatCompatibleWithFloat32` test for explicit same-type coverage, added `DCHECK_NE(list_arr->values(), nullptr)` in `ReconstructVariantColumnArray` for defense-in-depth, `PERF TODO` prefix on `ObjectFieldShredder::AppendObject` field-lookup TODO (clarifies real performance implications for wide objects), NullArray usage comment in `ShredVariantColumnObject` explaining why null-typed array is semantically acceptable for non-Primitive sub-schemas, `ShredPrimitiveLoop` `native_val` initialization comment (documents that value is only read on extract success), test helper `.ok()`/`.ValueOrDie()` convention comment added (mirrors `variant_builder_test.cc` established pattern)
  - 21st pass: **principal engineer performance refactoring** — `ObjectFieldShredder::AppendObject` refactored from O(s×k) to O(s+k) via single-pass `unordered_map<string_view, FieldInfo>` field lookup (eliminates inner marking loop that re-iterated all object fields per schema field), `ReconstructVariantColumnObject` builder reuse via cached `unique_ptr<VariantBuilder>` + metadata bytes comparison (eliminates O(n×k) dictionary copies for common case of uniform metadata), `#include <unordered_map>` added
  - 22nd pass: **Rust parity — StringView/BinaryView shredding + LargeList reconstruction** — STRING_VIEW and BINARY_VIEW added as full shredding targets (shred + reconstruct), `GetBinaryValue` helper extended to support BinaryViewArray/StringViewArray, `IsVariantCompatibleWithType` updated for STRING_VIEW/BINARY_VIEW compatibility, input type validation broadened to accept BINARY_VIEW, `ReconstructVariantColumnArray` refactored via generic lambda to accept both LIST and LARGE_LIST typed_value arrays (64-bit offset support for Parquet files), TODOs updated to reflect resolved gaps, 5 new tests (StringViewShredRoundTrip, BinaryViewShredRoundTrip, ShortStringToStringView, LargeListReconstructRoundTrip, ReconstructArrayTypedValueLargeListAccepted)
  - 23rd pass: **principal engineer parity audit — Time64 unit validation + cleanup** — `IsVariantCompatibleWithType` now validates `Time64Type` unit (only accepts `TimeUnit::MICRO`, rejects NANO — prevents micros→nanos misinterpretation in typed_value column; matches Rust which uses `Time64MicrosecondType` specifically), removed duplicate TODO block (copy-paste artifact from 16th pass edits — Decimal32/64 + TimestampSecond/Milli lines were repeated verbatim), added PERF comment on residual loop's redundant `GetObjectFieldAt` call (documents acceptable O(1) cost per field), added BinaryViewBuilder int32→int64 widening documentation comment, 3 new tests (`Time64MicroCompatibleWithTime64Micro`, `Time64NanoNotCompatibleWithTime64Micro`, `ZeroRowInput` — covers all three schema kinds with empty arrays)
  - 24th pass: **principal engineer final polish — naming, documentation, performance TODOs** — renamed `ExtractDouble` → `ExtractDoubleOrFloat` (clarifies it handles both kFloat widening and native kDouble), added DECIMAL256 scale asymmetry TODO in `IsVariantCompatibleWithType` (notes divergence from strict Decimal128 scale matching), added `cached_meta_bytes` lifetime safety comment in `ReconstructVariantColumnObject` (documents string_view→metadata_array buffer lifetime guarantee), added PERF TODO on `ObjectFieldShredder::AppendObject` per-row `unordered_map` allocation (suggests lifting map to struct for column-scan reuse), added `\return` documentation in `variant_shredding.h` for `ReconstructVariantColumn` (clarifies that both-null produces 0x00 and callers must check struct validity bitmap for SQL NULL vs variant-null distinction). **Encoding branch (70a364b71e):** implemented `AddKey()` `lookup_buf_` optimization — replaces per-call `std::string` temporary with a reusable member buffer; `assign()` reuses existing capacity for keys that fit, eliminating the dominant allocation in column-scan workloads. Force-pushed to origin.

**Next actions (when ready to submit PR):**
1. Run Docker tests to verify all review pass changes compile and pass:
   ```bash
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
2. Stage and amend the review fixes into the single commit:
   ```bash
   git add -A && git commit --amend --no-edit
   ```
3. Run clang-format:
   ```bash
   docker run --rm -v "${PWD}:/arrow" -w /arrow ubuntu:24.04 bash -c \
     "apt-get update && apt-get install -y clang-format-18 >/dev/null 2>&1 && \
     find cpp/src/arrow/extension/variant* -name '*.cc' -o -name '*.h' | \
     xargs clang-format-18 -i"
   ```
4. Amend commit after formatting: `git add -A && git commit --amend --no-edit`
5. Push: `git push origin variant-shredding-impl --force-with-lease`
6. Create PR targeting `variant-encoding` branch (or `main` after 45946+45947 merge)

**Key files:**
- `cpp/src/arrow/extension/variant_shredding.cc` — ~1720 lines, core shred/reconstruct engine (template-refactored, object native extraction, performance-optimized, StringView/BinaryView/LargeList)
- `cpp/src/arrow/extension/variant_shredding_test.cc` — ~1480 lines, all round-trip + error tests
- `cpp/src/arrow/extension/variant_shredding.h` — ~180 lines, public API
- `cpp/src/arrow/extension/parquet_variant.h/.cc` — updated for shredded storage

**Design doc:** `C:\Users\jx815f\Desktop\development\iceberg-notes\pyiceberg_variant\arrow_issue_45948_solution_proposal.md`

---

## Repository Layout

| Repo | Local Path | Remote |
|------|-----------|--------|
| Apache Arrow (C++) | `C:\...\arrow` | `origin` = `qzyu999/arrow`, `upstream` = `apache/arrow` |
| Apache Arrow Go | `C:\...\arrow-go` | `origin` = `qzyu999/arrow-go`, `upstream` = `apache/arrow-go` |
| Apache Arrow Rust | `C:\...\arrow-rs` | Reference only (read for parity checks) |
| Notes/Context (this repo) | `C:\...\iceberg-notes` | — |

---

## Branch Structure (C++ — `apache/arrow`)

```
main (e16067a78c)
  └── variant-decoding (e980fd0867) — GH-45946: [C++][Parquet] Variant decoding
       └── variant-encoding (70a364b71e) — GH-45947: [C++][Parquet] Variant encoding
            └── variant-shredding-impl (f855bcac4d) — GH-45948: [C++][Parquet] Variant shredding ✅
```

- **Linear history**: shredding sits on top of encoding, which sits on top of decoding.
- **Single commit per branch** — clean for squash-merge or rebase by reviewers.
- **Ready for force-push** to `origin/variant-decoding` and `origin/variant-encoding`.
- **Shredding is NOT yet pushed** — local only on `variant-shredding-impl`.
- Merge order: **45946 first, then 45947, then 45948**. Each targets the previous (or main after merge).
- **Docker tests pass**: 286/286 tests (shredding, pre-22nd pass — needs re-verification ~291 expected after 22nd pass), 238/238 tests (encoding), 165/165 tests (decoding standalone), `BUILD_WARNING_LEVEL=CHECKIN` (warnings-as-errors).
- **Shredding branch**: Implementation complete and Docker-verified. All three shredding paths (primitive, object, array) working with native typed_value extraction + Decimal128 support. Review fixes applied (timestamp bug, error handling, new tests).
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
- `AddKey()` uses `lookup_buf_` member to avoid per-call std::string allocation for hash map lookups (C++17-compatible optimization; reuses buffer capacity for existing keys)
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

### Branch: `variant-shredding-impl` (c5971e293c + uncommitted review fixes) ✅ DOCKER VERIFIED

**Scope**: Full variant shredding — builder extensions, schema definition, type compatibility, shred/reconstruct kernels for all three paths (primitive, object, array), VariantExtensionType evolution.

**Test results**: 286/286 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN` (verified 2026-06-11, needs re-verification after 16th review pass)

**Files** (12 changed, +2430→~2720 lines on top of encoding after 22nd pass):
- `cpp/src/arrow/extension/variant_internal.h` — Added `BuildWithoutMeta()`, `UnsafeAppendEncoded()`, `SetAllowDuplicates()` + `allow_duplicates_` member
- `cpp/src/arrow/extension/variant_builder.cc` — Implementation of 3 new methods + `FinishObject()` duplicate handling
- `cpp/src/arrow/extension/variant_builder_test.cc` — 10 new tests for builder extensions
- `cpp/src/arrow/extension/variant_shredding.h` (~175 lines) — `VariantShreddingSchema`, `IsVariantCompatibleWithType()`, `ShredVariantColumn()`, `ReconstructVariantColumn()`
- `cpp/src/arrow/extension/variant_shredding.cc` (~1720 lines) — Full shred/reconstruct engine (template-refactored, StringView/BinaryView/LargeList)
- `cpp/src/arrow/extension/variant_shredding_test.cc` (~1480 lines) — Schema + compat + round-trip + error tests
- `cpp/src/arrow/extension/parquet_variant.h` — Added `typed_value()`, `is_shredded()`, updated doc
- `cpp/src/arrow/extension/parquet_variant.cc` — `IsSupportedStorageType()` accepts shredded storage; constructor finds fields by name + DCHECK; comment on value-absent schema rejection
- Build files (CMake + Meson) — Added `variant_shredding.cc` and test; `variant_shredding.h` in install list

**What's implemented (✅ — Rust parity achieved, Docker-verified):**
- `BuildWithoutMeta()` — raw value bytes without metadata
- `UnsafeAppendEncoded(data, size)` — zero-copy append of pre-encoded variant bytes
- `SetAllowDuplicates(bool)` — last-value-wins duplicate key compaction
- `VariantShreddingSchema` — tree schema: `Primitive(DataType)`, `Object({name: sub_schema})`, `Array(elem_schema)`
- `ToArrowType()` — converts schema to Arrow DataType with proper struct wrapping
- `IsVariantCompatibleWithType()` — **strict** type matching: checks TimeUnit+timezone for timestamps, scale for decimals, byte_width for UUID
- `VariantExtensionType` — supports shredded storage (`{metadata, value?, typed_value?}`)
- **Primitive shredding** — native extraction for: Int8, Int16, Int32, Int64, Float, Double, Bool, String, LargeString, StringView, Date32, Timestamp, Time64, Binary, LargeBinary, BinaryView, UUID (FixedSizeBinary(16)), Decimal128
- **Template-based shred loop** — `ShredPrimitiveLoop<BuilderT, NativeT, ExtractFn>()` and `ShredBinaryLoop<BuilderT>()` eliminate per-type copy-paste
- **Object shredding** — field-level routing to typed_value sub-columns + residual object construction via `BuildWithoutMeta()`. Primitive sub-schemas recursively extract native typed columns (Int64Array, StringArray, etc.) via `ShredVariantColumn` reuse.
- **Object reconstruction** — pre-computed per-field reconstruction (O(n) per field), merge shredded fields + residual using `UnsafeAppendEncoded()` and `SetAllowDuplicates()`
- **Array shredding** — element extraction into ListArray of binary variant bytes
- **Array reconstruction** — list/large_list → variant array via `UnsafeAppendEncoded()` (accepts both 32-bit and 64-bit offset lists)
- **Primitive reconstruction** — re-encodes all native types back to variant bytes using `BuildWithoutMeta()` (O(1) per row, no metadata rebuild)
- **Decimal width preservation** — reconstruction uses smallest encoding (Decimal4/8/16) that fits the value, ensuring round-trip byte identity
- Round-trip identity: `Reconstruct(Shred(v)) == v` proven for all three schema kinds

**What remains (minor, non-blocking — can be follow-up PRs):**
1. ~~**Recursive native extraction for object sub-fields**~~ — ✅ DONE (17th pass): Primitive sub-schemas now recursively extract native typed columns via `ShredVariantColumn` reuse.
2. **Recursive shredding for nested Object/Array sub-schemas** — Object/Array sub-schema fields still store variant binary in the "value" sub-column (only Primitive sub-schemas get native extraction). Enabling recursive Object/Array shredding would require recursive `ShredVariantColumnObject`/`Array` calls.
3. **Parquet bridge** — `VariantToNode`/`NodeToArrow` in `parquet/arrow/schema.cc` (C++-specific, not in Rust's shredding module)
4. **clang-format** — needs to be run before pushing
5. ~~**BinaryView/StringView support**~~ ✅ DONE (22nd pass): Full shred + reconstruct for STRING_VIEW and BINARY_VIEW targets
6. **FixedSizeList/ListView support** — Rust supports all list-like types; C++ supports List + LargeList (reconstruction). FixedSizeList and ListView remain as TODO.
7. **Cast-based mode** — Uint8/16/32/64, Float16, Decimal32/64, TimestampSecond/Millisecond (requires CastOptions infrastructure analogous to Rust's `shred_variant_with_options()`)
8. **Value-absent shredded schemas** — spec allows `{metadata, typed_value}` without `value`; currently rejected by `IsSupportedStorageType()` (documented)

**Rust reference files:**
- Shredding: `arrow-rs/parquet-variant-compute/src/shred_variant.rs`
- Reconstruction: `arrow-rs/parquet-variant-compute/src/unshred_variant.rs`
- Type conversion: `arrow-rs/parquet-variant-compute/src/variant_to_arrow.rs`

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
| BuildWithoutMeta | — | ✅ (shredding branch) | `Builder.BuildWithoutMeta()` |
| UnsafeAppendEncoded | — | ✅ (shredding branch) | `Builder.UnsafeAppendEncoded()` |
| SetAllowDuplicates | — | ✅ (shredding branch) | `Builder.SetAllowDuplicates()` |
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

### Test results (verified 2026-06-10, shredding needs re-verification after 8th review)

| Branch | Tests | Result | Warning Level |
|--------|-------|--------|---------------|
| `variant-decoding` | 165 (standalone) | ✅ PASSED | CHECKIN (werror) |
| `variant-encoding` | 238 (full suite) | ✅ PASSED | CHECKIN (werror) |
| `variant-shredding-impl` | ~314 (full suite) | ⏳ PENDING | CHECKIN (werror) |

> The shredding branch includes all extension tests (bool8, json, uuid, opaque, tensor,
> variant decoder, variant builder, variant shredding). Expected ~314 = 165 decoder + 73 encoder + 76 shredding (48 original + 6 from 8th pass + 3 from 10th pass + 2 from 11th pass + 3 compat tests from 8th pass + 2 error tests from 13th pass + 2 error tests from 15th pass + 1 from 17th pass + 1 from 18th pass + 5 from 22nd pass + 3 from 23rd pass).
> **Note:** 9th pass changes (Variant::Null semantics) do not add new tests but modify 2 existing tests. 17th pass enhances `FullyShredded` test and adds `MissingFieldNativeExtraction` test. 18th pass adds `ReconstructObjectFieldCountMismatch` error test. 22nd pass adds StringView/BinaryView shred + LargeList reconstruction tests. 23rd pass adds Time64 unit compatibility tests and zero-row edge case test.

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

### 11. `IsVariantCompatibleWithType` strict timestamp unit+timezone matching

**C++ checks TimeUnit and timezone. Rust does the same.**

`IsVariantCompatibleWithType` enforces that `kTimestampMicros` is only compatible with `timestamp(MICRO, <non-empty tz>)`, not with `timestamp(NANO, ...)` or NTZ targets. This prevents a subtle data corruption path where a Micros value could be shredded into a Nano column and reconstructed as the wrong type.

The Rust implementation enforces this implicitly through its type-specific `AppendToVariantBuilder` trait implementations (each timestamp variant has a dedicated builder type that only accepts matching Arrow timestamps).

### 12. Decimal reconstruction preserves encoding width

**C++ preserves Decimal4/8/16. Rust preserves the original width via dedicated types.**

During reconstruction, we check if the Decimal128 value fits in 4 bytes (→ Decimal4), 8 bytes (→ Decimal8), or requires full 16 bytes (→ Decimal16). This ensures byte-identical round-trips: `Reconstruct(Shred(Decimal4(12345, scale=2)))` produces Decimal4, not Decimal16.

The Rust implementation achieves this through separate `VariantDecimal4`/`VariantDecimal8`/`VariantDecimal16` types that track the original width. Our approach is equivalent but infers width from value magnitude at reconstruction time.

### 13. Float→Double widening is a lossy type-tag round-trip

`IsVariantCompatibleWithType` allows `kFloat` to be compatible with `Type::DOUBLE`. This means `Shred(Float(3.14f))` stores `double(3.14f)` in the typed column, and `Reconstruct()` produces `Double(3.14...)` — the variant type changes from Float to Double (value precision is preserved since float→double is lossless for the numeric value).

This matches the Rust behavior where widening is allowed during shredding but narrows the set of types that pass through the typed column. A follow-up could add a `Type::FLOAT` shredding target for Float-only columns.

### 14. Variant::Null is NOT shredded (matches Rust)

**C++ and Rust agree: Variant::Null stays in the value column.**

`IsVariantCompatibleWithType` returns `false` for `kNull`. This means `Variant::Null` (the encoded `0x00` byte) is stored in the `value` column with `typed_value` null. This is semantically important: it distinguishes "variant-typed null" (value=0x00, typed=null, struct-valid) from "SQL NULL / missing" (value=null, typed=null, struct-invalid or both null).

The Rust test (`test_primitive_shredding_comprehensive`, row 4) explicitly verifies this: `value` is valid and `typed_value` is null for `Variant::Null`.

### 15. Int auto-sizing during reconstruction

**C++ uses `vb.Int()` which auto-sizes. Rust does the equivalent.**

When reconstructing a shredded Int64 column, `vb.Int(42)` produces `Int8(42)` (smallest encoding). The variant *value* is preserved but the *encoding width* may narrow. This is acceptable per the spec (values must be preserved, not encodings). The Rust implementation has the same behavior.

### 16. Typed_value field declares physical type (int64) for TIMESTAMP/TIME64

**C++ declares the typed_value StructArray field as `int64()` for TIMESTAMP and TIME64 schemas.**

Arrow stores timestamps as int64 internally, and the shredding uses `Int64Builder` to produce the typed_value array. Rather than declaring the field as the logical type (e.g., `timestamp(MICRO, "UTC")`) which would mismatch the actual array type, we use `int64()` — the physical storage type. The reconstruction path dispatches on `schema.type()->id()` to determine how to re-encode the variant, so the semantic information is preserved through the schema, not the field metadata.

Rust stores timestamps in `PrimitiveArray<TimestampMicrosecondType>` which carries the logical type. The C++ difference is because Arrow C++ doesn't have a `TimestampBuilder` — timestamps are built as `Int64Builder` and the type is only available through casting. This is a cosmetic divergence; the data and semantics are identical.

### 17. `FinishObject` sorts fields in-place via mutable reference

**C++ `FinishObject` takes `std::vector<FieldEntry>&` (non-const ref) and reorders the caller's vector.**

The spec requires field IDs and offsets to be in lexicographic order of key names. Rather than copying and sorting internally, `FinishObject` sorts the caller's `fields` vector in-place and (in allow_duplicates mode) compacts it. This is an unusual API pattern — callers pass a mutable vector that gets reordered under them.

**Rationale**: Avoids an O(n) copy of the fields vector on every object construction. The pattern is documented in the header ("sorts in-place") and all callers construct `fields` as a local variable that is not reused after the call. This is a deliberate API design choice for the builder's hot path.

**Reviewer callout**: Mention this in the PR description. The non-const-ref parameter signals mutating intent per C++ conventions, but reviewers unfamiliar with the builder may wonder why the vector is modified.

### 18. Reconstruction both-null emits Variant::Null (0x00) unconditionally

**C++ `ReconstructVariantColumn` produces a non-nullable BinaryArray. When both value and typed_value are null for a row, the output contains a 0x00 byte (Variant::Null).**

This means SQL NULL (structurally-absent row) is indistinguishable from Variant::Null in the reconstructed output. The Rust implementation avoids this ambiguity by returning a separate `NullBuffer` alongside the data array, which tracks struct-level validity independently.

**Impact**: Callers that round-trip through shred→reconstruct lose the ability to distinguish SQL NULL from variant-null **unless** they check the original struct-level validity bitmap before calling `ReconstructVariantColumn`. This is documented in the `\return` description in `variant_shredding.h`.

**Follow-up consideration**: Accept an optional validity bitmap parameter to produce a nullable output array. This would achieve full parity with Rust's `unshred_variant` semantics. Acceptable as a first implementation without this — the common Parquet use case has all rows valid (NULL is encoded at the Parquet definition level, not the variant encoding level).

---

## Additional Issues Filed

| Issue/PR | Repo | Description |
|----------|------|-------------|
| apache/arrow-go#839 (PR) | arrow-go | `valueSize()` array `is_large` bit position fix |
| TBD | arrow-go | `ObjectValue.ValueByKey()` unsigned underflow in binary search |

---

## What's Next

### GH-45948: Variant Shredding (COMPLETE — Docker verified, ready for PR)
- **Branch**: `variant-shredding-impl` (base commit `c5971e293c` + uncommitted review fixes, local only — not yet pushed)
- **Depends on**: both encoder and decoder (merged into branch lineage)
- **Test result**: 286/286 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN`
- **Remaining before PR:**
  1. Amend review fixes into the commit (`git add -A && git commit --amend --no-edit`)
  2. Run clang-format-18 (style compliance)
  3. Push to origin
  4. Create PR
- **Follow-up work (separate PRs):**
  - ~~Recursive native extraction for object sub-fields~~ ✅ DONE (17th pass)
  - Recursive shredding for nested Object/Array sub-schemas
  - Parquet bridge (C++-specific reader/writer integration)
  - ~~BinaryView/StringView shredding targets~~ ✅ DONE (22nd pass)
  - ~~LargeList reconstruction~~ ✅ DONE (22nd pass)
  - FixedSizeList/ListView shredding targets
  - CastOptions mode (Uint, Float16, Decimal32/64, TimestampSecond/Milli)
  - Value-absent shredded schemas
- See `arrow_issue_45948_solution_proposal.md` for full design doc
- See `arrow_issue_45948_solution.md` for review findings + fixes applied

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
  - `FinishObject` sorts fields in-place — spec requires field IDs in lexicographic key order. The `std::vector<FieldEntry>&` parameter (non-const ref) mutates the caller's vector — an intentional API choice documented in the header. All callers construct `fields` as a local variable not reused after the call.
  - `AddKey` constructs a temporary `std::string` on every lookup (heterogeneous lookup TODO documented). This is measurable for column-scan workloads but acceptable for first PR — the fix requires changing `dict_` to a transparent-hash map which is a follow-up optimization.
  - Builder is move-only, dictionary preserved across `Finish()` calls
  - TODOs documented for GH-45948 shredding: `BuildWithoutMeta`, `UnsafeAppendEncoded`, `SetAllowDuplicates`
  - 4GB size limit comment — spec's 4-byte offset maximum (Go enforces stricter 128MB)
- **What was tested**: 238 total tests (73 encoder + 165 decoder) pass with `BUILD_WARNING_LEVEL=CHECKIN`

### Push Commands (already pushed 2026-06-11)
```bash
# Force-push updated decoder branch:
git push origin variant-decoding --force-with-lease  # done → e980fd0867

# Force-push updated encoding branch:
git push origin variant-encoding --force-with-lease  # done → 70a364b71e (24th pass: AddKey lookup_buf_ optimization)
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

### Seventh review pass (2026-06-11, shredding self-review fixes):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Critical fix:**
1. **Timestamp reconstruction bug (issue 2.1)**: Changed reconstruction switch from dispatching on `typed_value_array->type_id()` (which was `INT64` — the physical storage type) to `schema.type()->id()` (which is `TIMESTAMP`). The `TIMESTAMP` case now reads `TimeUnit` and timezone from the schema to select the correct encoder method. This fixes a round-trip data corruption bug where all timestamps were being re-encoded as plain `Int()`.

**Error handling fix:**
2. **Replaced `.ok()` / `.ValueOrDie()` with proper error handling (issue 4.3)**: All reconstruction encoder calls now use `ARROW_RETURN_NOT_OK()` and `ARROW_ASSIGN_OR_RAISE()` instead of discarding Status.

**Documentation / TODO comments added:**
3. **ReadLE comment (issue 2.2)**: Documented the zero-extend + narrowing-cast pattern
4. **O(n²) field lookup TODO (issue 2.3)**: Documented performance concern in `ObjectFieldShredder::AppendObject`
5. **Primitive code duplication TODO (issue 3.1)**: Suggested template refactor
6. **Object shredding TODO (issue 3.2)**: Enhanced comment for recursive native extraction follow-up
7. **Array shredding TODO (issue 3.3)**: Added recursive element shredding TODO
8. **GetBinaryValue BinaryView TODO (issue 3.4)**: Warning about silent data loss

**New tests added (7 tests):**
9. `Decimal128RoundTrip` — Decimal128 shredding with matching scale round-trip
10. `Decimal128ScaleMismatch` — Scale mismatch falls to residual
11. `UUIDRoundTrip` — UUID (FixedSizeBinary(16)) round-trip
12. `TimestampMicrosRoundTrip` — Verifies header byte = 0x30
13. `TimestampNanosRoundTrip` — Verifies header byte = 0x48
14. `TimestampMicrosNTZRoundTrip` — Verifies header byte = 0x34
15. `TimestampNanosNTZRoundTrip` — Verifies header byte = 0x4C

**Verification**: 286/286 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN` in Docker (all extensions).

### Eighth review pass (2026-06-11, principal engineer Rust-parity review):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fixes (data corruption / spec compliance):**
1. **`IsVariantCompatibleWithType` — strict timestamp checking**: Now validates TimeUnit (MICRO vs NANO) and timezone presence (TZ vs NTZ). Previously `TimestampMicros` was incorrectly considered compatible with `timestamp(NANO, "UTC")`, causing data corruption on reconstruction. Matches Rust's strict unit/timezone matching.
2. **`IsVariantCompatibleWithType` — strict decimal scale checking**: Now reads the scale byte from variant binary data and compares against the target type's scale. Previously returned `true` for any decimal→Decimal128 match regardless of scale.
3. **Timestamp shred loop — uses `IsVariantCompatibleWithType` as gatekeeper**: The `Type::TIMESTAMP` shred case now calls `IsVariantCompatibleWithType(data, len, target_type)` before `ExtractTimestamp()`, ensuring only correctly-matching timestamp variants reach the typed column (prevents TimestampMicros from being shredded into a NANO column).
4. **Decimal shred loop — uses `IsVariantCompatibleWithType` as gatekeeper**: Consolidated the per-row scale check to use the public API function (DRY, no possibility of disagreement between function and shred loop).

**Performance fix:**
5. **Primitive reconstruction uses `BuildWithoutMeta()`**: Replaced `vb.Finish()` (which allocates metadata dictionary per row) with `vb.BuildWithoutMeta()`. Primitives don't reference dictionary keys. Also replaced "both null" case with direct `0x00` byte write (no builder overhead). Reduces reconstruction from O(n) allocations to O(1).

**Correctness fix (round-trip identity):**
6. **Decimal reconstruction preserves encoding width**: Instead of always emitting Decimal16, now checks if value fits in 4 bytes (Decimal4) or 8 bytes (Decimal8). Ensures `Reconstruct(Shred(Decimal4(x))) == Decimal4(x)` byte-identical round-trip.

**Build system fix:**
7. **`variant_shredding.h` added to meson.build install list**: The header uses `ARROW_EXPORT` (public API) but was missing from the selective Meson install list. CMake's `arrow_install_all_headers()` covers it implicitly, but Meson requires explicit listing.

**Code quality:**
8. **`VariantExtensionType` constructor DCHECK**: Added `DCHECK_NE(metadata_, nullptr)` after field-finding loop. Catches programming errors where the public constructor is called without prior `IsSupportedStorageType()` validation.
9. **`ToArrowType()` unreachable path**: Added `DCHECK(false)` instead of silent `nullptr` return.
10. **`ReadLE` documentation**: Added comment explaining relationship to `variant_internal.cc`'s `ReadUnsignedLE` (returns int64_t for 8-byte signed extraction).
11. **Float→Double widening comment**: Documented that shred(Float)→reconstruct produces Double (lossy type-tag round-trip, value precision preserved).
12. **`ObjectFieldShredder` construction**: Uses C++20 designated initializers for clarity.
13. **Object shredding complexity comment**: Full O(s × k) analysis with inner marking loop.

**New tests (6 tests):**
14. `TimestampMicrosNotCompatibleWithNanos` — MICRO variant rejected by NANO target
15. `TimestampMicrosNotCompatibleWithNTZ` — TZ variant rejected by NTZ target
16. `TimestampNanosNTZCompatibleWithNanosNTZ` — NanosNTZ passes with matching target
17. `DecimalScaleMismatchNotCompatible` — scale=3 rejected by scale=2 target
18. `DecimalScaleMatchCompatible` — scale=2 accepted by scale=2 target

**Verification**: PENDING — needs Docker re-run after 8th pass changes.

### Ninth review pass (2026-06-11, full Rust/C++ parity audit):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**CRITICAL fix (Rust semantic divergence):**
1. **`Variant::Null` handling — matches Rust semantics**: Previously `Variant::Null` was routed to typed_value (both columns null). Per the Rust implementation (`test_primitive_shredding_comprehensive`, row 4), `Variant::Null` must be stored in the `value` column as raw bytes (`0x00`), with `typed_value` null. This distinguishes "variant-typed null" (value=0x00, typed=null) from "SQL NULL / missing" (both null). Fixed in all 12 primitive shred cases.
2. **`IsVariantCompatibleWithType` returns `false` for `kNull`**: Previously returned `true` (Null compatible with anything). Per Rust, Null is NOT shredded — it always goes to the value column.
3. **Updated `NullCompatibleWithAnything` test → `NullNotCompatibleWithTypedColumns`**: Tests now assert `false` for null compatibility, matching Rust behavior.
4. **Updated `NullVariantIsRouted` test**: Now verifies value column has content and typed_value is null for Variant::Null rows.

**C++17 compatibility fix:**
5. **Removed C++20 designated initializers**: `ObjectFieldShredder` was initialized with `.field = value` syntax which may fail on MSVC in C++17 mode. Replaced with a proper constructor (`ObjectFieldShredder(schema, num_rows)`).

**Documentation:**
6. **Int auto-sizing comment in reconstruction**: Added comment in `case Type::INT64` noting that `vb.Int()` auto-sizes (Shred(Int64(42))→Reconstruct() produces Int8(42)). Matches Rust behavior; value preserved, encoding width may narrow.

**Verification**: PENDING — needs Docker re-run after 9th pass changes.

### Tenth review pass (2026-06-11, principal engineer full Rust/C++ parity audit):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Rust parity (new shredding targets):**
1. **INT8/INT16 shredding support**: Added `ExtractInt8()`, `ExtractInt16()` helpers plus full shred/reconstruct cases for `Type::INT8` and `Type::INT16`. Rust supports `Int8`/`Int16` as shredding targets; C++ was missing these. `IsVariantCompatibleWithType` already handled the compatibility (Int8→INT8, Int8/Int16→INT16) so only the extraction + builder cases were needed.
2. **INT8/INT16 reconstruction**: Added `case Type::INT8` and `case Type::INT16` in the reconstruction switch, using `vb.Int8()` and `vb.Int16()` respectively (preserves encoding width, no auto-sizing).

**C++17 compatibility fix (test file):**
3. **Removed C++20 designated initializers from test `ObjectRow` usage**: `{.fields = {...}}` replaced with aggregate initialization `{{...}}`. MSVC in C++17 mode rejects designated initializers; the `ObjectFieldShredder` struct was already fixed in 9th pass but the test file `VariantShredObjectTest` still used them.

**Documentation / comments:**
4. **INT64 shred case comment**: Documented why no `IsVariantCompatibleWithType()` gatekeeper is needed (ExtractInt64 already only accepts int variants, all int→int64 widening is valid).
5. **Reconstruction "both null" ambiguity**: Added detailed comment in all three reconstruction paths (primitive, array, object) explaining that both-null could mean SQL NULL or variant-null, and that callers should check struct-level validity bitmap to disambiguate. References Rust's `NullBuffer` approach.
6. **Builder reuse comment**: Added note that `VariantBuilder vb` is reused across rows safely because primitives never add dictionary keys.
7. **Decimal128 Rust parity comment**: Noted that Rust supports Decimal32/Decimal64 as separate shredding targets via dedicated types; C++ consolidates into Decimal128.

**New tests (3 tests):**
8. `FloatWidenedToDoubleRoundTrip` — Float shredded into Double column, verifies value preserved but type tag changes to Double on reconstruction (header byte = 0x1C)
9. `Int8ShredTargetRoundTrip` — Int8 shredded into Int8 column; Int16 does NOT match (no narrowing)
10. `Int16ShredTargetRoundTrip` — Int8+Int16 shredded into Int16 column; Int32 does NOT match

**Verification**: PENDING — needs Docker re-run after 10th pass changes.

### Eleventh review pass (2026-06-11, final Rust parity — LARGE_STRING/LARGE_BINARY):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Rust parity (new shredding targets):**
1. **LARGE_STRING shredding support**: Added `case Type::LARGE_STRING` in the shredding switch using `LargeStringBuilder`. `IsVariantCompatibleWithType` already returned `true` for `kString/kShortString → LARGE_STRING`, so only the shred loop and reconstruction cases were needed. Rust supports `LargeUtf8` as a shredding target in `shred_variant.rs`.
2. **LARGE_BINARY shredding support**: Added `case Type::LARGE_BINARY` in the shredding switch using `LargeBinaryBuilder`. `IsVariantCompatibleWithType` already returned `true` for `kBinary → LARGE_BINARY`. Rust supports `LargeBinary` as a shredding target.
3. **LARGE_STRING reconstruction**: Added `case Type::LARGE_STRING` in the reconstruction switch, reading from `LargeStringArray` and encoding back via `vb.String()`.
4. **LARGE_BINARY reconstruction**: Added `case Type::LARGE_BINARY` in the reconstruction switch, reading from `LargeBinaryArray` and encoding back via `vb.Binary()`.

**New tests (2 tests):**
5. `LargeStringShredRoundTrip` — String shredded into LargeString column; Int does NOT match; verifies short-string header byte on reconstruction
6. `LargeBinaryShredRoundTrip` — Binary shredded into LargeBinary column; String does NOT match; verifies byte-identical content

**Verification**: PENDING — needs Docker re-run after 11th pass changes.

### Twelfth review pass (2026-06-11, principal engineer Rust parity — type compatibility fix + nits):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fix (type compatibility correctness):**
1. **`IsVariantCompatibleWithType` — removed `BINARY`/`LARGE_BINARY` from `kShortString` compatibility**: Short strings are semantically UTF-8 text (same as `kString`), not binary data. The previous code claimed `kShortString → BINARY` and `kShortString → LARGE_BINARY` were compatible, but `ExtractBinary()` only handles `kBinary` — so short strings would never actually extract into a binary column (they'd always fall to value anyway). This was a dead code path but semantically wrong and misleading. Rust makes the same distinction: strings go to `Utf8`/`LargeUtf8`, binary goes to `Binary`/`LargeBinary`. Now: `kShortString → STRING | LARGE_STRING` only.

**Documentation (Rust divergence + TODOs):**
2. **Added NOTE comment about Rust's casting-based shredding approach**: Rust's `shred_variant()` uses `arrow::compute::cast()` which allows cross-type conversions (e.g., `Int32→Float64`, `Float32→Int32`). C++ only shreds values whose variant type matches the target column type directly (with safe widening within the same numeric family). This is spec-compliant but less aggressive for predicate pushdown. Added a NOTE comment in the primitive shredding dispatch section.
3. **`ReadLE` — LE-only caveat comment**: Documented that this function assumes little-endian architecture (Arrow C++ targets x86_64/ARM LE exclusively). On big-endian the `FromLittleEndian()` byte-swap + mask approach would produce incorrect results.
4. **Added StringView/BinaryView shredding target TODOs**: Added TODO comments in both the shredding switch `default:` case and the reconstruction switch `default:` case noting Rust supports `Utf8View`/`BinaryView` as targets.
5. **`ShredVariantColumn` header doc enhanced**: Added note about strict type matching semantics and Rust's CastOptions divergence in the public API doc in `variant_shredding.h`.

**Code quality nits:**
6. **Test helper comment clarification**: Updated `GetBinaryView` test helper comment to note it's named to avoid confusion with `BinaryViewArray::GetView()`.

**Verification**: PENDING — needs Docker re-run after 12th pass changes.

### Thirteenth review pass (2026-06-11, principal engineer final Rust parity + correctness):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Correctness fix (typed_value field type mismatch):**
1. **Typed_value output field for TIMESTAMP/TIME64 now declares `int64()`**: Previously the output StructArray field was declared as `schema.type()` (e.g., `timestamp(MICRO, "UTC")`) but the actual array built by `Int64Builder` has type `int64()`. Arrow's `StructArray::Make` doesn't validate field-type/array-type compatibility, so this was a silent mismatch that could cause issues during downstream schema validation or casting. Now the field type matches the physical array type. The reconstruction path dispatches on `schema.type()->id()` (not the field metadata), so this change is transparent to round-trip correctness.

**Object reconstruction comment enhancement:**
2. **`SetAllowDuplicates(true)` rationale documented**: Replaced terse `// Safety for merge` with detailed explanation of why duplicates are allowed during reconstruction (defensive measure against shredder bugs; last-value-wins means residual would override typed which is semantically wrong; shredder guarantees non-overlap so duplicates should never occur in practice).

**Rust parity TODOs (new):**
3. **Added LargeList/FixedSizeList/ListView TODO in shredding switch**: Rust supports `GenericListArray<i32>`, `GenericListArray<i64>`, `GenericListViewArray`, and `FixedSizeListArray` as array shredding targets. C++ only supports `List`. Added TODO alongside existing StringView/BinaryView TODO.
4. **Added matching TODO in reconstruction switch**: Mirrors the shredding TODO for list-like types.

**Header doc enhancement:**
5. **`ShredVariantColumn` doc in `variant_shredding.h`**: Added explicit mention of Rust's additional supported types (StringView, BinaryView, FixedSizeList, LargeList, ListView) and CastOptions cross-type coercion capability.

**New tests (2 tests):**
6. `ReconstructBothNonNullPrimitiveSchema` — Verifies `ReconstructVariantColumn` returns `Status::Invalid` when both value and typed_value are non-null (invalid shredded state for primitive schemas).
7. `ShredUnsupportedTargetType` — Verifies `ShredVariantColumn` returns `Status::NotImplemented` when given an unsupported target type (e.g., `duration(MICRO)`).

**Build fix:**
8. **Added `#include "arrow/array/builder_primitive.h"` to test file**: Required for `Int64Builder` used in the new error test.

**Verification**: PENDING — needs Docker re-run after 13th pass changes.

### Fourteenth review pass (2026-06-12, principal engineer full Rust/C++ parity + robustness):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fixes (robustness / defense-in-depth):**
1. **Input type validation in `ShredVariantColumn`**: Added checks that `metadata_array` and `value_array` are `BINARY` or `LARGE_BINARY`. The internal `GetBinaryValue` helper silently returns empty for other array types (e.g., `BinaryViewArray`), which would cause subtle data corruption. Now returns `Status::Invalid` with a descriptive message. Also validates that metadata and value arrays have the same length.
2. **Array length consistency in `ReconstructVariantColumn`**: Added validation that `metadata_array`, `value_array`, and `typed_value_array` all have the same length. A mismatch would cause out-of-bounds array access.

**Code quality (DCHECKs):**
3. **`VariantExtensionType` constructor DCHECK for `value_`**: Added `DCHECK_NE(value_, nullptr)` alongside the existing `DCHECK_NE(metadata_, nullptr)`. `IsSupportedStorageType()` guarantees `value` field exists, but this catches programming errors where the constructor is called without prior validation.

**Documentation:**
4. **Narrowing-cast comments in `ExtractInt64`**: Added inline comments at the `kInt16` and `kInt32` cases explaining the subtle ReadLE→narrow→sign-extend pattern: "ReadLE zero-extends; narrowing to int16_t sign-extends back to int64_t."

**Rust parity assessment (no code changes needed):**
- Verified all supported Rust shredding targets are either implemented or have explicit TODOs
- Confirmed `Variant::Null` handling matches Rust semantics
- Confirmed decimal width preservation is equivalent to Rust's dedicated Decimal4/8/16 types
- Confirmed Float→Double widening matches Rust behavior
- Confirmed NullValue handling differences are documented (Rust uses NullBuffer; C++ uses both-null convention with struct validity bitmap)

**Verification**: PENDING — needs Docker re-run after 14th pass changes.

### Fifteenth review pass (2026-06-13, principal engineer — big-endian safety + defensive validation):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fixes (big-endian correctness — would fail on s390x CI):**
1. **`ReadLE` endian-safe rewrite**: Replaced `memcpy` + `FromLittleEndian` + mask pattern with byte-by-byte shift reconstruction. The old pattern was broken on big-endian: `memcpy` of fewer than 8 bytes into an `int64_t` puts data at low addresses, then `FromLittleEndian` (which byte-swaps the full 8-byte word on BE) moves those bytes to the high end, and the low-bit mask then keeps the wrong bits. The new implementation (`result |= buf[i] << (i*8)`) is architecture-independent, matching the approach in `variant_internal.cc`'s `ReadUnsignedLE`.
2. **Decimal128 reconstruction endian-safe rewrite**: Replaced `val.ToBytes(bytes)` + `memcpy` + `FromLittleEndian` pattern with `val.high_bits()` / `val.low_bits()` numeric accessors. The old pattern applied `FromLittleEndian` to native-endian `ToBytes()` output, which would double-swap on big-endian. The accessors return numeric values directly regardless of architecture. Decimal4/8/16 encoding now uses `bit_util::ToLittleEndian()` correctly on the numeric values before `memcpy` to byte arrays.

**Defensive validation (prevents UB on invalid inputs):**
3. **Type validation in `ReconstructVariantColumnArray`**: Added check that `typed_value_array` is `LIST` and its values are `BINARY`. Previously performed `static_cast<const ListArray*>` without validation — UB if wrong type passed.
4. **Type validation in `ReconstructVariantColumnObject`**: Added check that `typed_value_array` is `STRUCT`. Same pattern fix as above.
5. **Input validation in `ReconstructVariantColumn` entry point**: Added `BINARY`/`LARGE_BINARY` type checks on `metadata_array` and `value_array`, mirroring the validation already present in `ShredVariantColumn`. Without this, `GetBinaryValue` silently returns empty strings for non-binary arrays, causing data corruption.

**Completeness fix:**
6. **`default: return false` in `IsVariantCompatibleWithType` PrimitiveType switch**: Previously the switch had no default case. If a future spec version adds new primitive types (e.g., Float16), the function would have undefined behavior. Now safely returns `false` for unknown types.

**New tests (4 tests):**
7. `ShredInvalidMetadataArrayType` — Verifies `ShredVariantColumn` returns `Status::Invalid` when metadata_array is not BINARY/LARGE_BINARY.
8. `ReconstructInvalidValueArrayType` — Verifies `ReconstructVariantColumn` returns `Status::Invalid` when value_array is not BINARY/LARGE_BINARY.
9. `ReconstructArrayTypedValueNotList` — Verifies `ReconstructVariantColumnArray` returns `Status::Invalid` when typed_value is not a ListArray.
10. `ReconstructObjectTypedValueNotStruct` — Verifies `ReconstructVariantColumnObject` returns `Status::Invalid` when typed_value is not a StructArray.

**Non-issue confirmed (item #2 from review — builder reuse):**
- Verified that `BuildWithoutMeta()` moves the buffer out and clears it (`buffer_.clear()`), so `VariantBuilder vb` reuse across rows in reconstruction is safe. No fix needed.

**Verification**: PENDING — needs Docker re-run after 15th pass changes.

---

## Key Files in This Notes Repo

| File | Purpose |
|------|---------|
| `AGENT.md` | This file — agent context for continuing work |
| `arrow_issue_45946_solution.md` | Detailed decoder design doc |
| `arrow_issue_45947_solution.md` | Encoder design doc |
| `arrow_issue_45946_45947_solution.md` | Combined plan with Go parity analysis |
| `arrow_issue_45946_45947_45948_solution.md` | Full roadmap including shredding |
| `arrow_issue_45948_solution_proposal.md` | **Shredding design doc** — formal spec, architecture, Rust audit |
| `arrow_issue_45948_solution.md` | **Shredding code review** — findings, fixes applied, test additions |
| `arrow_go_bug.md` | Go `valueSize()` bug analysis + reproducer |
| `VariantEncoding.md` | Variant binary encoding spec (from parquet-format) |
| `VariantShredding.md` | Variant shredding spec (for future GH-45948) |
| `decoding_pr_plan.md` | Original PR plan |
| `development_strategy.md` | Overall strategy doc |

### Sixteenth review pass (2026-06-13, principal engineer — template refactor + Rust parity documentation):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Code quality / deduplication:**
1. **Template refactor for primitive shred loops**: Introduced `ShredPrimitiveLoop<BuilderT, NativeT, ExtractFn>()` and `ShredBinaryLoop<BuilderT>()` template helpers that eliminate ~360 lines of copy-paste across 12+ type-specific switch cases. Each case now fits in 5-7 lines instead of 25-30. Total file reduction: 1934 → 1720 lines (-214 lines).
2. **`GetBinaryValue` — added `DCHECK(false)` on unsupported type path**: Callers validate at public entry points, but the fallthrough `return {}` was a silent corruption vector if ever reached from internal code. Now crashes with a diagnostic message in debug builds.
3. **`VariantShreddingSchema::kind_` — default-initialized to `Kind::kPrimitive`**: Prevents undefined behavior if a default-constructed schema is accidentally used (factory methods always set it, but defensive initialization is cheap).
4. **`variant_shredding.h` — added `#include <cstdint>`**: The header uses `uint8_t`/`int64_t` directly. While pulled in transitively, explicit inclusion is the Arrow convention.

**Documentation / Rust parity gaps:**
5. **Expanded TODO in shred switch**: Added comprehensive list of Rust-supported types that require a cast-based mode: Uint8/16/32/64, Float16, Decimal32/64, TimestampSecond/Millisecond. Explains why these can't be added without a `CastOptions` infrastructure (variant spec only encodes signed ints, float32/64, micros/nanos).
6. **Expanded TODO in reconstruction switch**: Matching cast-mode TODO referencing the shred switch.
7. **`parquet_variant.cc` — comment on value-absent schema rejection**: `IsSupportedStorageType()` now documents WHY it rejects `{metadata, typed_value}` without `value` (the shredding spec allows this for fully-shredded leaf columns, but our implementation always produces a value column).

**Verification**: PENDING — needs Docker re-run after 16th pass changes.

### Seventeenth review pass (2026-06-13, principal engineer — sign-extension clarity + Rust parity: object native extraction):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**MAJOR Rust parity feature implemented: Object sub-field native extraction:**
1. **Recursive primitive extraction for object fields**: For each object field with a `Primitive` sub-schema, the output construction now calls `ShredVariantColumn` on the per-field BinaryArray, producing proper `{value, typed_value}` sub-structs where compatible values are extracted into native typed columns (Int64Array, StringArray, etc.). This matches Rust's `VariantToShreddedObjectVariantRowBuilder` behavior and enables Parquet statistics-based predicate pushdown on object sub-fields.
2. **Updated `ReconstructVariantColumnObject`**: Pre-computes per-field reconstructed variant arrays (column-level, O(n) per field) for Primitive sub-schemas. The per-row loop checks if the field was present (value or typed_value non-null in the sub-field struct) and uses the pre-computed reconstruction to produce variant bytes.
3. **Handles all edge cases correctly**:
   - Field present with compatible value → typed_value populated, value null
   - Field present with incompatible value → value populated, typed_value null
   - Field present with Variant::Null → value = 0x00 byte, typed_value null (Rust parity)
   - Field absent → both value and typed_value null → field skipped in reconstruction
4. **Non-primitive sub-schemas unchanged**: Object/Array sub-schemas still store field values as variant binary (recursive nested shredding is a separate follow-up).

**Code clarity (reviewer-friendliness):**
5. **Explicit sign-extension in `ExtractInt64/Int32/Int16`**: Double-casts make the sign-extension path explicit.
6. **Removed Go-reference comment from `Finish()` in `variant_builder.cc`**: Style compliance.

**New tests (1 test):**
7. `MissingFieldNativeExtraction` — verifies native extraction with missing fields across 3 rows with different field combinations. Checks typed_value arrays contain expected native values and reconstruction produces correct objects.

**Enhanced tests (1 test updated):**
8. `FullyShredded` — now verifies that typed_value sub-columns contain native StringArray/Int64Array values (previously only checked residual was null and reconstruction worked).

**Verification**: PENDING — needs Docker re-run after 17th pass changes.

### Eighteenth review pass (2026-06-14, principal engineer — hardening + bounds validation):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Correctness fix (prevents undefined behavior on mismatched schemas):**
1. **`ReconstructVariantColumnObject` — field count bounds validation**: Added check that `typed_struct->num_fields()` matches `schema_fields.size()` before accessing fields by index. Without this validation, a schema/data mismatch (e.g., from corrupted file metadata or programming error in a caller) would access `typed_struct->field(n)` out-of-bounds, causing UB. Now returns `Status::Invalid` with a descriptive message. This closes a gap identified during the principal engineer review: the original code assumed schema and struct always match, but no external guarantee enforces this.

**Robustness fix (defensive `UnsafeAppendEncoded`):**
2. **`UnsafeAppendEncoded` — graceful no-op for size≤0**: Added early return `if (size <= 0) return;` that prevents buffer corruption in release builds if a caller passes zero-length data (which shouldn't happen per invariant, but could during reconstruction of malformed inputs). The existing `DCHECK_GT(size, 0)` is retained for debug-build detection of invariant violations. Also added expanded comment documenting the contract and the DCHECK/release-guard duality.

**C++ standards compliance:**
3. **`variant_shredding.cc` — added `#include <cstdint>`**: The file uses `int64_t`, `uint8_t`, `int32_t`, `int8_t`, `int16_t` extensively. While these were transitively available through Arrow headers, explicit inclusion is required per C++ standard and matches the convention established in the header file (`variant_shredding.h`) which already includes `<cstdint>`.

**Code clarity:**
4. **`BuildWithoutMeta` — post-move `clear()` comment**: Added explanatory comment noting that the explicit `clear()` after `std::move` ensures deterministic empty state for reuse (move leaves the source in "valid but unspecified" state per C++ standard).

**New test (1 test):**
5. `ReconstructObjectFieldCountMismatch` — Verifies that `ReconstructVariantColumn` returns `Status::Invalid` when the typed_value StructArray has fewer fields than the shredding schema expects. Constructs a 1-field struct but passes a 2-field schema, confirming the new bounds check rejects the mismatch.

**Verification**: PENDING — needs Docker re-run after 18th pass changes.

### Nineteenth review pass (2026-06-14, principal engineer — C++ standards compliance + documentation):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**C++ standards compliance (previously raised, now addressed):**
1. **`variant_shredding.cc` — added `#include <string_view>`**: The file uses `std::string_view` in multiple places:
   - `GetBinaryValue()` return type and body
   - `ExtractString()` output parameter
   - `ShredPrimitiveLoop` template instantiations for STRING/LARGE_STRING
   - Object reconstruction `GetBinaryValue()` calls
   
   Previously relied on transitive inclusion via `arrow/extension/variant_internal.h` (which includes `<string_view>` for its own API). Per C++ standard [headers.synopsis], every translation unit should directly include the headers for types it references. More critically, under **Unity builds** (used by Arrow CI), include ordering is not guaranteed — a TU combining `variant_shredding.cc` with a file that doesn't transitively include `<string_view>` could fail. This was previously raised as a potential issue and is now resolved.

   **Historical note**: This was identified in the principal engineer review as item #2 ("Missing include: `<string_view>`"). It was not addressed in the 18th pass because that pass focused on correctness/UB fixes rather than compliance nits. Now addressed for completeness.

**Documentation / code clarity:**
2. **`ToArrowType()` header doc — logical vs physical type distinction**: Added NOTE to the `variant_shredding.h` header doc explaining that `ToArrowType()` returns the *logical* type (e.g., `timestamp(MICRO, "UTC")`), but the actual shredded output for TIMESTAMP and TIME64 schemas uses `int64()` as the physical field type. This prevents confusion when callers compare `ToArrowType()` output against actual shredded StructArray field types. The reconstruction path uses `schema.type()->id()` for dispatch, so the logical type information is preserved through the schema object, not the field metadata.

3. **`ReconstructVariantColumnObject` — per-row builder optimization TODO**: Added a NOTE comment + TODO documenting that creating a fresh `VariantBuilder(meta)` per row is correct (each row decodes its own metadata, and the builder accumulates dictionary keys via `NextField()`) but has O(n × k) cost where k = metadata dictionary size. The builder constructor copies the entire dictionary. For high-row-count columns with large dictionaries, this could be optimized via builder pooling (reset buffer but keep dictionary), or by hoisting builder creation outside the loop for the common case where all rows share the same metadata dictionary. This is a non-blocking performance observation — the current code is functionally correct.

**No new tests** — this pass contains only include/documentation changes that don't affect behavior.

**Files modified:**
- `cpp/src/arrow/extension/variant_shredding.cc` — added `#include <string_view>`, added per-row builder optimization TODO comment
- `cpp/src/arrow/extension/variant_shredding.h` — added `ToArrowType()` doc NOTE about logical vs physical types

**Verification**: PENDING — needs Docker re-run after 19th pass changes. Expected: no behavioral change, should compile cleanly since `<string_view>` was already transitively available.

### Twentieth review pass (2026-06-14, principal engineer final review — test correctness + defensive hardening):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fix (test name contradicts assertion — would confuse reviewers):**
1. **Renamed `FloatNotCompatibleWithFloat64` → `FloatCompatibleWithFloat64ViaWidening`**: The test asserted `ASSERT_TRUE` (Float IS compatible with Double via widening) but was named "NotCompatible". This directly contradicts the assertion and would cause reviewer confusion. The comment already explained the widening semantics but the name was misleading.

**New test (coverage gap):**
2. **Added `FloatCompatibleWithFloat32` test**: Explicit coverage for `kFloat → FLOAT` direct compatibility (same type, no widening). Previously only Float→Double was explicitly tested. This ensures the `|| target_type.id() == Type::FLOAT` branch is directly exercised.

**Defensive hardening:**
3. **Added `DCHECK_NE(list_arr->values(), nullptr)` in `ReconstructVariantColumnArray`**: After the type validation (`type_id() == LIST`, `value_type() == BINARY`), added a DCHECK on the values pointer before the `static_cast<const BinaryArray*>`. Consistent with the defensive spirit of the rest of the code (DCHECK on metadata_ and value_ in `VariantExtensionType`). Catches programming errors if `ListArray::values()` ever returns null due to corruption.

**Documentation / code clarity:**
4. **`PERF TODO` prefix on `ObjectFieldShredder::AppendObject` field-lookup TODO**: Changed from plain `TODO GH-45948 follow-up` to `PERF TODO GH-45948 follow-up` and added "This has real performance implications for wide objects (e.g., 50+ field schemas against 100-field objects)." Clarifies this is not just a code-cleanup follow-up but has production impact.
5. **NullArray usage comment in `ShredVariantColumnObject`**: Added 6-line comment explaining why using `NullArray` (type null()) for the typed_value sub-column of non-Primitive sub-schemas is semantically acceptable — the field is always null so no consumer inspects the typed data; the declared field type serves only as schema documentation.
6. **`ShredPrimitiveLoop` — `native_val` initialization comment**: Added `// Default-initialized; only read when extract() returns true.` to clarify that the zero-initialized value is never consumed on extraction failure (the template pattern routes to the residual on false return).
7. **Test helper `.ok()`/`.ValueOrDie()` convention comment**: Added documentation comment to `VariantShredRoundTripTest::BuildVariantColumn` explaining this is the established Arrow test convention for non-void helper functions (mirrors the explicit comment in `variant_builder_test.cc` line 35-38).

**Files modified:**
- `cpp/src/arrow/extension/variant_shredding.cc` — PERF TODO prefix, NullArray comment, DCHECK addition, native_val comment
- `cpp/src/arrow/extension/variant_shredding_test.cc` — test rename + new test + helper convention comment

**Verification**: PENDING — needs Docker re-run after 20th pass changes. Expected test count: ~308 (previous ~306 + 1 new test `FloatCompatibleWithFloat32` + test rename is net-zero on count).

### Twenty-first review pass (2026-06-14, principal engineer — performance refactoring):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Performance fix #1 (object shredding — eliminates O(s×k) inner loop):**
1. **`ObjectFieldShredder::AppendObject` — single-pass `unordered_map` field lookup**: Replaced the O(s×k) pattern where each schema field's `FindObjectField` result required a second inner loop (`GetObjectFieldAt` over all k fields) to find the positional index for `is_shredded[]` marking. New approach builds an `unordered_map<string_view, FieldInfo>` (name → {index, offset, size}) in a single O(k) pass over object fields, then looks up each schema field in O(1). Total per-row complexity: O(s + k) vs previous O(s × k). For a 10-field schema against 100-field objects across 1M rows, this eliminates ~1B redundant `GetObjectFieldAt` calls.

**Performance fix #2 (object reconstruction — eliminates O(n×k) dictionary copies):**
2. **`ReconstructVariantColumnObject` — cached `VariantBuilder` reuse across rows**: Previously created `VariantBuilder(meta)` per row, copying the entire metadata dictionary (hash map insertions + string allocations) each time. New approach caches the decoded `VariantMetadata` and a `unique_ptr<VariantBuilder>` — only reconstructed when metadata bytes actually change (rare in columnar data where all rows in a chunk share the same metadata). Between rows, `BuildWithoutMeta()` clears the builder's buffer but preserves its dictionary (`dict_`, `dict_keys_`), so subsequent `NextField()` calls resolve keys via the existing hash map without re-insertion. For the common case (uniform metadata), reduces builder construction from O(n × k) to O(1) amortized. Mixed-metadata columns degrade gracefully to per-change reconstruction.

**C++ standards compliance:**
3. **Added `#include <unordered_map>` to `variant_shredding.cc`**: Required for the new `std::unordered_map<std::string_view, FieldInfo>` in `AppendObject`. Previously not needed; now used explicitly.

**No new tests** — both changes are semantically equivalent to the previous behavior (identical observable outputs). Existing round-trip tests validate correctness. The performance improvements affect constant factors and algorithmic complexity, not behavior.

**Files modified:**
- `cpp/src/arrow/extension/variant_shredding.cc` — `AppendObject` refactored (O(s+k)), `ReconstructVariantColumnObject` builder caching, `#include <unordered_map>` added

**Verification**: PENDING — needs Docker re-run after 21st pass changes. Expected test count: unchanged (~308).

### Twenty-second review pass (2026-06-15, Rust parity — StringView/BinaryView + LargeList):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**Rust parity feature #1: StringView/BinaryView as full shredding targets:**
1. **`GetBinaryValue` helper — BinaryView/StringView support**: Extended to handle `BINARY_VIEW` and `STRING_VIEW` array types via `BinaryViewArray::GetView()` (returns `std::string_view`, same interface as BinaryArray). Removes the previous DCHECK-and-return-empty fallback.
2. **`IsVariantCompatibleWithType` — STRING_VIEW/BINARY_VIEW compatibility**: `kShortString` and `kString` now match `STRING_VIEW` target; `kBinary` now matches `BINARY_VIEW` target. Matches Rust's behavior where `Utf8View`/`BinaryView` are valid shredding targets.
3. **Shredding switch — STRING_VIEW case**: Uses `ShredPrimitiveLoop<StringViewBuilder, std::string_view>` with `ExtractString` — identical pattern to the STRING/LARGE_STRING cases.
4. **Shredding switch — BINARY_VIEW case**: Uses `ShredBinaryLoop<BinaryViewBuilder>` — identical pattern to the BINARY/LARGE_BINARY cases.
5. **Reconstruction switch — STRING_VIEW case**: Reads from `StringViewArray::GetView(i)` (returns `std::string_view`), encodes via `vb.String()`.
6. **Reconstruction switch — BINARY_VIEW case**: Reads from `BinaryViewArray::GetView(i)` (returns `std::string_view`), encodes via `vb.Binary()`.
7. **Input type validation broadened**: Both `ShredVariantColumn` and `ReconstructVariantColumn` now accept `BINARY_VIEW` as valid metadata/value input array type (in addition to BINARY and LARGE_BINARY).

**Rust parity feature #2: LargeList reconstruction support:**
8. **`ReconstructVariantColumnArray` — accepts LIST and LARGE_LIST**: Refactored from a hardcoded `ListArray*` path to a generic lambda (`[&](auto* list_arr) -> Status`) that handles both `ListArray` (32-bit offsets) and `LargeListArray` (64-bit offsets). Uses `auto` for offset variables (`value_offset()` returns `int32_t` for List, `int64_t` for LargeList). Dispatches based on `typed_value_array->type_id()`.
9. **Shredding still produces ListArray**: Array shredding output remains `ListArray` with 32-bit offsets (sufficient for per-row element counts). LargeList support is one-sided (reconstruction only) for reading Parquet files that may use 64-bit offsets.

**Documentation updates:**
10. **Header doc** (`variant_shredding.h`): Updated Rust parity gaps list — removed StringView/BinaryView, noted LargeList in reconstruction, kept FixedSizeList/ListView as remaining.
11. **Inline TODOs updated**: Removed resolved StringView/BinaryView TODOs in shredding and reconstruction switches. Updated LargeList TODO to note shredding-side not needed (reconstruction sufficient). Remaining TODOs: FixedSizeList, ListView, cast-based mode.
12. **Test comments updated**: Error test comments clarified to reflect BINARY_VIEW acceptance.

**New tests (5 tests):**
13. `StringViewShredRoundTrip` — String variants shredded into StringView column; verifies typed array is STRING_VIEW; round-trip reconstruction produces correct short-string header byte (0x15 for "hello").
14. `BinaryViewShredRoundTrip` — Binary variants shredded into BinaryView column; verifies typed array is BINARY_VIEW; byte-identical content after reconstruction.
15. `ShortStringToStringView` — Short strings (≤63 bytes, BasicType::kShortString) are compatible with StringView target and extract correctly.
16. `LargeListReconstructRoundTrip` — Shreds array → ListArray, copies elements into LargeListArray via LargeListBuilder, reconstructs from LargeListArray, verifies byte-identical output vs List reconstruction.
17. `ReconstructArrayTypedValueLargeListAccepted` — Empty LargeList of binary passes validation in `ReconstructVariantColumn` for array schemas.

**Build system:**
- Test file: added `#include "arrow/array/builder_nested.h"` for `LargeListBuilder`.
- No other build system changes needed (all array/builder headers already included).

**Files modified:**
- `cpp/src/arrow/extension/variant_shredding.cc` — `GetBinaryValue` + 2 shred cases + 2 reconstruct cases + input validation + generic lambda in array reconstruction
- `cpp/src/arrow/extension/variant_shredding.h` — updated parity gap documentation
- `cpp/src/arrow/extension/variant_shredding_test.cc` — 5 new tests + `builder_nested.h` include + comment updates

**Verification**: PENDING — needs Docker re-run after 22nd pass changes. Expected test count: ~311 (was 286 + 5 new + ~20 from passes 17-21 not yet verified).

### Twenty-third review pass (2026-06-16, principal engineer parity audit — Time64 unit + cleanup):

**Shredding branch** (`variant-shredding-impl`, uncommitted on top of c5971e293c):

**BLOCKING fix (type compatibility correctness — latent data misinterpretation):**
1. **`IsVariantCompatibleWithType` — strict Time64 unit validation**: `kTimeNTZ` now validates that the target `Time64Type` has `TimeUnit::MICRO`. Previously accepted any `time64()` target. The variant spec's `kTimeNTZ` stores microseconds since midnight — shredding into a `time64(NANO)` target would cause downstream consumers reading the typed_value column directly (for predicate pushdown) to misinterpret values as nanoseconds. The reconstruction path (`vb.TimeNTZ()`) always writes microseconds regardless of the typed column's declared unit, so round-trip via Reconstruct(Shred()) was safe — but direct typed_value access was not. Rust avoids this implicitly by using `Time64MicrosecondType` as the specific target type.

**Code quality fix (duplicate comment removal):**
2. **Removed duplicate TODO block in shredding switch default case**: Lines 1327-1331 duplicated the Decimal32/64 + TimestampSecond/Milli + CastOptions comment verbatim. This was a copy-paste artifact from multiple review passes stacking edits (16th pass expanded the TODO, likely introduced the duplication during a partial re-application).

**Documentation / PERF comments:**
3. **Added PERF comment on residual loop's redundant `GetObjectFieldAt` call**: In `ObjectFieldShredder::AppendObject`, the residual-building loop calls `GetObjectFieldAt()` again for every non-shredded field even though `object_field_map` already stores offset+size. Documents that this is O(1) per field (header arithmetic only) and acceptable for first implementation, with a suggestion to iterate `object_field_map` entries instead.
4. **Added BinaryViewBuilder int32→int64 widening comment**: In the `Type::BINARY_VIEW` shred case, documents that `ShredBinaryLoop` calls `typed_builder.Append(bin_data, bin_size)` where `bin_size` is `int32_t` but `BinaryViewBuilder::Append` accepts `int64_t`. The implicit widening is safe.

**New tests (3 tests):**
5. `Time64MicroCompatibleWithTime64Micro` — Verifies `kTimeNTZ` is compatible with `time64(MICRO)` target (positive case).
6. `Time64NanoNotCompatibleWithTime64Micro` — Verifies `kTimeNTZ` is NOT compatible with `time64(NANO)` target (would cause value misinterpretation).
7. `ZeroRowInput` — Tests all three schema kinds (Primitive, Object, Array) with zero-row empty arrays. Verifies shred produces correct output structure (3 fields, length=0) and round-trip reconstruction succeeds on empty inputs. Catches any off-by-one errors in Reserve/Finish patterns that only manifest with empty arrays.

**Files modified:**
- `cpp/src/arrow/extension/variant_shredding.cc` — Time64 unit check, duplicate TODO removed, 2 PERF/doc comments
- `cpp/src/arrow/extension/variant_shredding_test.cc` — 3 new tests

**Verification**: PENDING — needs Docker re-run after 23rd pass changes. Expected: ~314 total tests.
