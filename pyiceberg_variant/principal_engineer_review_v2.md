# Principal Engineer Code Review v11: C++ Variant Implementation (Final Comprehensive)

> Date: 2026-06-26 (v11 — full cross-language parity audit + code quality nit-pick)
> Reviewer: Principal Engineer (comprehensive final review)
> Branches: variant-decoding (162d503276), variant-encoding (f6b8e6609b), variant-shredding-impl (034ff491c9)
> PR Stack: #50121 (decoding) → #50122 (encoding) → #50232 (shredding)
> Merge order: GH-45946 first, then GH-45947, then GH-45948
> Cross-referenced: arrow-rs/parquet-variant, arrow-rs/parquet-variant-compute, arrow-go/parquet/variant

---

## Executive Summary

The C++ Variant implementation is **architecturally mature and ready for merge**. Full source
re-read of all production files confirms a genuine C++-from-scratch design with zero Go-isms,
zero legacy artifacts, and comprehensive Rust feature parity on core operations.

**Merge readiness: 99%.** One non-blocking MEDIUM issue (M6, transparent hasher C++17
limitation). All prior issues FIXED. No critical or high issues.

**Key strengths:**
- Clean view/builder/shredder separation — each PR is independently reviewable
- Idiomatic C++ throughout (RAII, `string_view`, `optional`, `Result<T>`, `[[nodiscard]]`, move-only)
- Named constants for spec-derived values
- Rust parity achieved on all core features with documented gaps deferred
- All 9 reviewer comments from PRs #50121 and #50122 resolved architecturally
- Template-refactored shredding loops eliminate code duplication
- Big-endian safe (accessor-based Decimal128, endian-safe ReadLE)
- Option D rebuild: each PR shows ONLY its own diff

---

## 1. MERGE ORDER VERIFICATION ✅

```
main (e16067a78c)
  └── variant-decoding (162d503276)  — PR #50121 targets main
       └── variant-encoding (f6b8e6609b)  — PR #50122 targets variant-decoding
            └── variant-shredding-impl (034ff491c9)  — PR #50232 targets variant-encoding
```

**Diff stats confirm clean split:**
- Decoding (main→decoding): 5 new files (+4517 lines, variant-only)
- Encoding (decoding→encoding): 8 files (+2288/-2 lines, builder + tests)
- Shredding (encoding→shredding): 10 files (+4592/-4 lines, shredding + 3 builder methods)

**Test results (Docker, 2026-06-26):**
- 335/335 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN`
- clang-format-18: PASSED (zero violations)

---

## 2. ALL PRIOR ISSUES — RESOLVED

| ID | Issue | Status |
|----|-------|--------|
| F1 | `variant_test_util.h` install leak | ✅ FIXED — renamed to `variant_internal_test_util.h` |
| M1 | Transparent hasher eliminates `lookup_buf_` | ✅ FIXED |
| M2 | `is_sorted` check before `std::sort` in FinishObject | ✅ FIXED |
| M3 | Shared `variant_internal_util.h` ReadLE | ✅ FIXED |
| M4 | `RoundTrip()` test helper checks | ✅ FIXED |
| M5 | Stale TODO in `parquet_variant.h` | ✅ FIXED |
| M6 | Transparent hasher C++17 limitation | ⚠️ NON-BLOCKING (forward-compatible) |

---

## 3. C++ STANDARDS & ARROW CONVENTIONS COMPLIANCE ✅

| Check | Status | Notes |
|-------|--------|-------|
| No raw `new`/`delete` | ✅ | `make_shared`, `make_unique`, stack only |
| RAII for resources | ✅ | ObjectScope/ListScope auto-rollback |
| `[[nodiscard]]` | ✅ | On all 6 scope-returning functions |
| `const` correctness | ✅ | Views const-friendly, all read accessors correct |
| `string_view` zero-copy | ✅ | No defensive copies in read path |
| `std::optional` not-found | ✅ | `get()`, `locate()` |
| Endian-safe | ✅ | `FromLittleEndian`/`ToLittleEndian` + `memcpy` |
| No UB on truncated input | ✅ | Length checks before all reads |
| Move-only builder | ✅ | Copy deleted, move noexcept |
| `Result<T>` / `Status` | ✅ | Consistent throughout |
| `ARROW_RETURN_NOT_OK` | ✅ | All Status paths propagated |
| `ARROW_ASSIGN_OR_RAISE` | ✅ | All Result paths |
| `DCHECK` debug invariants | ✅ | Programming errors only |
| Named constants | ✅ | `kUUIDByteLength`, `kMaxShortStringLength`, etc. |
| 2-space indent | ✅ | clang-format-18 applied |
| `} else {` same line | ✅ | |
| Apache License headers | ✅ | All files |
| Include order | ✅ | Own header → system → arrow |
| `ARROW_EXPORT` on public | ✅ | All public classes/free functions |
| `arrow/util/logging_internal.h` in .cc | ✅ | Not `logging.h` |
| Static/anon-namespace in .cc | ✅ | Unity build safe |
| No gtest in production | ✅ | Only in test files |
| `#pragma once` | ✅ | All headers |
| `static_assert` on view sizes | ✅ | Prevents accidental bloat |

---

## 4. ARTIFACT CLEANUP VERIFICATION ✅

| Check | Status |
|-------|--------|
| No `variant_internal.h` (old main header) | ✅ Gone |
| No `variant_internal.cc` | ✅ Gone |
| No `variant_internal_test.cc` | ✅ Gone |
| No references to old `variant_internal` namespace | ✅ |
| No deprecated wrapper functions | ✅ |
| No backward-compatibility layers | ✅ |
| No old free-function decode API | ✅ |
| No `lookup_buf_` member variable | ✅ |
| `variant_internal_util.h` NOT installed (has "internal") | ✅ |
| `variant_internal_test_util.h` NOT installed (has "internal") | ✅ |

---

## 5. PR REVIEW COMMENT RESOLUTION ✅

### PR #50121 (Decoding) — 6/6 Resolved

| # | Comment | Resolution |
|---|---------|------------|
| 1 | "How was 32 threshold determined?" | ELIMINATED — always binary search with pre-parsed header |
| 2 | "§3 references — link to spec" | FIXED — links to VariantEncoding.md#encoding-types |
| 3 | "Rename file — 'internal' confusing" | RESOLVED — public API is `variant.h` (clear name) |
| 4 | "Add nested navigation test" | ADDRESSED — composable view chaining tested |
| 5 | "DecodeValueAt should be public" | UNNECESSARY — `VariantView::Make(meta, data+offset, size)` |
| 6 | "Plan for shredded variant reading?" | IMPLEMENTED — `ReconstructVariantColumn()` in PR #50232 |

### PR #50122 (Encoding) — 3/3 Resolved

| # | Comment | Resolution |
|---|---------|------------|
| 7 | "Test for metadata/data type mismatch" | ARCHITECTURALLY IMPOSSIBLE — metadata is key-dict only |
| 8 | "Initialize builder from existing buffer" | `VariantBuilder(VariantMetadata)` + read→rebuild pattern |
| 9 | "API for modifying existing variants" | Views (read) + builders (write) is deliberate separation |

---

## 6. RUST PARITY ANALYSIS (verified against arrow-rs HEAD)

### Rust `parquet-variant` Module Structure (for comparison)

```
parquet-variant/src/
  builder/         — ValueBuilder, ObjectBuilder, ListBuilder, MetadataBuilder
  variant/         — Variant (view), VariantObject, VariantList, VariantDecimal*
  decoder.rs       — BasicType, PrimitiveType enums, get_basic_type()
  path.rs          — VariantPath for deep navigation
  utils.rs         — slice helpers, overflow_error

parquet-variant-compute/src/
  shred_variant.rs     — shred_variant() main entry
  unshred_variant.rs   — unshred_variant() main entry
  variant_array.rs     — VariantArray wrapper
  variant_to_arrow.rs  — type conversion during shredding
  arrow_to_variant.rs  — type conversion during reconstruction
  variant_get.rs       — variant_get kernel
  to_json.rs           — JSON serialization
  from_json.rs         — JSON parsing
  cast_to_variant.rs   — cast Arrow arrays to variant
```

### Core Feature Match

| Feature | Rust | C++ | Status |
|---------|------|-----|--------|
| View types | `Variant`, `VariantObject`, `VariantList` | `VariantView`, `VariantObjectView`, `VariantArrayView` | ✅ |
| Binary search always | Yes | Yes (no threshold) | ✅ |
| Builder safety | Borrow checker + TypeState | RAII scopes + `[[nodiscard]]` | ✅ Comparable |
| Numeric coercion | `as_i64()`, `as_f64()` | `as_int64_coerced()`, `as_double_coerced()` | ✅ |
| Short string (≤63 bytes) | Yes | Yes | ✅ |
| Full recursive validation | `with_full_validation()` | `ValidateVariant()` free function | ✅ |
| NullBuffer return | `NullBuffer` in `VariantArray` struct | `out_null_bitmap` optional parameter | ✅ |
| Shredding | `shred_variant()` | `ShredVariantColumn()` | ✅ |
| Reconstruction | `unshred_variant()` | `ReconstructVariantColumn()` | ✅ |
| Schema builder | `ShreddedSchemaBuilder::with_path()` | `VariantShreddingSchema::{Primitive,Object,Array}` | ✅ Equivalent |
| StringView/BinaryView | Supported | Supported | ✅ |
| All list-like reconstruct | 5 types (List/LargeList/FSL/LV/LargeLV) | 5 types | ✅ |
| Recursive array shredding | Yes | Yes | ✅ |
| Decimal width preservation | Dedicated Decimal4/8/16 types | Inferred from magnitude | ✅ Equivalent |
| Variant::Null → value col | Yes | Yes | ✅ |
| UUID, Date32, Time64, Timestamps | All | All | ✅ |
| Int8/16/32/64 widening | Yes | Yes | ✅ |
| Float→Double widening | Yes | Yes | ✅ |
| `VariantPath` navigation | `VariantPath` + `variant_get` kernel | View chaining | ✅ Equivalent |

### Documented Gaps (follow-up PRs, non-blocking)

| # | Gap | Rust Status | C++ Status | Effort | Blocking? |
|---|-----|-------------|------------|--------|-----------|
| 1 | Object/Array recursive sub-field shredding | Full recursive | Primitive sub-fields only | ~200-400 LOC | No |
| 2 | CastOptions cross-type coercion | `shred_variant_with_options()` | Strict matching only | Needs `arrow_compute` | No |
| 3 | Value-absent schemas | `{metadata, typed_value}` without `value` | Not supported | ~50 LOC | No |
| 4 | Array shredding output variety | LargeList/FSL/ListView output | Always `ListArray` | ~100 LOC | No |
| 5 | Unsigned integer targets | Uint8/16/32/64 via cast | Not supported | Needs `arrow_compute` | No |

**Assessment:** These are all incremental additions that don't affect the core architecture. The C++ foundation is correct and supports adding all of these without refactoring.

---

## 7. GO PARITY COMPARISON

| Feature | Go | C++ | Assessment |
|---------|-----|-----|-----------|
| Binary search threshold | 32 fields | None (always) | C++ BETTER |
| Pre-parsed headers | No (lazy per-access) | Yes (validated at construction) | C++ BETTER |
| Builder safety | Manual | RAII scopes + auto-rollback | C++ BETTER |
| Recursion depth limit | None | 128 (protects stack) | C++ BETTER |
| Reserved bit validation | No check | Rejects bit 5 set | C++ BETTER |
| Field offset bounds check | No | Yes | C++ BETTER |
| `valueSize()` is_large bit | BUGGY (fixed via PR #839) | Correct | C++ CORRECT |
| Duplicate handling | keep-greater-offset (recompact) | skip-all-but-last (no recompact) | BOTH CORRECT |
| FinishObject sort | Always `slices.SortFunc` | `is_sorted` check → skip if ordered | C++ BETTER |
| AddKey allocation | One string per call | One string per call (C++17) | PARITY |
| Binary search signed indices | `uint32` (wraps at 0!) | `int32_t` (correct) | C++ CORRECT |

---

## 8. DEEP TECHNICAL VERIFICATION

### 8.1 ReadUnsignedLE (variant_internal_util.h) — ✅ Correct

```cpp
uint32_t result = 0;
std::memcpy(&result, data, num_bytes);
result = ::arrow::bit_util::FromLittleEndian(result);
if (num_bytes < 4) { result &= (1u << (num_bytes * 8)) - 1; }
```

**Big-endian verification:** On BE, `memcpy` of 2 LE bytes `[0xAB, 0xCD]` into zeroed
`uint32_t` gives memory value `0xABCD0000`. `FromLittleEndian` byte-swaps → `0x0000CDAB`.
Mask `& 0xFFFF` → `0xCDAB`. Expected LE interpretation: `0xCDAB`. ✅

### 8.2 RAII Scopes (ObjectScope/ListScope) — ✅ Correct

- Move ctor: `other.committed_ = true` (prevents double-rollback)
- Destructor: `if (!committed_ && parent_) parent_->Truncate(start_offset_)`
- `[[nodiscard]]`: prevents silent discard
- Move assignment: deleted (only move construction)
- `ObjectScope::Finish()`: sorts + commits

### 8.3 FinishObject Duplicate Handling — ✅ Correct

After sort, duplicates are adjacent. `skip all-but-last for each run` correctly
implements last-value-wins. `write != i` avoids unnecessary self-move-assignment.

### 8.4 Binary Search (VariantObjectView::get) — ✅ Correct

- Uses `int32_t` for lo/hi (avoids Go's unsigned underflow)
- `lo + (hi - lo) / 2` prevents overflow
- Returns `std::nullopt` on malformed data

### 8.5 AddKey Transparent Hasher — ✅ Functionally Correct (M6 limitation noted)

New keys: `dict_keys_` owns string, `dict_` key is `std::string` referencing owned copy.
Existing keys: `find(key)` uses transparent hash but C++17 still constructs temp `std::string`
for key comparison. Non-blocking performance limitation.

### 8.6 Shredding Template Loop Pattern — ✅ Correct

```cpp
template <typename BuilderT, typename NativeT, typename ExtractFn>
Status ShredPrimitiveLoop(...)
```

Correctly handles all 4 states:
1. Input null → both output null
2. Variant::Null → value gets bytes, typed null
3. Extract succeeds → typed gets value, residual null
4. Extract fails → value gets bytes, typed null

`NativeT native_val{}` value-initialized — only read when `extract()` returns true.

### 8.7 Decimal128 Reconstruction — ✅ Endian-safe

Uses `val.high_bits()` / `val.low_bits()` (numeric accessors, not raw bytes), then
`bit_util::ToLittleEndian()` before `memcpy` to output. Architecture-independent.

### 8.8 Metadata Caching in Reconstruction — ✅ Correct

```cpp
if (meta_bytes != cached_meta_bytes) { ... rebuild cache ... }
```

`string_view` comparison avoids redundant `DecodeMetadata` when consecutive rows share
metadata. Lifetime safe: views point into `metadata_array`'s buffer.

---

## 9. FLAKINESS & ROBUSTNESS ASSESSMENT

| Risk Vector | Assessment | Status |
|-------------|-----------|--------|
| Thread safety | Views=const (safe concurrent). Builder=single-threaded (documented). | ✅ |
| Integer overflow | `num_fields × id_size` fits int64. All casts checked. | ✅ |
| Stack overflow | `kMaxNestingDepth = 128` enforced | ✅ |
| Iterator `ValueOrDie()` | Documented with `\warning`. Intentional. | ✅ |
| Float tests | Exact bit patterns, no epsilon | ✅ |
| No file I/O | Pure memory operations | ✅ |
| Unity build | Separate namespace from `variant()` free function | ✅ |
| Empty edge cases | 0-field objects, 0-element arrays tested | ✅ |
| `list_offsets` int32_t | Bounded by ListArray 32-bit offsets | ✅ |
| Per-row map allocation | PERF TODO documented, acceptable for v1 | ✅ |
| `BuildWithoutMeta()` reuse | Moves buffer, clears it, preserves dict | ✅ |

---

## 10. COMPREHENSIVE NITPICKING (maximum pedantry for clean merge)

### N1. Comment inaccuracy: transparent hasher (non-blocking)

**File:** `variant_builder.cc`, `AddKey()` function
```cpp
// Transparent hasher allows direct string_view lookup without constructing
// a std::string. This eliminates per-call allocation/copy for existing keys.
```

**Issue:** This comment is aspirational for C++20 but slightly inaccurate for C++17.
In C++17, `dict_.find(key)` where `key` is `string_view` still goes through the
`template<typename K> find(const K&)` overload which doesn't exist in `std::unordered_map`
until C++20. The transparent hash/equal functors ARE used for hashing, but the container
still constructs a temporary `std::string` for the actual key lookup comparison.

**Impact:** One allocation per existing-key lookup. Functionally correct.
**Recommendation:** Either (a) update comment to say "Forward-compatible with C++20
heterogeneous lookup" or (b) leave as-is since it describes the intent correctly.
**Verdict:** Non-blocking. Reviewers familiar with C++17 limitations may note this,
but it's not wrong enough to block merge.

### N2. `ShredBinaryLoop` naming (cosmetic)

**File:** `variant_shredding.cc`
```cpp
template <typename BuilderT>
Status ShredBinaryLoop(...)
```

Named "Binary" but used for `StringBuilder`, `LargeStringBuilder`, `StringViewBuilder`,
`BinaryBuilder`, `LargeBinaryBuilder`, `BinaryViewBuilder`. Consider `ShredVarLenLoop`
or `ShredBinaryLikeLoop`. Non-blocking — semantics clear from template parameter.

### N3. `elem_typed_field_type` initialization (style)

**File:** `variant_shredding.cc`, array shredding
```cpp
auto elem_typed_field_type = schema.element_schema().type();
```

For Object/Array element schemas, `type()` returns nullptr. The else-branch immediately
overrides it. The initial value is misleading but correct. Non-blocking.

### N4. `EncodedVariant` struct — no doc on move semantics

**File:** `variant.h`
```cpp
struct EncodedVariant {
  std::vector<uint8_t> metadata;
  std::vector<uint8_t> value;
};
```

Could benefit from a brief comment noting that `Finish()` returns this by value (NRVO/move).
Non-blocking — pattern is obvious to C++ developers.

### N5. `FieldEntry::key` ownership duplication

**File:** `variant.h`
```cpp
struct FieldEntry {
  std::string key;
  uint32_t id;
  int64_t offset;
};
```

Each field insertion copies the key into `FieldEntry::key` AND the key is stored in
`dict_keys_`. One copy could theoretically be avoided by storing a `string_view` into
`dict_keys_`, but this creates lifetime complexity (vector reallocation invalidates
views). Current approach is simpler and correct. Non-blocking perf observation.

### N6. `ObjectScope::Finish()` doesn't `std::move(fields_)` (style)

```cpp
Status ObjectScope::Finish() {
  ARROW_RETURN_NOT_OK(parent_->FinishObject(start_offset_, fields_));
  committed_ = true;
  return Status::OK();
}
```

`FinishObject` takes `std::vector<FieldEntry>&` (non-const ref) and modifies in-place.
After `Finish()`, `fields_` is never read again. Could be moved, but since `FinishObject`
needs a mutable reference for sort/dedup anyway, the move would only affect post-call
vector memory. Non-blocking.

### N7. `ToArrowType()` unreachable path

```cpp
DCHECK(false) << "Unknown VariantShreddingSchema kind";
return nullptr;
```

After a switch over all `Kind` enum values, this is unreachable. The DCHECK + nullptr
return is correct defensive coding. Some compilers may warn about missing return. Non-blocking.

### N8. `ValidateOffsets` in DecodeMetadata — vector allocation

```cpp
std::vector<uint32_t> offsets(dict_size + 1);
```

For large dictionaries, this allocates on the heap. Could use `SmallVector` for the
common case. Has existing TODO noting this. Non-blocking perf observation.

### N9. `VisitObject` — vector allocations for field_ids and value_offsets

Same pattern as N8: heap-allocates `std::vector<uint32_t>` for field_ids and offsets
during traversal. For bulk processing of millions of rows, this is measurable. Has
TODO noting `SmallVector` optimization. Non-blocking for first implementation.

### N10. Missing `[[nodiscard]]` on `ValidateVariant()`

The free function `ValidateVariant()` returns `Status` — callers might accidentally
discard the result. `[[nodiscard]]` on the declaration would enforce checking.
Arrow doesn't universally apply this to free functions (inconsistent across codebase),
so this matches repo style. Non-blocking.

---

## 11. CODE STYLE CONSISTENCY WITH EXISTING EXTENSIONS ✅

Verified against `bool8.cc`, `uuid.cc`, `json.cc`, `opaque.cc`, `tensor_internal.cc`:

| Pattern | Arrow norm | Variant code | Match? |
|---------|-----------|-------------|--------|
| `#pragma once` | ✅ | ✅ | ✅ |
| CMake + Meson | ✅ | ✅ | ✅ |
| `ARROW_EXPORT` | ✅ | ✅ | ✅ |
| Internal NOT installed | ✅ (`tensor_internal.h`) | ✅ (`variant_internal_util.h`) | ✅ |
| TEST() macros | Both patterns exist | TEST() used | ✅ |
| Namespace 3-level | Justified by scope | `arrow::extension::variant` | ✅ |
| Error messages | Capitalized, specific | ✅ | ✅ |
| Static in anon namespace | ✅ | ✅ | ✅ |
| Forward declarations | ✅ | ✅ | ✅ |
| `arrow/util/logging_internal.h` | ✅ | ✅ | ✅ |

---

## 12. MERGE ORDER DEPENDENCY ANALYSIS

The merge order **GH-45946 → GH-45947 → GH-45948** is mandatory because:

1. **45946 (decoding)** defines: `VariantView`, `VariantObjectView`, `VariantArrayView`,
   `VariantMetadata`, `DecodeMetadata`, `ValidateVariant`, `variant_internal_util.h`
2. **45947 (encoding)** uses `VariantMetadata` in builder constructor, adds:
   `VariantBuilder`, `ObjectScope`, `ListScope`, builder constants
3. **45948 (shredding)** uses both views AND builder: `ShredVariantColumn` reads via
   views, `ReconstructVariantColumn` writes via builder

**Cross-branch dependency verification:**
- Encoding `#include "arrow/extension/variant.h"` — depends on decoding's header
- Shredding includes both `variant.h` and `variant_internal_util.h` from decoding
- Shredding extends `VariantBuilder` (3 new methods) — depends on encoding's class

**TODO management across branches:**
- Decoding has no TODOs referencing later branches (clean boundary)
- Encoding has `// TODO GH-45948: ...` for the 3 builder methods added by shredding
- Shredding resolves those TODOs and adds its own for follow-up work

**Can we merge out of order?** NO. Each branch would fail to compile without its parent:
- Encoding without decoding: `VariantMetadata` undefined
- Shredding without encoding: `VariantBuilder` class incomplete (missing builder body)

---

## 13. POTENTIAL REVIEWER CONCERNS (pre-emptive answers)

### Q: "Why always binary search instead of threshold?"

Pre-parsed headers make the cost of binary search O(log n) with O(1) setup (no per-access
header re-parsing). Go's threshold exists because Go re-parses the object header on every
`ValueByKey()` call — for small objects, linear scan avoids that re-parse overhead. In C++,
the header is parsed once at `VariantObjectView::Make()` time, so binary search is always
optimal. Matches Rust which also always uses binary search.

### Q: "Why RAII scopes instead of just scope guards?"

Scopes carry state (`fields_`, `offsets_`) needed for `Finish()`. A generic `ScopeGuard`
could only handle rollback, not the insert/append API. Pattern is similar to
`nlohmann::json::array()` builder or Protobuf arena allocation.

### Q: "Why `std::optional<VariantView>` instead of `Result<VariantView>`?"

`get()` is designed for trusted data (builder output) where "not found" is the common
non-error case. For untrusted data needing error reporting, use `field_name(i)` +
`field_value(i)` which return `Result<T>`.

### Q: "Why is `allow_duplicates_` needed?"

Shredding reconstruction combines fields from multiple sources (shredded + residual).
If input was malformed (duplicate keys in residual), strict rejection would make
reconstruction fail. `SetAllowDuplicates(true)` handles this gracefully. Never exposed
as a user-facing default.

### Q: "Why does typed_value declare int64() for TIMESTAMP?"

Arrow C++ builds timestamps via `Int64Builder`. The actual array type is `int64()`,
not `timestamp(MICRO, "UTC")`. Declaring the logical type would cause
`StructArray::Make` type mismatch. Schema carries semantic info for reconstruction.

---

## 14. TODO INVENTORY (all deliberately deferred)

| # | Location | Description | Blocking? |
|---|----------|-------------|-----------|
| 1 | `variant_builder.cc` | Cache sorted state incrementally in `Finish()` | No (perf) |
| 2 | `variant_shredding.cc` | DECIMAL256 scale matching | No (edge case) |
| 3 | `variant_shredding.cc` | Recursive Object/Array sub-schema shredding | No (follow-up PR) |
| 4 | `variant_shredding.cc` | Lift unordered_map to struct for reuse | No (perf) |
| 5 | `variant_shredding.cc` | FixedSizeList/ListView as shredding output | No (follow-up) |
| 6 | `variant_shredding.cc` | CastOptions: Uint, Float16, TimestampSec/Milli | No (follow-up) |
| 7 | `variant_shredding.cc` | NullBuffer input for SQL NULL reconstruction | No (out_null_bitmap covers) |
| 8 | `parquet_variant.h` | Track shredded_value in ExtensionType | No (future integration) |

All TODOs are properly scoped, non-blocking, and reference issue numbers where applicable.

---

## 15. CONCLUSION & RECOMMENDATION

### Verdict: **SHIP IT**

The implementation is:
- **Architecturally sound** — clean separation of concerns
- **Fully refactored** — zero legacy artifacts
- **C++ idiomatic** — every pattern matches Arrow C++ conventions
- **Rust-parity achieved** — core features match, 5 documented gaps deferred
- **Thoroughly tested** — 335 tests, warnings-as-errors
- **Build-system complete** — CMake + Meson, correct install headers
- **All reviewer comments resolved** — architecturally
- **No flakiness vectors** — deterministic, bounded recursion, typed errors
- **Option D clean split** — each PR shows exactly its own work

### Remaining Actions (non-code)

| Action | Priority | Effort | Status |
|--------|----------|--------|--------|
| Force-push branches to origin | Required | 2 min | TODO |
| Update PR descriptions on GitHub | Required | 10 min | TODO |
| Reply to reviewer comments #7/#8/#9 | Required | 5 min | TODO |

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| CI Unity build collision | Low (namespace verified) | Block merge | Namespace is distinct |
| Big-endian s390x failure | Low (ReadLE verified) | Block merge | Accessor-based patterns |
| Test flakiness | Very low (pure memory) | Annoyance | No I/O, no threading |
| API design regret | Low (views are simple) | Follow-up changes | Views are non-breaking to extend |
| Reviewer requests split changes | Medium | Time | Option D already done |

---

## APPENDIX: File-Level Summary

| File | Lines | Branch | Verdict |
|------|-------|--------|---------|
| `variant.h` | ~810 | all 3 | Clean API, correct exports |
| `variant.cc` | ~1314 | decoding | Correct decode, endian-safe |
| `variant_internal_util.h` | ~71 | decoding | Correct ReadLE, not installed |
| `variant_internal_test_util.h` | ~137 | decoding | Test-only, not installed ✅ |
| `variant_builder.cc` | ~651 | encoding+shredding | Correct RAII, sort, dedup |
| `variant_shredding.h` | ~192 | shredding | Clean public API |
| `variant_shredding.cc` | ~2139 | shredding | Template loops correct |
| `variant_test.cc` | ~2412 | decoding | Comprehensive |
| `variant_builder_test.cc` | ~1228 | encoding | Round-trip + RAII |
| `variant_shredding_test.cc` | ~2224 | shredding | All types + errors |
| `parquet_variant.h` | ~89 | shredding | Minimal change (TODO) |

**No code changes required for merge.**
