# Variant Encode/Decode — Agent Context (v2)

> Last updated: 2026-06-26 (PE review v11 — comprehensive final audit)
> Owner: @qzyu999
> Umbrella issue: GH-45937 [C++][Parquet] Add variant support

---

## ⚡ QUICK STATUS

**Status:** READY TO PUSH. All fixes applied. Docker verified. PE review v11 PASS.
**Merge readiness: 99%** — Only non-blocking M6 remains (transparent hasher C++17).

**Branch order (merge order):** GH-45946 (decoding) → GH-45947 (encoding) → GH-45948 (shredding)

### Branch state (2026-06-26, Option D clean split — FINAL)

```
main (e16067a78c)
  └── variant-decoding (162d503276)  — PR #50121
       └── variant-encoding (f6b8e6609b)  — PR #50122
            └── variant-shredding-impl (034ff491c9)  — PR #50232
```

- 335/335 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN`
- clang-format-18: PASSED (zero violations)
- Each PR = single commit, shows ONLY its own diff
- Working tree CLEAN (no uncommitted changes)
- Install header leak FIXED (`variant_internal_test_util.h`)

---

## Outstanding Actions (non-code — push + PR management only)

| Action | Priority | Effort | Status |
|--------|----------|--------|--------|
| Force-push branches to origin | Required | 2 min | TODO |
| Update PR descriptions on GitHub | Required | 10 min | TODO |
| Reply to reviewer comments #7/#8/#9 on PR #50122 | Required | 5 min | TODO |

---

## All Prior Tasks — COMPLETE

1. ✅ Fix B1 (SetAllowDuplicates dead code)
2. ✅ Docker verify (335/335 PASSED)
3. ✅ Fix C1/C2/C3/N2
4. ✅ clang-format-18
5. ✅ Numeric coercion (TODO 1)
6. ✅ Shared ReadLE utility (TODO 2)
7. ✅ NullBuffer return (TODO 3)
8. ✅ ValidateVariant (TODO 4)
9. ✅ Option D clean branch split
10. ✅ PE review v7, v8, v9, v10, v11 — no blocking issues
11. ✅ Rename `variant_test_util.h` → `variant_internal_test_util.h`

---

## Key Design Decisions

1. **Single header:** `variant.h` contains views, builder, scopes, visitor
2. **Namespace:** `arrow::extension::variant`
3. **No threshold:** Binary search always (pre-parsed header)
4. **RAII builders:** `ObjectScope`/`ListScope` auto-rollback
5. **NO legacy compat layer:** Old free functions removed entirely
6. **`std::optional` for not-found:** `VariantObjectView::get()`
7. **`[[nodiscard]]` on scopes:** Prevents accidental discard
8. **Move-only builder:** Copy deleted, move noexcept
9. **Validated factories:** `Make()` static methods validate at construction
10. **Zero-copy reads:** `string_view` into source buffer
11. **Transparent hasher:** Forward-compatible with C++20
12. **Sorted-check optimization:** `FinishObject` skips sort when ordered
13. **Named constants:** `kUUIDByteLength`, `kMaxShortStringLength`, `kMaxDecimalScale`, `kLargeContainerThreshold`

---

## Rust Parity Summary

**Core features match.** 5 documented gaps for follow-up:

1. Object/Array recursive sub-schema shredding (~200-400 lines, separate PR)
2. CastOptions cross-type coercion (needs arrow_compute)
3. Value-absent schemas (`{metadata, typed_value}` without `value`)
4. Array shredding output variety (C++ always List; Rust: LargeList/FSL/ListView)
5. Unsigned integer targets (Uint8/16/32/64 via cast)

---

## Go Parity Summary

- C++ strictly better: no threshold, pre-parsed headers, RAII, depth limit, is_sorted
- FinishObject duplicate handling: semantically equivalent (different strategy, same result)
- Go `valueSize()` bug: C++ has correct implementation (Go fix submitted as #839)
- Go `ObjectValue.ValueByKey()` unsigned underflow: C++ uses signed int32_t (correct)

---

## File Layout

| File | Branch | Purpose |
|------|--------|---------|
| `variant.h` | all 3 | Public API: views, builder, scopes, visitor |
| `variant.cc` | decoding | View implementations, metadata, visitor, coercion, validation |
| `variant_internal_util.h` | decoding | Shared internal ReadLE (NOT installed) |
| `variant_internal_test_util.h` | decoding | Test-only RecordingVisitor (NOT installed ✅) |
| `variant_builder.cc` | encoding+shredding | Builder + RAII scopes |
| `variant_shredding.h` | shredding | Shredding public API |
| `variant_shredding.cc` | shredding | Full shred/reconstruct engine |
| `parquet_variant.h` | pre-existing | VariantExtensionType |

---

## PR Links

- Decoding: https://github.com/apache/arrow/pull/50121
- Encoding: https://github.com/apache/arrow/pull/50122
- Shredding: https://github.com/apache/arrow/pull/50232

---

## Push Commands (ready now)

```bash
git push origin variant-decoding --force-with-lease
git push origin variant-encoding --force-with-lease
git push origin variant-shredding-impl --force-with-lease
```

---

## Reviewer Comment Replies (pre-drafted)

**#7 (metadata/data type mismatch test):**
> The variant metadata dictionary contains only key names (string interning for field
> names), not value types. The format is self-describing — each value carries its own
> type tag in its header byte. A "type mismatch between metadata and values" is
> architecturally impossible because metadata doesn't encode types at all. The refactored
> `VariantMetadata` docstring states explicitly: "NOT a schema — contains key names only."

**#8 (initialize builder from existing buffer):**
> The variant binary format is immutable by design — inserting a field requires rewriting
> the header (field IDs and offsets are packed arrays). The refactored design handles
> "start from existing data" via `VariantBuilder(existing_metadata)` (pre-populates
> dictionary) + `UnsafeAppendEncoded` (zero-copy field transfer). This read→rebuild
> pattern is exactly what shredding reconstruction uses.

**#9 (API for modifying existing variants):**
> Views (read) + builders (write) is the deliberate separation. "Modify" = read old via
> `VariantObjectView` → build new selectively via `ObjectScope` + `UnsafeAppendEncoded`.
> Matches Rust's `Variant` (read-only) vs `VariantBuilder` (write-only). A higher-level
> mutable DOM could layer on top in follow-up.

---

## Follow-up Work (after merge, not blocking)

- `VariantPath` convenience class — DEFERRED (pure convenience)
- Object/Array recursive sub-schema shredding — DEFERRED (separate PR)
- CastOptions cross-type coercion — DEFERRED (needs arrow_compute)
- FixedSizeList/ListView as shredding output — DEFERRED
- JSON serialization — SEPARATE CONCERN
- `variant_get` kernel — SEPARATE CONCERN
- Transparent hasher → `absl::flat_hash_map` or C++20 — WHEN ARROW ADOPTS

---

## Build & Test

### Docker-based testing (required for CI parity)

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

### Formatting check

```bash
docker run --rm -v "${PWD}:/arrow" -w /arrow ubuntu:24.04 bash -c \
  "apt-get update && apt-get install -y clang-format-18 >/dev/null 2>&1 && \
  find cpp/src/arrow/extension/variant* -name '*.cc' -o -name '*.h' | \
  xargs clang-format-18 --dry-run --Werror"
```

---

## PE Review v11 Key Findings

### No Code Changes Required

The implementation passes all quality gates:
- C++ standards compliance ✅
- Arrow conventions ✅
- Rust parity (core) ✅
- Big-endian safety ✅
- Flakiness assessment ✅
- Artifact cleanup ✅
- All 9 reviewer comments resolved ✅

### Non-Blocking Nitpicks (10 identified, none blocking)

1. N1: Transparent hasher comment slightly aspirational for C++17
2. N2: `ShredBinaryLoop` name could be `ShredVarLenLoop`
3. N3: `elem_typed_field_type` misleading initial value
4. N4: `EncodedVariant` struct no doc on move semantics
5. N5: `FieldEntry::key` ownership duplication (perf)
6. N6: `ObjectScope::Finish()` doesn't `std::move(fields_)`
7. N7: `ToArrowType()` unreachable path defensive return
8. N8: `ValidateOffsets` heap allocation (SmallVector TODO exists)
9. N9: `VisitObject` field_ids/value_offsets allocation (same)
10. N10: Missing `[[nodiscard]]` on `ValidateVariant()` (matches repo style)

**Verdict:** None of these warrant delaying merge. All are follow-up polish.

---

## Merge Order Dependency Analysis

**45946 → 45947 → 45948 is MANDATORY because:**

1. Encoding uses `VariantMetadata` (defined in decoding)
2. Shredding uses both `VariantView` classes (decoding) AND `VariantBuilder` (encoding)
3. Cannot compile any branch without its parent merged

**Cannot merge out of order — each branch fails to compile independently.**

---

## Architecture Overview (for new agents picking this up)

```
┌─────────────────────────────────────────────────────────┐
│                    variant.h (public API)                 │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────┐  │
│  │  VariantView  │ │VariantBuilder│ │  ObjectScope    │  │
│  │  ObjectView   │ │              │ │  ListScope      │  │
│  │  ArrayView    │ │              │ │  [[nodiscard]]  │  │
│  │  Visitor      │ │  move-only   │ │  auto-rollback  │  │
│  └──────┬───────┘ └──────┬───────┘ └────────┬────────┘  │
│         │                 │                   │           │
│         ▼                 ▼                   ▼           │
│   variant.cc         variant_builder.cc                   │
│   (decode+validate)  (encode+scopes)                     │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│             variant_shredding.h / .cc                     │
│  ┌──────────────────┐ ┌──────────────────────────────┐   │
│  │VariantShredSchema │ │ ShredVariantColumn()          │   │
│  │  Primitive        │ │ ReconstructVariantColumn()    │   │
│  │  Object           │ │ IsVariantCompatibleWithType() │   │
│  │  Array            │ │                              │   │
│  └──────────────────┘ └──────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│             parquet_variant.h                             │
│        VariantExtensionType (pre-existing)               │
│        TODO: shredded storage layout integration         │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

**Shredding:** `metadata_array` + `value_array` → `ShredVariantColumn(schema)` →
`StructArray{metadata, value(nullable), typed_value(nullable)}`

**Reconstruction:** `metadata_array` + `value_array` + `typed_value_array` →
`ReconstructVariantColumn(schema)` → `BinaryArray` (fully materialized variant bytes)

### Thread Safety

- **Views:** immutable (const member access) → safe for concurrent reads
- **Builder:** mutable (buffer appends) → single-threaded only (documented)
- **Shredding functions:** stateless (all state in local variables) → thread-safe per-call

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| v1 | 2026-06-07 | Initial agent context |
| v2 (this) | 2026-06-26 | PE review v11 final, all fixes applied, ready for push |
