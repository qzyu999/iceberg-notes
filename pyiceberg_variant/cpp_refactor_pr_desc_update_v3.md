# PR Description Update v3 + Review Comment Replies + Push Plan (FINAL)

> Date: 2026-06-26
> Context: Option D rebuild COMPLETE. All 3 branches are single-commit, clean-split.
> PE review v11 PASSED. `variant_internal_test_util.h` rename DONE. Ready to push.
> Previous versions:
>   - `cpp_refactor_pr_desc_update.md` (Option A/B/C/D analysis — resolved via Option D)
>   - `cpp_refactor_pr_desc_update_v2.md` (pre-rename, used stale commit hashes)

---

## What Changed Since v2

1. **`variant_test_util.h` → `variant_internal_test_util.h`** — Fixed install header leak.
   CMake's `ARROW_INSTALL_ALL_HEADERS` only excludes files with "internal" in the name.
   Without the rename, the test-only header (containing gtest-dependent `RecordingVisitor`)
   would be installed as a public SDK header.
2. **All branch commits updated** — Rebased after rename. New commit hashes.
3. **Docker re-verified** — 335/335 tests PASSED after rename.
4. **PE review v11** — Comprehensive final audit. No blocking issues. Ship it.

---

## Current Branch State (FINAL — verified 2026-06-26)

```
main (e16067a78c)
  └── variant-decoding (162d503276)  — PR #50121
       └── variant-encoding (f6b8e6609b)  — PR #50122
            └── variant-shredding-impl (034ff491c9)  — PR #50232
```

- 335/335 tests PASSED with `BUILD_WARNING_LEVEL=CHECKIN`
- clang-format-18: PASSED (zero violations)
- Each PR = single commit, shows ONLY its own diff
- Working tree CLEAN
- Install headers verified (only `variant.h` and `variant_shredding.h` installed)

---

## Push Plan

### Step 1: Force-push commands (from arrow repo root)

```bash
# Verify current state
git log --oneline variant-decoding variant-encoding variant-shredding-impl -n1 --no-walk
# Expected output:
#   162d503276 (variant-decoding) GH-45946: [C++][Parquet] Variant decoding
#   f6b8e6609b (variant-encoding) GH-45947: [C++][Parquet] Variant encoding with RAII builders
#   034ff491c9 (variant-shredding-impl) GH-45948: [C++][Parquet] Variant shredding

# Force-push all three (order doesn't matter for push, merges must be sequential)
git push origin variant-decoding --force-with-lease
git push origin variant-encoding --force-with-lease
git push origin variant-shredding-impl --force-with-lease
```

### Step 2: Update PR descriptions

Use `gh pr edit` (GitHub CLI) or the web UI:

```bash
gh pr edit 50121 --body-file /tmp/pr_50121_body.md
gh pr edit 50122 --body-file /tmp/pr_50122_body.md
gh pr edit 50232 --body-file /tmp/pr_50232_body.md
```

### Step 3: Post force-push comments (§2)

### Step 4: Reply to reviewer comments (§3)

---

## §1. PR Descriptions (FINAL — reflects rename + 335 tests)

### PR #50121 — GH-45946: [C++][Parquet] Variant decoding

```markdown
### Rationale for this change

Implements full Variant binary decoding per the [VariantEncoding spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md). Part of [GH-45937](https://github.com/apache/arrow/issues/45937) (Add variant support to C++).

### What changes are included in this PR?

Adds `variant.h` (public API) and `variant.cc` (implementation) providing:

- **View classes** (`VariantView`, `VariantObjectView`, `VariantArrayView`): zero-copy,
  stack-allocated views that pre-parse headers at construction and provide type-safe
  access thereafter. O(log n) object field lookup via binary search always (no threshold).
- **SAX-style visitor** (`VariantVisitor`): recursive traversal interface for full tree
  processing, following Arrow C++ conventions (TypeVisitor, ArrayVisitor).
- **Metadata decoding** (`DecodeMetadata`, `FindMetadataKey`): string dictionary parsing
  with binary search for sorted dictionaries.
- **Numeric coercion** (`as_int64_coerced`, `as_int32_coerced`, `as_double_coerced`):
  widening accessors matching Rust's `as_i64()` / `as_f64()` pattern.
- **Recursive validation** (`ValidateVariant`): deep structural validation for untrusted
  input (validates all offsets, field IDs, nesting depth ≤128).
- **Shared internal utility** (`variant_internal_util.h`): endian-safe ReadLE helpers
  used by both decoding and shredding implementations. NOT installed (internal only).

Design decisions:
- Parse once, query many (views pre-parse headers, subsequent access is O(1))
- Zero-copy (`string_view` into source buffers, no heap allocation for reads)
- Recursion depth limit (`kMaxNestingDepth = 128`) — security hardening for C++ stack
- Binary search always — no threshold heuristic (pre-parsed header makes it optimal)
- `std::optional` for not-found semantics (idiomatic C++)
- Validated factories (`Make()`) ensure bounds-safe subsequent access
- `static_assert` on view class sizes (≤32/80/64 bytes — cache-friendly)

### Are these changes tested?

134 variant-specific tests pass with `BUILD_WARNING_LEVEL=CHECKIN` covering: all 21
primitive types, short/long strings, objects (including 3-byte offsets), arrays
(including is_large), nesting, depth limits, metadata edge cases, error paths,
view API, numeric coercion, recursive validation, and visitor traversal.

### Are there any user-facing changes?

New public API in `arrow/extension/variant.h`: `VariantView`, `VariantObjectView`,
`VariantArrayView`, `VariantVisitor`, `VariantMetadata`, `DecodeMetadata`,
`FindMetadataKey`, `ValueSize`, `ValidateVariant`, and associated types/enums.
All in namespace `arrow::extension::variant`.

**AI Disclosure:** AI coding assistants were used during development for scaffolding,
test generation, and review iteration. All code has been reviewed, debugged, and
verified by the author who owns and understands the changes.

* GitHub Issue: #45946
```

---

### PR #50122 — GH-45947: [C++][Parquet] Variant encoding with RAII builders

```markdown
### Rationale for this change

Implements Variant binary encoding (the write side of decoding from [GH-45946](https://github.com/apache/arrow/issues/45946)). Part of [GH-45937](https://github.com/apache/arrow/issues/45937) (Add variant support to C++). Depends on #50121.

**Note:** This PR depends on #50121 (Variant decoding) and is branched from it. Please review/merge #50121 first.

### What changes are included in this PR?

Adds `VariantBuilder` class and RAII scope helpers to `variant.h` / `variant_builder.cc`:

- **`VariantBuilder`**: move-only encoder supporting all 21 primitive types + containers.
  `Int()` auto-selects smallest encoding width. `String()` auto-selects short-string
  (≤63 bytes) vs long-string encoding. Dictionary (key interning) preserved across
  `Finish()` calls for column-scan workloads.
- **`ObjectScope` / `ListScope`**: RAII scopes returned by `StartObject()` / `StartList()`.
  Destructor auto-rolls back the buffer if `Finish()` is not called — safe under
  exceptions, early returns, and scope exit.
- **`[[nodiscard]]`** on scope-returning functions prevents accidental discard.
- **Duplicate key handling**: strict rejection by default (spec-compliant). Configurable
  via `SetAllowDuplicates(true)` for shredding reconstruction ([GH-45948](https://github.com/apache/arrow/issues/45948)).
- **Sorted-check optimization**: `FinishObject()` skips `std::sort` when fields are
  already in lexicographic order (common for schema-driven insertion).
- **Transparent hasher**: `dict_` uses `is_transparent` tags for forward-compatibility
  with C++20 heterogeneous lookup (eliminates the old `lookup_buf_` member variable).

Design decisions:
- Move-only (no accidental copies of builder state)
- RAII rollback matches C++ idiom for transactional operations
- Low-level API (`Offset`/`NextField`/`FinishObject`) retained for shredding internals
- `FinishObject` sorts fields in-place (documented non-const-ref parameter)
- Dictionary preserved across `Finish()` — amortizes key lookup for repeated schemas
- Scale validation on Decimal (≤38, matching spec)

### Are these changes tested?

221 total tests (87 new encoder + 134 decoder) pass with `BUILD_WARNING_LEVEL=CHECKIN`
covering: all primitives, auto-sizing, int boundaries, short/long string boundary,
special floats (NaN, ±Inf), arrays, objects, duplicate rejection, sorting, RAII scopes,
reset/reuse, builder from existing metadata, large containers (>255 elements), and
round-trip verification via decoder views.

### Are there any user-facing changes?

New public API: `VariantBuilder`, `ObjectScope`, `ListScope` in `variant.h`.
`VariantBuilder::EncodedVariant` return type from `Finish()`.

**AI Disclosure:** AI coding assistants were used during development for scaffolding,
test generation, and review iteration. All code has been reviewed, debugged, and
verified by the author who owns and understands the changes.

* GitHub Issue: #45947
```

---

### PR #50232 — GH-45948: [C++][Parquet] Variant shredding

```markdown
### Rationale for this change

Implements variant shredding/unshredding for C++ ([GH-45948](https://github.com/apache/arrow/issues/45948)), part of the [GH-45937](https://github.com/apache/arrow/issues/45937) umbrella. Enables decomposing variant binary columns into native typed Arrow columns for Parquet statistics-based predicate pushdown. Depends on #50121 (decoding) and #50122 (encoding).

**Note:** This PR depends on #50121 and #50122. Please review/merge those first.

### What changes are included in this PR?

Adds `variant_shredding.h` / `variant_shredding.cc` implementing:

- **`VariantShreddingSchema`** — tree structure defining shredding targets (Primitive,
  Object, Array). C++ equivalent of Rust's `ShreddedSchemaBuilder`.
- **`IsVariantCompatibleWithType()`** — strict type compatibility with safe int widening,
  Float→Double widening, timestamp unit+timezone matching, and decimal scale matching.
- **`ShredVariantColumn()`** — column-level shredding producing `{metadata, value, typed_value}`.
  Template-refactored loops (`ShredPrimitiveLoop<>`, `ShredBinaryLoop<>`) for all
  15+ supported Arrow target types.
- **`ReconstructVariantColumn()`** — column-level reconstruction reassembling shredded
  columns back to variant binary. Supports all list-like typed_value types (List,
  LargeList, FixedSizeList, ListView, LargeListView).

Extends `VariantBuilder` with 3 methods for shredding support:
- `BuildWithoutMeta()` — produce value bytes without metadata (for primitives)
- `UnsafeAppendEncoded()` — zero-copy raw byte append
- `SetAllowDuplicates(true)` — last-value-wins dedup for reconstruction safety

**Supported shredding targets (Rust parity):**
Bool, Int8, Int16, Int32, Int64, Float, Double, String, LargeString, StringView,
Binary, LargeBinary, BinaryView, Date32, Timestamp(Micro/Nano, TZ/NTZ), Time64(Micro),
FixedSizeBinary(16) (UUID), Decimal128 (scale-matched)

**Variant::Null semantics (Rust parity):** Variant::Null (0x00) is stored in the
value column, NOT the typed_value column. Distinguishes variant-null from SQL NULL.

**NullBuffer output (Rust parity):** Optional `out_null_bitmap` parameter on
`ReconstructVariantColumn` for SQL NULL disambiguation (bit=0 where both value and
typed_value are null).

**Known gaps (documented TODOs for follow-up PRs):**
- Recursive Object/Array sub-schema shredding in object fields (primitives only currently)
- CastOptions cross-type coercion (Uint, Float16, Decimal32/64, TimestampSecond/Milli)
- FixedSizeList/ListView as shredding output targets (reconstruction accepts all)
- Value-absent schemas (`{metadata, typed_value}` without `value`)
- DECIMAL256 shredding target (compatibility check exists but shred/reconstruct not wired)

### Are these changes tested?

335 total tests (114 new shredding + 221 prior) pass with `BUILD_WARNING_LEVEL=CHECKIN`
covering: schema definition, type compatibility, primitive round-trip for all supported
types, object shredding (full/partial/fallback), array shredding (recursive elements),
typed round-trip (Decimal128, UUID, all timestamps, Float→Double, Int8/Int16, LargeString,
LargeBinary, StringView, BinaryView), all list-like reconstruction, error cases, and
NullBitmap semantics.

### Are there any user-facing changes?

New public API in `arrow/extension/variant_shredding.h`: `VariantShreddingSchema`,
`IsVariantCompatibleWithType()`, `ShredVariantColumn()`, `ReconstructVariantColumn()`.
New methods on `VariantBuilder`: `BuildWithoutMeta()`, `UnsafeAppendEncoded()`,
`SetAllowDuplicates()`.

**AI Disclosure:** AI coding assistants were used during development for scaffolding,
test generation, and review iteration. All code has been reviewed, debugged, and
verified by the author who owns and understands the changes.

* GitHub Issue: #45948
```

---

## §2. Force-Push Comment (post on each PR after force-push)

### Comment for PR #50121:

```
I've force-pushed a refactored version that addresses all review feedback. After carefully
reviewing the comments, it became clear that several design choices in the earlier iteration
stemmed from initially trying to follow Go's implementation patterns (free functions, manual
buffer management, linear/binary threshold). I've since reworked the implementation from
scratch with C++ ergonomics and idiom as the guiding principle.

Key changes in this force-push:
- **View classes** (`VariantView`, `VariantObjectView`, `VariantArrayView`) replace the
  previous free-function API — parse headers once at construction, O(log n) binary search
  always (no threshold), `std::optional` for not-found semantics
- **Numeric coercion accessors** (`as_int64_coerced`, `as_double_coerced`) for Rust parity
- **Recursive validation** (`ValidateVariant`) for untrusted input
- **Shared internal utility** (`variant_internal_util.h`) consolidates ReadLE helpers
- Previous `variant_internal.h` naming confusion resolved — main API is `variant.h`
- Test utility renamed to `variant_internal_test_util.h` (ensures it's not installed)

All 335 tests pass end-to-end with `BUILD_WARNING_LEVEL=CHECKIN` (134 decoder-specific).
```

### Comment for PR #50122:

```
Force-pushed a refactored version. The initial implementation carried over some Go
patterns (manual start/finish without safety guarantees, separate lookup buffer). After
reviewing the feedback — particularly around initialization from existing buffers and
the question about modifying existing variants — I reworked the builder with C++ idiom
at the center of the design.

Key changes:
- **RAII scopes** (`ObjectScope`/`ListScope`) with auto-rollback on destruction replace
  the previous unguarded start/finish pattern
- **`[[nodiscard]]`** on scope-returning functions prevents silent discard
- **Transparent hasher** on `dict_` eliminates the old `lookup_buf_` member variable
- **Sorted-check optimization** in `FinishObject` (skip sort when fields already ordered)
- Move-only builder (copy deleted) enforces single ownership

Regarding the earlier review questions about modifying existing variants and type mismatch
testing — I've addressed these in replies below with architectural context from the
refactored design.

All 335 tests pass end-to-end with `BUILD_WARNING_LEVEL=CHECKIN` (87 encoder-specific).
```

### Comment for PR #50232:

```
Force-pushed the shredding implementation built on top of the refactored decoding/encoding
layers. This uses the idiomatic C++ view classes and RAII builder from the parent PRs.

Highlights:
- Template-refactored shredding loops (`ShredPrimitiveLoop<>`, `ShredBinaryLoop<>`)
  eliminate per-type code duplication
- Recursive array element shredding (Rust parity)
- All 5 list-like types supported in reconstruction (List, LargeList, FixedSizeList,
  ListView, LargeListView)
- `out_null_bitmap` parameter for SQL NULL disambiguation (Rust NullBuffer parity)
- Metadata caching in reconstruction path (avoids redundant `DecodeMetadata` per row)
- Object sub-field native extraction (primitives only; Object/Array sub-schemas deferred)

335 tests pass with `BUILD_WARNING_LEVEL=CHECKIN` (114 shredding-specific).
```

---

## §3. Replies to Reviewer Comments

### PR #50121 (Decoding) — 6 Comment Replies

#### Reply to Comment #1: "How was the 32 threshold determined?"

> Thanks for raising this — it was a fair question about the original design. The
> threshold was inherited from the Go implementation which uses 32 as the cutoff
> between linear scan and binary search. After reflecting on the review feedback, I
> realized this was a case where I was carrying over Go's pattern without questioning
> whether it made sense for C++.
>
> In the refactored design, the threshold is **eliminated entirely**. The new
> `VariantObjectView` pre-parses the object header at construction time (field count,
> ID array start, offset array start, data start), so subsequent field lookups are just
> binary search through a pre-computed structure — O(log n) for all n, with no per-access
> parsing overhead. This makes a threshold unnecessary because the cost that justified
> linear scan for small objects (re-parsing the header each time) no longer exists.
>
> The pre-parsed approach is similar to how `arrow-rs`'s `VariantObject` works — it
> validates structure upfront and provides O(1) indexed access and O(log n) name lookup
> thereafter.

#### Reply to Comment #2: "§3 references — link to spec"

> Fixed. All enum comments now reference the canonical spec location:
> `https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types`

#### Reply to Comment #3: "Rename file — 'internal' confusing"

> Agreed — the naming was confusing. In the refactored layout:
> - **`variant.h`** — the main public API (views, builder, visitor, types). Clear name.
> - **`variant_internal_util.h`** — a small (~71 line) file with shared `ReadLE` utilities.
>   Genuinely internal (not installed), and "internal" in the name is accurate since it's
>   excluded from `install_headers()` by CMake's glob filter.
> - **`variant_internal_test_util.h`** — test-only header with `RecordingVisitor`.
>   Also excluded from install (has "internal" in name).
>
> The original `variant_internal.h` that contained the full public API (confusingly named)
> no longer exists.

#### Reply to Comment #4: "Add nested navigation test"

> Added. The refactored view classes support composable navigation:
> ```cpp
> auto obj = view.as_object();
> auto inner = obj->get("address")->as_object();
> auto city = inner->get("city")->as_string();
> ```
> Tests exercise this chaining pattern with multi-level nesting (object → object → value,
> object → array → value, etc.).

#### Reply to Comment #5: "DecodeValueAt should be public"

> In the refactored design, this use case is covered by
> `VariantView::Make(metadata, data + offset, size)` — you can construct a view at any
> byte offset within a buffer. There's no separate `DecodeValueAt` because the view
> factory IS the decode-at-offset operation.
>
> For object fields specifically, `VariantObjectView::locate(name)` returns an
> `optional<FieldLocation>` with offset + size without constructing the inner view,
> which is useful for zero-copy byte transfer (used by the shredding path).

#### Reply to Comment #6: "Plan for shredded variant reading?"

> Implemented in [PR #50232](https://github.com/apache/arrow/pull/50232) (the shredding
> PR in this stack). `ReconstructVariantColumn()` handles the "unshredding" path —
> reassembling typed Parquet columns back into variant binary.

---

### PR #50122 (Encoding) — 3 Comment Replies

#### Reply to Comment #7: "Test for metadata/data type mismatch"

> Thanks for the suggestion. After working through the refactoring, I realized this test
> isn't possible because of how the variant format works — the metadata dictionary
> contains **only key names** (string interning for object field names), not value types.
> The format is self-describing: each value carries its own type tag in its header byte.
>
> A "type mismatch between metadata and data" is architecturally impossible because
> metadata doesn't encode types at all. The refactored `VariantMetadata` struct's
> docstring makes this explicit: "This is NOT a schema — it contains key names only,
> not value types."
>
> What you CAN test (and what the validation tests cover) is structural invariant
> violations: field IDs that exceed the dictionary size, offsets that point out of bounds,
> truncated values, etc. These are exercised by the `ValidateVariant` tests.

#### Reply to Comment #8: "Initialize builder from existing buffer"

> Good question. The variant binary format is immutable by design — inserting a field into
> an existing object requires rewriting the header (field IDs and offsets are packed
> arrays, not linked structures). So "modify in place" isn't feasible at the format level.
>
> The refactored design handles the "start from existing data" use case through:
>
> 1. **`VariantBuilder(const VariantMetadata& existing_metadata)`** — constructor that
>    pre-populates the key dictionary from an existing metadata buffer.
>
> 2. **Read→rebuild pattern** — `VariantObjectView` (read existing) + `ObjectScope` +
>    `UnsafeAppendEncoded` (write new). This enables zero-copy field transfer between
>    variants. It's exactly what the shredding reconstruction path uses.
>
> This matches Rust's `Variant` (read-only) vs `VariantBuilder` (write-only) architecture,
> and is the standard immutable-format pattern (FlatBuffers, Cap'n Proto, etc.).

#### Reply to Comment #9: "API for modifying existing variants / move context"

> The refactored design makes this separation explicit and deliberate:
>
> - **Views** — zero-copy navigation of existing bytes (stack-allocated)
> - **Builder** — produces new bytes from scratch (dictionary preserved across `Finish()`)
>
> "Modify existing" = read the parts you want to keep via views, write them into a new
> builder with `UnsafeAppendEncoded`, add/change what you need, finish.
>
> A higher-level mutable convenience API (think `nlohmann::json`-style) could be layered
> on top in a follow-up. For the first implementation, keeping the primitives clean and
> composable felt like the right foundation.

---

## §4. Order of Operations (checklist)

```
[x] 1. Force-push all 3 branches:                              ✅ DONE 2026-06-26
       git push origin variant-decoding --force-with-lease      (b0c22987b9 → 162d503276)
       git push origin variant-encoding --force-with-lease      (8ab28f0a34 → f6b8e6609b)
       git push origin variant-shredding-impl --force-with-lease (c92cb110b0 → 034ff491c9)

[x] 2. Update PR #50121 description (via gh pr edit)            ✅ DONE 2026-06-26
[x] 3. Update PR #50122 description (via gh pr edit)            ✅ DONE 2026-06-26
[x] 4. Update PR #50232 description (via gh pr edit)            ✅ DONE 2026-06-26

[ ] 5. Post force-push comment on PR #50121 (from §2)
[ ] 6. Post force-push comment on PR #50122 (from §2)
[ ] 7. Post force-push comment on PR #50232 (from §2)

[ ] 8. Reply to Comments #1-#6 on PR #50121
[ ] 9. Reply to Comments #7-#9 on PR #50122
```

---

## §5. Diff Summary (what reviewers will see)

### PR #50121 diff (main → variant-decoding):

**New files:**
- `cpp/src/arrow/extension/variant.h` — Public API (~810 lines): enums, constants, metadata, views, visitor, builder declarations, scope declarations
- `cpp/src/arrow/extension/variant.cc` — Implementation (~1314 lines): decode logic, views, visitor, coercion, validation
- `cpp/src/arrow/extension/variant_internal_util.h` — Internal ReadLE utilities (~71 lines)
- `cpp/src/arrow/extension/variant_internal_test_util.h` — Test-only RecordingVisitor (~137 lines)
- `cpp/src/arrow/extension/variant_test.cc` — 134 decoder tests (~2412 lines)

**Modified files:**
- `cpp/src/arrow/CMakeLists.txt` — Added `variant.cc` to sources
- `cpp/src/arrow/extension/CMakeLists.txt` — Added test files to `CANONICAL_EXTENSION_TESTS`
- `cpp/src/arrow/meson.build` — Mirror of CMake source addition
- `cpp/src/arrow/extension/meson.build` — Test + install header entries

### PR #50122 diff (variant-decoding → variant-encoding):

**New files:**
- `cpp/src/arrow/extension/variant_builder.cc` — Builder + scopes (~635 lines)
- `cpp/src/arrow/extension/variant_builder_test.cc` — 87 encoder tests (~1228 lines)

**Modified files:**
- `cpp/src/arrow/extension/variant.h` — Added VariantBuilder, ObjectScope, ListScope, constants (+231 lines)
- `cpp/src/arrow/extension/variant_test.cc` — Added coercion/validation integration tests (+190 lines)
- Build files (CMake + Meson) — Added builder source and test

### PR #50232 diff (variant-encoding → variant-shredding-impl):

**New files:**
- `cpp/src/arrow/extension/variant_shredding.h` — Shredding public API (~192 lines)
- `cpp/src/arrow/extension/variant_shredding.cc` — Shred/reconstruct engine (~2140 lines)
- `cpp/src/arrow/extension/variant_shredding_test.cc` — 114 shredding tests (~2224 lines)

**Modified files:**
- `cpp/src/arrow/extension/variant.h` — Added 3 builder methods for shredding (+9 lines)
- `cpp/src/arrow/extension/variant_builder.cc` — Implementation of 3 methods (+18 lines)
- `cpp/src/arrow/extension/parquet_variant.h` — Added shredding integration TODO (+5 lines)
- Build files (CMake + Meson) — Added shredding files

---

## §6. Key Technical Points for Reviewers

### Why no threshold for binary search?

Pre-parsed headers (`VariantObjectView::Make()`) make the per-access cost O(1) for
index-based operations. Binary search on the pre-parsed field ID array is always optimal
because there's no header re-parsing overhead to amortize. Go uses a threshold because
it re-parses the object header on every `ValueByKey()` call.

### Why RAII scopes instead of start/finish pairs?

C++ has no borrow checker. Without RAII, an exception or early return between `StartObject()`
and `FinishObject()` leaves the builder in a corrupt state (partially-written object bytes
in the buffer). The scope destructor auto-truncates, making the builder always consistent.

### Why `std::optional` for `get()` instead of `Result<T>`?

`get()` is designed for trusted data (builder output) where "not found" is the expected
case, not an error. For untrusted data, use `field_name(i)` + `field_value(i)` which
return `Result<T>` with descriptive error messages.

### Why does shredding use `int64()` for TIMESTAMP typed_value fields?

Arrow C++ doesn't have a `TimestampBuilder` — timestamps are built via `Int64Builder`.
The actual array type is `int64()`, not `timestamp(MICRO, "UTC")`. Using the logical type
would cause a type mismatch in `StructArray::Make`. The schema object carries semantic
information for reconstruction dispatch.

### Why `SetAllowDuplicates(true)` in reconstruction?

Shredding reconstruction merges fields from multiple sources (shredded typed columns +
residual binary). If the input had duplicate keys in the residual (malformed but possible),
strict rejection would crash reconstruction. Last-value-wins dedup handles this gracefully.
It's never exposed as a user-facing default.

---

## §7. Notes on Tone for Replies

- **Open-minded** — acknowledging feedback was valuable and drove real improvements
- **Truthful** — the original DID follow Go patterns too closely; refactoring fixed it
- **Technical but not defensive** — explaining WHY without dismissing reviewers
- **Forward-looking** — mentioning follow-up possibilities shows roadmap awareness

Common thread: "Review feedback made me realize I was carrying over patterns from Go/Rust
without questioning whether they were the right fit for C++. The refactoring designs
around C++ idioms (RAII, views, `optional`, `Result<T>`) rather than transliterating."

---

## §8. Post-Merge Follow-up PRs (roadmap for reviewers who ask)

| Follow-up | Priority | Effort | Dependency |
|-----------|----------|--------|------------|
| `VariantPath` convenience class | Low | ~200 LOC | None |
| Object/Array recursive sub-schema shredding | Medium | ~300 LOC | None |
| CastOptions cross-type coercion | Medium | ~200 LOC | `arrow_compute` |
| FixedSizeList/ListView as shredding output | Low | ~100 LOC | None |
| Value-absent schemas | Low | ~50 LOC | None |
| JSON serialization | Low | ~500 LOC | Separate concern |
| `variant_get` kernel | Medium | ~300 LOC | Separate concern |
| Parquet reader/writer integration | High | ~1000 LOC | Reader infra |
| Transparent hasher → C++20 or absl | Low | ~10 LOC | Arrow C++20 adoption |
