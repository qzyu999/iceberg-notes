# PR Description Update v2 + Review Comment Replies + Push Plan

> Date: 2026-06-26
> Context: Option D rebuild complete. All 3 branches are single-commit, clean-split.
> Previous version: `cpp_refactor_pr_desc_update.md` (Option A/B/C/D analysis — now resolved via Option D)

---

## Push Plan

### Force-push commands (from arrow repo root)

```bash
# Verify current state
git log --oneline variant-decoding variant-encoding variant-shredding-impl -n1 --no-walk
# Should show:
#   f2db415fe9 (variant-decoding) GH-45946: [C++][Parquet] Variant decoding
#   1e3e1cbafc (variant-encoding) GH-45947: [C++][Parquet] Variant encoding with RAII builders
#   5274e657f7 (variant-shredding-impl) GH-45948: [C++][Parquet] Variant shredding

# Force-push all three (order doesn't matter for push, but merges must be sequential)
git push origin variant-decoding --force-with-lease
git push origin variant-encoding --force-with-lease
git push origin variant-shredding-impl --force-with-lease
```

### After push: Update PR descriptions

Use `gh pr edit` (GitHub CLI):

```bash
# PR #50121 (decoding)
gh pr edit 50121 --body-file /tmp/pr_50121_body.md

# PR #50122 (encoding)
gh pr edit 50122 --body-file /tmp/pr_50122_body.md

# PR #50232 (shredding)
gh pr edit 50232 --body-file /tmp/pr_50232_body.md
```

Or update via the GitHub web UI (Edit button on each PR).

### After push: Reply to reviewer comments

Post the replies from §3 below on the respective PRs.

---

## §1. PR Descriptions (updated for refactored state)

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
  input (validates all offsets, field IDs, nesting depth).
- **Shared internal utility** (`variant_internal_util.h`): endian-safe ReadLE helpers
  used by both decoding and shredding implementations.

Design decisions:
- Parse once, query many (views pre-parse headers, subsequent access is O(1))
- Zero-copy (`string_view` into source buffers, no heap allocation for reads)
- Recursion depth limit (`kMaxNestingDepth = 128`) — security hardening for C++ stack
- Binary search always — no threshold heuristic (pre-parsed header makes it optimal for all n)
- `std::optional` for not-found semantics (idiomatic C++)
- Validated factories (`Make()`) ensure bounds-safe subsequent access

### Are these changes tested?

134 variant-specific tests pass with `BUILD_WARNING_LEVEL=CHECKIN` covering: all 21
primitive types, short/long strings, objects (including 3-byte offsets), arrays
(including is_large), nesting, depth limits, metadata edge cases, error paths,
view API, numeric coercion, and recursive validation.

### Are there any user-facing changes?

New public API in `arrow/extension/variant.h`: `VariantView`, `VariantObjectView`,
`VariantArrayView`, `VariantVisitor`, `VariantMetadata`, `DecodeMetadata`,
`FindMetadataKey`, `ValueSize`, `ValidateVariant`, and associated types/enums.
All in namespace `arrow::extension::variant`.
```

---

### PR #50122 — GH-45947: [C++][Parquet] Variant encoding with RAII builders

```markdown
### Rationale for this change

Implements Variant binary encoding (the write side of decoding from [GH-45946](https://github.com/apache/arrow/issues/45946)). Part of [GH-45937](https://github.com/apache/arrow/issues/45937) (Add variant support to C++). Depends on #50121.

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

Design decisions:
- Move-only (no accidental copies of builder state)
- RAII rollback matches C++ idiom for transactional operations
- Low-level API (`Offset`/`NextField`/`FinishObject`) retained for shredding internals
- Dictionary preserved across `Finish()` — amortizes key lookup for repeated schemas

### Are these changes tested?

221 total tests (87 new encoder + 134 decoder) pass with `BUILD_WARNING_LEVEL=CHECKIN`
covering: all primitives, auto-sizing, int boundaries, short/long string boundary,
special floats (NaN, ±Inf), arrays, objects, duplicate rejection, sorting, RAII scopes,
reset/reuse, builder from existing metadata, large containers, round-trip via decoder.

### Are there any user-facing changes?

New public API: `VariantBuilder`, `ObjectScope`, `ListScope` in `variant.h`.
`VariantBuilder::EncodedVariant` return type from `Finish()`.
```

---

### PR #50232 — GH-45948: [C++][Parquet] Variant shredding

```markdown
### Rationale for this change

Implements variant shredding/unshredding for C++ ([GH-45948](https://github.com/apache/arrow/issues/45948)), part of the [GH-45937](https://github.com/apache/arrow/issues/45937) umbrella. Enables decomposing variant binary columns into native typed Arrow columns for Parquet statistics-based predicate pushdown. Depends on #50121 (decoding) and #50122 (encoding).

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
`ReconstructVariantColumn` for SQL NULL disambiguation.

**Known gaps (documented TODOs for follow-up PRs):**
- Recursive Object/Array sub-schema shredding in object fields (primitives only currently)
- CastOptions cross-type coercion (Uint, Float16, Decimal32/64, TimestampSecond/Milli)
- FixedSizeList/ListView as shredding output targets (reconstruction accepts all)
- Value-absent schemas (`{metadata, typed_value}` without `value`)
- DECIMAL256 shredding target (compatibility check exists but shred/reconstruct path not wired)

### Are these changes tested?

335 total tests (114 new shredding + 221 prior) pass with `BUILD_WARNING_LEVEL=CHECKIN`
covering: schema definition, type compatibility, primitive round-trip for all supported
types, object shredding (full/partial/fallback), array shredding (recursive elements),
typed round-trip (Decimal128, UUID, all timestamps, Float→Double, Int8/Int16, LargeString,
LargeBinary), all list-like reconstruction, error cases, and NullBitmap semantics.

### Are there any user-facing changes?

New public API in `arrow/extension/variant_shredding.h`: `VariantShreddingSchema`,
`IsVariantCompatibleWithType()`, `ShredVariantColumn()`, `ReconstructVariantColumn()`.
New methods on `VariantBuilder`: `BuildWithoutMeta()`, `UnsafeAppendEncoded()`,
`SetAllowDuplicates()`.
```

---

## §2. Force-Push Comment (post on each PR after force-push)

Post this as a comment on all 3 PRs right after the force-push so reviewers understand
what happened:

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

All 134 variant-specific tests pass with `BUILD_WARNING_LEVEL=CHECKIN`.
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

All 221 tests pass with `BUILD_WARNING_LEVEL=CHECKIN`.
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

335 tests pass with `BUILD_WARNING_LEVEL=CHECKIN`.
```

---

## §3. Replies to Reviewer Comments

### PR #50121 (Decoding) — Individual Comment Replies

---

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

---

#### Reply to Comment #2: "§3 references — link to spec"

> Fixed. All enum comments now reference the canonical spec location:
> `https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types`

---

#### Reply to Comment #3: "Rename file — 'internal' confusing"

> Agreed — the naming was confusing. In the refactored layout:
> - **`variant.h`** — the main public API (views, builder, visitor, types). Clear name.
> - **`variant_internal_util.h`** — a small (~71 line) file with shared `ReadLE` utilities
>   used by `variant.cc` and `variant_shredding.cc`. Genuinely internal (not installed),
>   and the "internal" in the name is now accurate since it's excluded from
>   `install_headers()` in both CMake and Meson.
>
> The original `variant_internal.h` that contained the full public API (confusingly named)
> no longer exists.

---

#### Reply to Comment #4: "Add nested navigation test"

> Added. The refactored view classes support composable navigation:
> ```cpp
> auto obj = view.as_object();
> auto inner = obj->get("address")->as_object();
> auto city = inner->get("city")->as_string();
> ```
> Tests exercise this chaining pattern with multi-level nesting (object → object → value,
> object → array → value, etc.). The `VariantObjectView::get()` returns
> `std::optional<VariantView>` which chains naturally with C++ optional patterns.

---

#### Reply to Comment #5: "DecodeValueAt should be public"

> In the refactored design, this use case is covered by
> `VariantView::Make(metadata, data + offset, size)` — you can construct a view at any
> byte offset within a buffer. There's no separate `DecodeValueAt` because the view
> factory IS the decode-at-offset operation. The `Make()` factory validates the buffer
> and returns a fully-functional view ready for typed access.
>
> For object fields specifically, `VariantObjectView::locate(name)` returns an
> `optional<FieldLocation>` with offset + size without constructing the inner view,
> which is useful for zero-copy byte transfer (used by the shredding path).

---

#### Reply to Comment #6: "Plan for shredded variant reading?"

> Implemented in [PR #50232](https://github.com/apache/arrow/pull/50232) (the shredding
> PR in this stack). `ReconstructVariantColumn()` handles the "unshredding" path —
> reassembling typed Parquet columns back into variant binary. The shredding schema
> definition (`VariantShreddingSchema`) supports Primitive, Object, and Array targets.
>
> The `VariantExtensionType` in `parquet_variant.h` has a TODO for tracking the
> shredded_value field when the Parquet reader integration lands. For now, shredding
> operates on raw arrays externally from the extension type.

---

### PR #50122 (Encoding) — Individual Comment Replies

---

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

---

#### Reply to Comment #8: "Initialize builder from existing buffer"

> Good question. The variant binary format is immutable by design — inserting a field into
> an existing object requires rewriting the header (field IDs and offsets are packed
> arrays, not linked structures). So "modify in place" isn't feasible at the format level.
>
> The refactored design handles the "start from existing data" use case through two
> mechanisms:
>
> 1. **`VariantBuilder(const VariantMetadata& existing_metadata)`** — constructor that
>    pre-populates the key dictionary from an existing metadata buffer. This avoids
>    redundant hash-map insertions when rebuilding objects with the same field names.
>
> 2. **Read→rebuild pattern** — `VariantObjectView` (read existing) + `ObjectScope` +
>    `UnsafeAppendEncoded` (write new). This enables zero-copy field transfer between
>    variants. It's exactly the pattern the shredding reconstruction path uses for
>    merging shredded fields back with residual objects.
>
> This separation of "views for reading" and "builders for writing" is the standard
> immutable-format pattern in C++ (similar to FlatBuffers, Cap'n Proto, etc.) and matches
> Rust's `Variant` (read-only) vs `VariantBuilder` (write-only) architecture.

---

#### Reply to Comment #9: "API for modifying existing variants / move context"

> The refactored design makes this separation explicit and deliberate:
>
> - **Views** (`VariantView`, `VariantObjectView`, `VariantArrayView`) — zero-copy
>   navigation of existing bytes. Stack-allocated, no heap allocation.
> - **Builder** (`VariantBuilder` + RAII scopes) — produces new bytes from scratch.
>   Dictionary preserved across `Finish()` calls for efficiency.
>
> "Modify existing" is expressed as: read the parts you want to keep via views, write
> them into a new builder with `UnsafeAppendEncoded`, add/change what you need, finish.
> This is explicit, predictable, and avoids the complexity of a mutable DOM with COW
> semantics.
>
> A higher-level mutable convenience API (think `nlohmann::json`-style) could absolutely
> be layered on top of these primitives in a follow-up — it would use views internally
> for reading and a builder for producing the result. For the first implementation,
> keeping the primitives clean and composable felt like the right foundation.

---

## §4. Order of Operations (checklist)

```
[ ] 1. Force-push all 3 branches:
       git push origin variant-decoding --force-with-lease
       git push origin variant-encoding --force-with-lease
       git push origin variant-shredding-impl --force-with-lease

[ ] 2. Update PR #50121 description (copy from §1 above)
[ ] 3. Update PR #50122 description (copy from §1 above)
[ ] 4. Update PR #50232 description (copy from §1 above)

[ ] 5. Post force-push comment on PR #50121 (from §2)
[ ] 6. Post force-push comment on PR #50122 (from §2)
[ ] 7. Post force-push comment on PR #50232 (from §2)

[ ] 8. Reply to Comment #1 on PR #50121 (threshold)
[ ] 9. Reply to Comment #2 on PR #50121 (spec refs)
[ ] 10. Reply to Comment #3 on PR #50121 (file naming)
[ ] 11. Reply to Comment #4 on PR #50121 (nested nav test)
[ ] 12. Reply to Comment #5 on PR #50121 (DecodeValueAt)
[ ] 13. Reply to Comment #6 on PR #50121 (shredded reading plan)

[ ] 14. Reply to Comment #7 on PR #50122 (metadata type mismatch)
[ ] 15. Reply to Comment #8 on PR #50122 (init from buffer)
[ ] 16. Reply to Comment #9 on PR #50122 (modify existing)
```

---

## §5. Notes on Tone

The replies are structured to be:

- **Open-minded and flexible** — acknowledging the feedback was valuable and drove real
  improvements (it genuinely did — the threshold removal, view class design, and RAII
  scopes all came from reflecting on what the reviewers were really asking for)
- **Truthful** — the original implementation DID follow Go's patterns too closely, and
  the refactoring DID fix the issues raised
- **Technical but not defensive** — explaining WHY certain things work the way they do
  without dismissing the reviewer's perspective
- **Forward-looking** — mentioning follow-up possibilities (DOM API, fuzz targets) shows
  awareness of the broader roadmap without overcommitting in this PR

The common thread: "Review feedback made me realize I was carrying over patterns from
Go/Rust without questioning whether they were the right fit for C++. The refactoring
addresses this by designing around C++ idioms (RAII, views, `optional`, `Result<T>`)
rather than transliterating from another language."
