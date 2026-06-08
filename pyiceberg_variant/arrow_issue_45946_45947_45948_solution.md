# Combined Solution: Variant Decoding, Encoding, and Shredding

> **Issues**: GH-45946 (decoding) → GH-45947 (encoding) → GH-45948 (shredding)
> **Umbrella**: GH-45937 [C++][Parquet] Add variant support
> **Specs**: [VariantEncoding.md](VariantEncoding.md) + [VariantShredding.md](VariantShredding.md)
> **Reference**: Go implementation in `apache/arrow-go` (`parquet/variant/`)

---

## Merge Order

```
GH-45946 (decoding) ──► GH-45947 (encoding) ──► GH-45948 (shredding)
     │                        │                        │
     ▼                        ▼                        ▼
  variant-decoding       variant-encoding        variant-shredding
  (pushed, PR-ready)    (pushed, PR-ready)       (branch created)
```

Each PR depends on the previous. Shredding depends on both encoder and decoder.

---

## PR 1: GH-45946 — Variant Decoding ✅ COMPLETE

**Branch**: `variant-decoding` → `qzyu999/arrow`
**Commit**: `GH-45946: [C++][Parquet] Variant decoding`
**Tests**: 87 passing

### What It Implements (from VariantEncoding.md)

| Spec Section | Implementation |
|---|---|
| Metadata encoding (§2) | `DecodeMetadata()` — parses header, dictionary, validates offsets |
| Value encoding (§3) — all basic types | `DecodeVariantValue()` — visitor-based recursive traversal |
| Primitive types (IDs 0-20) | All 21 types decoded correctly |
| Short string (basic_type=1) | Decoded via `DecodeShortString()` |
| Object (basic_type=2) | `DecodeObject()` with correct bit layout |
| Array (basic_type=3) | `DecodeArray()` with correct is_large (bit 2 of value_header) |
| Object field ordering | `FindObjectField()` — binary search on lex-sorted field IDs |
| Value size computation | `ValueSize()` — computes byte size without full decode |
| Random access | `GetArrayElement()`, `GetObjectFieldAt()` |
| Dictionary lookup | `FindMetadataKey()` — binary search if sorted |

### Files

| File | Lines | Purpose |
|---|---|---|
| `variant_internal.h` | ~340 | All declarations: enums, structs, decoder API, visitor, random access |
| `variant_internal.cc` | ~970 | All decoder implementations |
| `variant_internal_test.cc` | ~1540 | 87 tests |
| Build files (4) | +6 lines | CMake + Meson registration |

---

## PR 2: GH-45947 — Variant Encoding ✅ COMPLETE

**Branch**: `variant-encoding` → `qzyu999/arrow`
**Commit**: `GH-45947: [C++][Parquet] Variant encoding`
**Tests**: 116 total (87 decoder + 29 encoder)

### What It Implements (from VariantEncoding.md)

| Spec Section | Implementation |
|---|---|
| Metadata encoding grammar | `Finish()` — builds metadata buffer with correct header, offsets, sorted flag |
| Object header construction | `FinishObject()` — correct bit layout, field ID sorting, duplicate rejection |
| Array header construction | `FinishArray()` — correct is_large at bit 4, offset optimization |
| All primitive encoding | `Null/Bool/Int/Float/Double/Date/Timestamp*/Decimal*/String/Binary/UUID` |
| Short string optimization | `String()` — auto uses short encoding for ≤63 bytes |
| Integer auto-sizing | `Int()` — selects smallest int type (int8/16/32/64) |
| is_large flag | Automatic for >255 elements |
| Offset size optimization | `IntSize()` — minimum 1-4 bytes |

### Files

| File | Lines | Purpose |
|---|---|---|
| `variant_internal.h` | +100 (additions) | `VariantBuilder` class declaration + TODOs for GH-45948 |
| `variant_builder.cc` | ~290 | Builder implementation |
| `variant_builder_test.cc` | ~330 | 29 round-trip + encoder-specific tests |
| Build files (4) | +4 lines | CMake + Meson registration |

### TODOs Left for GH-45948

```cpp
// TODO GH-45948: Add BuildWithoutMeta() to return raw value bytes without
// metadata, needed for shredded variant encoding.

// TODO GH-45948: Add UnsafeAppendEncoded(const uint8_t* data, int64_t size)
// to append pre-encoded variant value bytes for composition/shredding.

// TODO GH-45948: Add SetAllowDuplicates(bool) for duplicate key tolerance
// with last-value-wins semantics (uses ValueSize for compaction).
```

---

## PR 3: GH-45948 — Variant Shredding 🔜 PLANNED

**Branch**: `variant-shredding` (created, empty)
**Depends on**: Both decoder and encoder PRs merged

### What It Must Implement (from VariantShredding.md)

The shredding spec defines how Variant values are decomposed into typed Parquet columns for query optimization. This requires:

#### A. Schema-Level Changes to `VariantExtensionType`

The existing `parquet_variant.h` has:
```cpp
// TODO GH-45948 added shredded_value
std::shared_ptr<Field> metadata_;
std::shared_ptr<Field> value_;
```

Shredded variants expand the storage type from:
```
Struct<metadata: Binary, value: Binary>
```
to:
```
Struct<metadata: Binary, value: Optional<Binary>, typed_value: Optional<T>>
```

**Changes needed**:
- `IsSupportedStorageType()` must accept the shredded layout (optional value + typed_value fields)
- `VariantExtensionType` must store the optional `typed_value` field
- Schema conversion in `parquet/arrow/schema.cc` must handle shredded Parquet groups

#### B. Shredding (Write Path)

Decompose a Variant value into:
- `metadata` — the full dictionary (includes all field names, shredded or not)
- `value` — the "remainder" object with shredded fields removed (or null if fully shredded)
- `typed_value` — the extracted typed column(s)

**Algorithm** (per the spec's `construct_variant` pseudocode, but in reverse):
1. Given a Variant object and a shredding schema (which fields to extract)
2. For each shredded field:
   - If the field exists and matches the typed column's type → write to `typed_value`, set `value` to null for that field
   - If the field exists but doesn't match → write to `value` as variant binary
   - If the field doesn't exist → both null (missing)
3. Write remaining non-shredded fields into the `value` binary column

**Builder features needed** (from TODOs):
- `BuildWithoutMeta()` — get the "remainder" variant bytes for partial objects
- `UnsafeAppendEncoded()` — compose the remainder from existing encoded fragments
- `SetAllowDuplicates()` — merge shredded + non-shredded fields during reconstruction

#### C. Unshredding (Read Path)

Reconstruct a full Variant value from shredded columns:
1. Read `typed_value` — if non-null and is an object group, recursively reconstruct sub-fields
2. Read `value` — if non-null, decode as Variant
3. Combine per the spec's truth table:
   - Both null → missing
   - value non-null, typed_value null → use value as-is
   - value null, typed_value non-null → convert typed_value to Variant
   - both non-null → partially shredded object (union the fields)

**Decoder features needed**:
- `ValueSize()` — for computing sizes during field extraction ✅ (already implemented)
- `FindObjectField()` — for extracting specific fields ✅ (already implemented)
- Object reconstruction using `VariantBuilder` + `FinishObject()` ✅ (already implemented)

#### D. Parquet Integration

This is where shredding connects to the actual Parquet read/write pipeline:
- **Write**: `parquet/arrow/writer.cc` must know how to produce the shredded column layout
- **Read**: `parquet/arrow/reader.cc` must know how to reconstruct from shredded columns
- **Schema**: `parquet/arrow/schema.cc` must handle the `typed_value` field in schema conversion

### Estimated Scope

| Component | Est. Lines | Effort |
|---|---|---|
| `VariantExtensionType` expansion | ~100 | Medium |
| Shred function (decompose variant into columns) | ~300 | Hard |
| Unshed function (reconstruct from columns) | ~250 | Hard |
| Builder additions (BuildWithoutMeta, UnsafeAppendEncoded, SetAllowDuplicates) | ~150 | Medium |
| Schema conversion updates | ~100 | Medium |
| Parquet reader integration | ~200 | Hard |
| Parquet writer integration | ~200 | Hard |
| Tests | ~500 | Medium |
| **Total** | **~1800** | **4-6 weeks** |

### Key Complexity

Shredding is significantly more complex than encoding/decoding because it:
1. Operates at the **column level** (not individual values) — processing entire record batches
2. Requires **schema negotiation** — deciding which fields to shred at write time
3. Involves **Parquet integration** — wiring into the existing reader/writer pipeline
4. Has **nested recursion** — objects within objects can each have their own typed_value

The VariantEncoding.md spec is a prerequisite for understanding the binary format, but the actual shredding logic lives in VariantShredding.md and operates at a higher level (Parquet column groups, not individual byte buffers).

---

## Summary: What Each Spec Section Maps To

### VariantEncoding.md Coverage

| Section | PR |
|---|---|
| §1 Variant in Parquet | Existing `parquet_variant.h` (GH-46104) |
| §2 Metadata encoding | GH-45946 decoder + GH-45947 encoder |
| §3 Value encoding (all types) | GH-45946 decoder + GH-45947 encoder |
| §4 Encoding types table (21 types) | Both PRs (enums + decode/encode) |
| §5 Object field ID order | GH-45946 (FindObjectField binary search) + GH-45947 (FinishObject sorting) |
| §6 Versions and extensions | GH-45946 (version validation, reserved bit 5) |
| §7 Shredding (pointer to VariantShredding.md) | GH-45948 |

### VariantShredding.md Coverage

| Section | PR |
|---|---|
| Variant Metadata (shared across shredded columns) | GH-45948 (schema changes) |
| Value Shredding (value/typed_value truth table) | GH-45948 (shred/unshed functions) |
| Shredded Value Types (Parquet type mapping) | GH-45948 (schema conversion) |
| Arrays (3-level list shredding) | GH-45948 |
| Objects (field group shredding) | GH-45948 |
| Nesting (recursive shredded objects) | GH-45948 |
| Data Skipping (statistics on typed_value) | Future (Parquet-level optimization) |
| Reconstructing a Shredded Variant (pseudocode) | GH-45948 (unshed function) |

---

## Go Bug Note

Our decoder's `ValueSize()` correctly uses `(type_info >> 2) & 0x01` for the array `is_large` flag. Go's `valueSize()` incorrectly uses `(typeInfo >> 4) & 0x1`. We filed this as a bug on `apache/arrow-go` (issue #839) and submitted a fix PR from `qzyu999/arrow-go:fix-valuesize-array-islarge`.

Our encoder's `FinishArray()` writes `is_large` at bit 4 of the full byte (bit 2 of value_header), consistent with both the spec and our decoder. Round-trip tests confirm correctness.
