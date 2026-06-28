# GH-45948: [C++][Parquet] Variant shredding

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-06-20 22:30:21.000 UTC

### Rationale for this change

Implements variant shredding/unshredding for C++ (GH-45948), part of the [GH-45937](https://github.com/apache/arrow/issues/45937) umbrella. This enables decomposing variant binary columns into native typed Arrow columns for Parquet statistics-based predicate pushdown.

Depends on #50121 (decoding) and #50122 (encoding).

### What changes are included in this PR?

Adds `variant_shredding.h/.cc` implementing:
- `VariantShreddingSchema` — schema definition for shredding targets (Primitive, Object, Array)
- `IsVariantCompatibleWithType()` — strict type compatibility checking
- `ShredVariantColumn()` — decomposes variant binary into native typed columns
- `ReconstructVariantColumn()` — reassembles shredded columns back to variant binary

Extends `VariantBuilder` with `BuildWithoutMeta()`, `UnsafeAppendEncoded()`, and `SetAllowDuplicates()` (required by the shredding/reconstruction paths).

Updates `VariantExtensionType` to accept shredded storage layouts.

**Rust parity** with `parquet-variant-compute` (`shred_variant.rs` / `unshred_variant.rs`):
- All primitive shredding targets (Bool, Int8–64, Float32/64, String/LargeString/StringView, Binary/LargeBinary/BinaryView, Date32, Timestamp, Time64, UUID, Decimal128)
- Object field routing with recursive native sub-field extraction
- Recursive array element shredding
- Reconstruction from all list-like typed_value types
- Variant::Null → value column (not typed_value)

**Known gaps (follow-up work):**
- NullBuffer return for SQL NULL disambiguation (currently uses both-null + struct validity bitmap)
- CastOptions cross-type coercion (Uint types, Float16, Decimal32/64, TimestampSecond/Milli)
- Value-absent shredded schemas (`{metadata, typed_value}` without `value`)
- Recursive Object/Array sub-schemas in object field shredding (only Primitive sub-schemas get native extraction currently)

### Are these changes tested?

319 tests pass with `BUILD_WARNING_LEVEL=CHECKIN` covering schema definition, type compatibility, round-trip shredding for all supported types, error cases, and builder extensions.

### Are there any user-facing changes?

New public API: `VariantShreddingSchema`, `IsVariantCompatibleWithType()`, `ShredVariantColumn()`, `ReconstructVariantColumn()` in `variant_shredding.h`. New methods on `VariantBuilder`: `BuildWithoutMeta()`, `UnsafeAppendEncoded()`, `SetAllowDuplicates()`.
* GitHub Issue: #45948

