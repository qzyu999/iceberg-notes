**Note:** This PR depends on #45946 (Variant decoding) and is branched from it. Please review/merge #45946 first. After it merges, this PR will be retargeted to `main`.

### Rationale for this change

This is part of the GH-45937 umbrella (Add variant support to C++ Parquet). It adds the encoding (writing) side of the Variant binary format, building on the decoder from GH-45946. The encoder is required for GH-45948 (variant shredding) and for any Parquet writer that needs to produce Variant columns.

As with the decoder, the implementation targets feature parity with the [arrow-go `parquet/variant.Builder`](https://github.com/apache/arrow-go/tree/main/parquet/variant), adapted to idiomatic C++ patterns. Divergences are deliberate and documented.

### What changes are included in this PR?
Adds `VariantBuilder` class in `variant_internal.h` / `variant_builder.cc` for encoding Variant binary values per the [Variant Encoding Spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).

**Builder API:**
- All 21 primitive types: `Null()`, `Bool()`, `Int()` (auto-sizes), `Int8/16/32/64()`, `Float()`, `Double()`, `Date()`, `TimestampMicros/NTZ()`, `TimestampNanos/NTZ()`, `TimeNTZ()`, `Decimal4/8/16()`, `String()` (auto short-string for ≤63 bytes), `Binary()`, `UUID()`
- Container construction: `Offset()` / `NextElement()` / `FinishArray()` for arrays, `NextField()` / `FinishObject()` for objects
- `Finish()` — produces encoded metadata + value buffers with sorted-flag detection
- `Reset()` — clears buffer for builder reuse; dictionary preserved across `Finish()` calls
- Constructor from existing `VariantMetadata` for shared-dictionary workflows

**Key design points:**
- Move-only (non-copyable, `noexcept` movable)
- `FinishObject()` sorts fields in-place by key — spec requires field IDs in lexicographic key order
- Strict duplicate key rejection (`Status::Invalid`) — spec says "An object may not contain duplicate keys"; configurable tolerance deferred to GH-45948 with TODO
- `FinishArray()` validates offsets are non-negative
- `Finish()` validates total dictionary size fits in 4-byte offsets
- Decimal scale validation (≤ 38) in encoder; decoder is lenient
- Go enforces a 128MB metadata limit (`metadataMaxSizeLimit`); C++ only enforces the spec's ~4GB 4-byte offset maximum

**TODOs for GH-45948 (shredding):**
```cpp
// TODO GH-45948: Add BuildWithoutMeta() — raw value bytes without metadata
// TODO GH-45948: Add UnsafeAppendEncoded() — append pre-encoded bytes
// TODO GH-45948: Add SetAllowDuplicates(bool) — last-value-wins semantics
```

### Are these changes tested?
Yes. 238 total tests pass with `BUILD_WARNING_LEVEL=CHECKIN` (73 encoder + 165 decoder):
- Primitive round-trips (14 tests including short/long string boundary at 63/64 bytes)
- Int auto-sizing boundaries: Int8→Int16→Int32→Int64 transitions (8 tests)
- Direct int type methods: `Int8/16/32/64` without auto-sizing (4 tests)
- Array round-trips: empty, simple, nested (3 tests)
- Object round-trips: empty, simple, nested, duplicate rejection, field sorting (5 tests)
- Builder features: reset, from-existing-metadata, sorted/unsorted flag (4 tests)
- Integration: complex nested object, large metadata (300 keys), offset-size computation, invalid start, negative offsets (5 tests)
- Special floats: NaN, ±Inf for float and double (6 tests)
- Large containers triggering `is_large` flag: 300-element array + 300-field object (2 tests)
- Decoder utility round-trips through builder output: FindObjectField, GetArrayElement, GetObjectFieldAt, ValueSize (4 tests)
- Builder reuse: dictionary preservation across multiple `Finish()` calls (2 tests)
- Pre-existing buffer: FinishObject/FinishArray with start > 0 (2 tests)
- Decimal scale validation: rejects scale > 38 (1 test)

### Are there any user-facing changes?
No breaking changes. This extends the public API added in GH-45946 with the `VariantBuilder` class in the same `arrow::extension::variant` namespace.

**AI Disclosure:** AI coding assistants were used during development for scaffolding, test generation, and review iteration. All code has been reviewed, debugged, and verified by the author who owns and understands the changes.

* GitHub Issue: #45947