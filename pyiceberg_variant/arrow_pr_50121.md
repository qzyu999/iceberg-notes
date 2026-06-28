# GH-45946: [C++][Parquet] Variant decoding

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-06-08 04:50:07.000 UTC

### Rationale for this change
This is part of the GH-45937 umbrella (Add variant support to C++ Parquet). The [Variant Encoding Spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) defines a binary format for semi-structured data in Parquet. This PR adds the decoding (reading) side, which is a prerequisite for the encoder (GH-45947) and shredding support (GH-45948).

The implementation targets feature parity with the existing [arrow-go `parquet/variant` package](https://github.com/apache/arrow-go/tree/main/parquet/variant), adapted to idiomatic C++ patterns. Divergences from Go are deliberate and documented in code comments.

### What changes are included in this PR?
Full Variant binary decoding per the Variant Encoding Spec. Adds `variant_internal.h/.cc` with:

**Decoder (visitor/SAX-style traversal):**
- `DecodeMetadata()` — parses the string dictionary from raw bytes
- `DecodeVariantValue()` — recursive traversal invoking a `VariantVisitor` for each value
- All 21 primitive types, short strings, objects (with non-monotonic offset support), arrays
- Recursion depth limit (`kMaxNestingDepth = 128`) — security hardening for C++ stack semantics (Go doesn't need this due to goroutine stack growth)

**Random-access utilities (for future Parquet reader integration):**
- `ValueSize()` — compute byte size of a value without full decode
- `FindObjectField()` — lookup by field name (binary search for ≥32 fields, linear for small objects)
- `GetArrayElement()` — O(1) element access by index
- `GetObjectFieldAt()` — positional field access
- `FindMetadataKey()` — dictionary ID lookup (binary search if sorted)

**Design choices (deliberate divergences from Go documented in code):**
- Visitor pattern (SAX-style) — idiomatic Arrow C++ (`TypeVisitor`, `ArrayVisitor` precedent)
- Reserved bit 5 enforcement in metadata header (Go does not check; we fail cleanly on future spec versions)
- Object field offset bounds validation (Go does not check; defense-in-depth against malformed input)
- No UTF-8 validation during decode (matches Go; documented for future follow-up)
- `FindObjectField` binary search uses signed `int32_t` for `lo`/`hi` to avoid an unsigned underflow pattern present in Go's `ObjectValue.ValueByKey()` (separate bug report TBD)

**Bug discovered in arrow-go during development:**
- `valueSize()` in `parquet/variant/utils.go` reads the wrong bit for array `is_large` — uses `(typeInfo >> 4) & 0x1` (object's is_large position) instead of `(typeInfo >> 2) & 0x1`. Fix submitted as apache/arrow-go#839.
- `ObjectValue.ValueByKey()` binary search uses `j = mid - 1` where `j` is `uint32` — wraps to `MaxUint32` when `mid == 0`, skipping elements. Reported as apache/arrow-go#842.

### Are these changes tested?
Yes. 165 tests pass with `BUILD_WARNING_LEVEL=CHECKIN` (warnings-as-errors):
- Metadata parsing (15 tests including error cases, all offset sizes 1-4, reserved bit rejection)
- All primitive types + boundary values (21 tests)
- Short strings (4 tests)
- Objects (5 tests including 3-byte offset/id sizes, non-monotonic offsets)
- Arrays + is_large (4 tests)
- Nesting + depth limit (5 tests)
- Visitor early abort propagation (2 tests)
- Spec-conformance with handcrafted byte sequences (6 tests)
- ValueSize including regression test for the Go bug (9 tests)
- Random access: FindObjectField, GetArrayElement, GetObjectFieldAt (8 tests)
- FindMetadataKey sorted/unsorted (4 tests)
- Binary search path for large objects ≥32 fields (4 tests)
- Error cases: type mismatches, truncation, offset overflows, negative indices (8 tests)

### Are there any user-facing changes?
No breaking changes. This adds new public API (`arrow::extension::variant` namespace) that did not previously exist. The header `variant_internal.h` is installed — "internal" in the filename refers to "binary encoding internals" not visibility.

**Follow-up:** The encoder PR (#45947) is stacked on this branch and should be reviewed/merged after this one.

**AI Disclosure:** AI coding assistants were used during development for scaffolding, test generation, and review iteration. All code has been reviewed, debugged, and verified by the author who owns and understands the changes.
* GitHub Issue: #45946

