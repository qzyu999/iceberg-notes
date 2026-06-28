# Review Analysis: misiek1984's Comments on PRs #50121 and #50122

> Principal engineer analysis — 2026-06-22
> Reviewer: @misiek1984
> PRs: apache/arrow#50121 (decoding), apache/arrow#50122 (encoding)

---

## PR #50121 — Variant Decoding

### 1. Threshold of 32 for binary search vs linear scan

**Comment:** "How was the 32 threshold determined?"

**Location:** `variant_internal.h`, `FindObjectField` doc comment

**Assessment: Valid question — should be addressed with a response.**

The threshold of 32 is directly taken from **arrow-go** which uses `const binarySearchThreshold = 32` in `variant.go:367` with the comment "if total list size is smaller than threshold, linear search will likely be faster than a binary search." The C++ mirrors this exactly: `constexpr int32_t kBinarySearchThreshold = 32`.

**Rust (arrow-rs) takes a different approach:**
- **Object field lookup** (`object.rs`): Always binary search, no threshold — since field IDs are spec-guaranteed sorted, Rust just binary searches unconditionally.
- **Metadata dictionary lookup** (`metadata.rs`): Uses a threshold of **10** (`self.len() > 10`), and only binary searches when the dictionary is both sorted AND larger than 10 entries.

The rationale for a threshold at all: binary search has overhead from string comparisons via metadata dictionary resolution (two indirections: field_id → dictionary index → strcmp). For small N, linear scan with sequential memory access and branch-prediction-friendly iteration wins. The crossover point for pointer-chasing binary search vs cache-friendly linear scan on modern CPUs is typically in the 16–64 range. 32 is a reasonable middle ground (Go's choice, empirically).

**Action:** Reply citing Go's `binarySearchThreshold = 32` as the source. Consider adding a brief comment in code: `// Matches Go's binarySearchThreshold; empirically, dictionary indirection + strcmp makes binary search slower than linear scan below this size.` Note the Rust divergence for context — Rust can afford always-binary-search because its `try_binary_search_range_by` doesn't need dictionary indirection for the object's sorted field IDs (it resolves names inline via `field_name(i)`).

No code change strictly required, but a one-line comment citing Go would preempt the question.

---

### 2. Spec section references (§3, §3.1 are outdated)

**Comment:** "The current version of the spec does not contain paragraph §3 and §3.1. I would just add a link to the section with tables."

**Location:** `variant_internal.h`, `BasicType` enum doc

**Assessment: Correct — fix required.**

The spec's markdown headings have changed. The current spec uses `## Encoding Types` as the relevant section header, not numbered paragraphs. The reviewer's suggestion to link directly to the section anchor (`VariantEncoding.md#encoding-types`) is the right approach.

**Action:** Replace `Variant Encoding Spec §3: "Value encoding"` with a direct link:
```
/// See: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
```
Do this for all spec section references that use the `§N` notation.

---

### 3. File naming — `variant_internal` is confusing

**Comment:** "Maybe instead of explaining in the comment what 'internal' means it would be better to rename a file e.g. to `variant_binary_encoding`, `variant_internal_encoding` etc."

**Location:** `variant_internal.h` header docblock

**Assessment: Reasonable suggestion but probably not worth the churn.**

The name `variant_internal` was chosen because it deals with the internal binary encoding format (as opposed to the extension type registration in `parquet_variant.h`). Renaming would touch CMakeLists, meson.build, all `#include` directives across 5+ files, and the installed header path — which is a public API surface (downstream code `#include "arrow/extension/variant_internal.h"`).

The comment already explains the naming. Arrow has precedent for "_internal" suffixed headers that ARE installed (they mean "implementation details of a concept" not "private header"). Examples exist in the Arrow codebase.

**Action:** Reply explaining the rationale and Arrow's convention. Acknowledge it's a valid point but argue the rename isn't worth the API surface change at this stage. If the reviewer feels strongly, offer to file a follow-up issue for potential rename before the first release that includes this code (since it hasn't shipped yet, a rename is still feasible before ABI stability commitments).

---

### 4. More integration tests with nested navigation

**Comment:** Wants a test showing multi-level nested field navigation: find "addresses" → find "postal" → find "city" → read value.

**Location:** `variant_internal_test.cc`, `FullRoundTrip` test

**Assessment: Good suggestion — adds real-world usage documentation via tests.**

The existing `FullRoundTrip` test demonstrates the API but uses a shallow object. A deeper test exercising the `FindObjectField` → `FindObjectField` → read pattern would serve as both validation and documentation for future users of the API.

**Action:** Add a test like:
```cpp
TEST_F(VariantIntegrationTest, NestedFieldNavigation) {
  // Build: {"name": "Alice", "age": 30, "addresses": {"postal": {"country": "USA", "city": "New York"}, ...}}
  // Navigate: FindObjectField("addresses") → FindObjectField("postal") → FindObjectField("city") → decode
}
```
This is a straightforward addition. It exercises the intended usage pattern and costs nothing in maintenance.

---

### 5. `DecodeValueAt` should be public

**Comment:** "I think this function should be public. If I want to read a specific nested field using a path (field_1.field_2.field_3), I need to implement the last step myself because `DecodeValueAt` is not public."

**Location:** `variant_internal.cc`, `DecodeValueAt` function

**Assessment: The reviewer has a valid use case, but the proposed solution needs nuance.**

The reviewer's workflow is:
1. `FindObjectField("field_1")` → get offset+size
2. `FindObjectField("field_2")` on that sub-object → get offset+size
3. Now has raw bytes of the leaf value but can't decode them without re-parsing from root

Currently, `DecodeVariantValue` takes the full value buffer and always starts from the root. The reviewer wants to decode from an arbitrary offset into the buffer.

However, `DecodeValueAt` takes a raw `const uint8_t*` pointer + length, which is essentially the same signature as `DecodeVariantValue` but without the `string_view` wrapper. The user CAN already achieve this by calling `DecodeVariantValue(metadata, {data + offset, size}, visitor)` — the function operates on any valid variant value bytes, not just the root.

**Action:** Two options:
1. **Preferred:** Reply clarifying that `DecodeVariantValue` already works on sub-values — the data/length returned by `FindObjectField` can be passed directly as a new `string_view` to `DecodeVariantValue`. Add a code example in the reply showing the pattern.
2. **Alternative:** If the reviewer's concern is about the function not being obviously usable this way, add a brief doc note to `DecodeVariantValue` stating it can decode any sub-value, not just root-level values.

No API change needed — this is a documentation/education gap, not a functionality gap.

---

### 6. Plan for reading/decoding shredded variants?

**Comment:** "Do you have a plan to also support reading/decoding shredded variants?"

**Location:** `variant_internal.h`, `DecodeVariantValue` doc

**Assessment: Good forward-looking question — answer with the full picture.**

The shredding PR (#50232) already implements `ReconstructVariantColumn()` which takes shredded columns and produces the variant binary that `DecodeVariantValue` can then consume. The workflow is: shredded columns → `ReconstructVariantColumn()` → variant binary → `DecodeVariantValue()`.

There is no plan for a "decode directly from shredded representation" API because that would bypass the variant binary format entirely — at that point you'd just read the native typed Arrow columns directly (which is the whole point of shredding: avoiding the decode step for pushdown-eligible data).

**Action:** Reply explaining the shredding PR (#50232) and the architectural separation: decoding operates on variant binary; shredding/unshredding converts between binary and native typed columns. Link to #50232 for full context.

---

## PR #50122 — Variant Encoding

### 7. Test for metadata/data type mismatch in `BuildFromExistingMetadata`

**Comment:** "I would add a test where we reuse existing metadata but make a mistake with the data types. For example, according to the metadata, we should write a string but we write an integer instead."

**Location:** `variant_builder_test.cc`, existing metadata reuse test

**Assessment: Insightful — but the premise reveals a misunderstanding of the format that should be clarified.**

The variant metadata dictionary contains only **string keys** (field names), not value types. The metadata is essentially a string intern table for object keys. It has NO relationship to the types of values written. You can reuse metadata from an object `{"name": "Alice"}` and then write `{"name": 42}` — this is perfectly valid because "name" is just a key string in the dictionary.

There is no "according to the metadata, we should write a string" scenario — metadata doesn't dictate value types. The variant format is self-describing: each value carries its own type tag in the value header byte.

However, there IS a valid edge case: using `BuildFromExistingMetadata` with a metadata dictionary, then writing an object with a key that's NOT in the existing dictionary. The builder handles this correctly (adds the new key to the dictionary), but a test demonstrating this would be useful.

**Action:** Reply explaining that metadata is a key-name dictionary (not a schema), so type mismatches are architecturally impossible. Offer to add a test showing what happens when new keys are added beyond the existing metadata (the builder grows the dictionary). This clarifies the format for the reviewer.

---

### 8. Initialize builder from existing value buffer

**Comment:** "It might also be useful to pass a value buffer to VariantBuilder to initialize `buffer_`. This way, it will be possible to continue building an existing Variant value."

**Location:** `variant_internal.h`, `VariantBuilder` class

**Assessment: Interesting idea but architecturally incompatible with the format.**

The variant format is not appendable. An encoded object is a fixed-size header + field_ids + offsets + concatenated values. You can't "continue building" a finished object because inserting a new field requires rewriting the field_id array, offset array, and header (which contains num_fields). Same for arrays.

The correct pattern for "modify an existing variant" is: decode → reconstruct in builder → modify → re-encode. This is the approach both Go and Rust take.

The `BuildFromExistingMetadata` constructor already optimizes the common case: you reuse the dictionary (avoiding re-interning keys) but build a fresh value from scratch.

**Action:** Reply explaining why the format doesn't support incremental append/modification, and that the decode→rebuild pattern is the intended workflow. This is a format constraint, not an implementation limitation. Mention that `UnsafeAppendEncoded()` (coming in #50232) partially addresses the "copy existing sub-values" case by allowing zero-copy insertion of pre-encoded variant bytes into a new container.

---

### 9. API for modifying existing Variant values (move context to a field)

**Comment:** "Did you also consider adding an API that allows modifying existing Variant values? We would need to add a function to VariantBuilder similar to FindObjectField, which would 'move' the context of VariantBuilder to a specific place/field."

**Location:** `variant_internal.h`, `VariantBuilder` class

**Assessment: Valid use case, but wrong layer for it — this is a higher-level "Variant DOM" concept.**

This describes a mutable DOM/cursor API: navigate to a path, replace the value in-place. This is fundamentally at odds with how the binary format works (immutable, size-prefixed, no in-place mutation), and it would require significant complexity:

1. Parse the existing value into a tree structure (DOM)
2. Navigate to the target field
3. Replace the value
4. Re-serialize the entire structure

This is a **higher-level library** built ON TOP of the encoder/decoder, not something that belongs in the low-level builder. The builder's job is to produce bytes efficiently for write paths. A "VariantEditor" or "MutableVariant" class could be a follow-up if there's demand.

**Action:** Reply acknowledging the use case, explain it would require a higher-level mutable DOM abstraction (not suitable for the encoder PR), and suggest it as a potential follow-up feature. Note that for the current Parquet write path, the pattern is always "construct from scratch" since you're materializing column data, not patching individual values.

---

## Summary of Required Actions

| # | PR | Action | Priority |
|---|------|--------|----------|
| 1 | 50121 | Reply with threshold rationale + optional code comment | Low |
| 2 | 50121 | Fix spec section references (§3 → direct link) | **High** (correctness) |
| 3 | 50121 | Reply explaining naming convention, offer follow-up issue | Low |
| 4 | 50121 | Add nested navigation integration test | Medium |
| 5 | 50121 | Reply clarifying `DecodeVariantValue` works on sub-values | Medium |
| 6 | 50121 | Reply explaining shredding architecture + link #50232 | Low |
| 7 | 50122 | Reply clarifying metadata is key-dictionary, not schema | Medium |
| 8 | 50122 | Reply explaining format is not appendable | Low |
| 9 | 50122 | Reply acknowledging use case, suggest higher-level follow-up | Low |

**Code changes needed:** Items 2 and 4 only. The rest are reply-only.

**Recommended order:** Address #50121 first (it's the base of the chain), then #50122.
