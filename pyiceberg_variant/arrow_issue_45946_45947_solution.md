# Combined Plan: Decoder Additions (#45946) + Encoder (#45947)

> **Goal**: Achieve full parity with the Go reference implementation across both PRs.
> **Strategy**: Add random-access utilities to the decoder branch first, then build the encoder on top.

---

## Part A: Additions to Decoder Branch (`variant-decoding`)

These are read-side features that Go's `variant.go` provides alongside basic decoding. They belong in the decoder PR because they are **decode/read operations**, not encoding.

### A.1 Functions to Add to `variant_internal.h` / `variant_internal.cc`

#### `ValueSize` — Compute byte size of a variant value from its header

Go equivalent: `valueSize()` in `utils.go`

```cpp
/// \brief Compute the total byte size of a variant value (header + data).
///
/// Determines how many bytes a variant value occupies by examining
/// its header and (for containers/variable-length types) reading
/// size information. Does NOT recursively validate the contents.
///
/// \param[in] data Pointer to the start of a variant value
/// \param[in] length Maximum bytes available
/// \return Total byte count of the value, or Status::Invalid if truncated
ARROW_EXPORT Result<int64_t> ValueSize(const uint8_t* data, int64_t length);
```

**Why needed:**
- Used by `FindObjectField` to skip over values during field lookup
- Will be used by encoder's `FinishObject` for duplicate key compaction
- Matches Go's `valueSize()` utility

#### `FindObjectField` — Look up object field by name

Go equivalent: `ObjectValue.ValueByKey()` in `variant.go`

```cpp
/// \brief Find an object field by name and return a pointer to its value.
///
/// Searches the field IDs in the object, resolving each against the
/// metadata dictionary. Per spec, field IDs are in lexicographic order
/// of their corresponding key names, enabling binary search.
///
/// \param[in] metadata Parsed metadata (for resolving field IDs to names)
/// \param[in] data Pointer to the object value buffer
/// \param[in] length Length of the value buffer
/// \param[in] field_name The field name to search for
/// \param[out] field_offset Set to the byte offset of the field's value
///             within data, or -1 if not found
/// \param[out] field_size Set to the byte size of the field's value
/// \return Status::OK if search completed (field may or may not be found),
///         Status::Invalid if the buffer is malformed
ARROW_EXPORT Status FindObjectField(const VariantMetadata& metadata,
                                    const uint8_t* data, int64_t length,
                                    std::string_view field_name,
                                    int64_t* field_offset,
                                    int64_t* field_size);
```

**Implementation notes:**
- Since spec requires field IDs to be in lexicographic key order, we can **binary search** on the field IDs, comparing each resolved key against `field_name`
- For small objects (<32 fields), linear scan may be faster (match Go's threshold)
- Returns offset+size so caller can decode just that field, or pass to `DecodeVariantValue`

#### `GetArrayElement` — Random access to array element by index

Go equivalent: `ArrayValue.Value(i)` in `variant.go`

```cpp
/// \brief Get a pointer to the i-th element of a variant array.
///
/// Uses the offset table for O(1) random access without traversing
/// preceding elements.
///
/// \param[in] data Pointer to the array value buffer
/// \param[in] length Length of the value buffer
/// \param[in] index Zero-based element index
/// \param[out] element_offset Set to the byte offset of the element
/// \param[out] element_size Set to the byte size of the element
/// \return Status::OK on success, Status::Invalid if not an array or
///         index is out of range
ARROW_EXPORT Status GetArrayElement(const uint8_t* data, int64_t length,
                                    int32_t index,
                                    int64_t* element_offset,
                                    int64_t* element_size);
```

#### `GetObjectFieldAt` — Positional access to object field

Go equivalent: `ObjectValue.FieldAt(i)` in `variant.go`

```cpp
/// \brief Get the i-th field of a variant object (by position).
///
/// Returns both the field name (resolved via metadata) and a pointer
/// to the field's value.
///
/// \param[in] metadata Parsed metadata
/// \param[in] data Pointer to the object value buffer
/// \param[in] length Length of the value buffer
/// \param[in] index Zero-based field index
/// \param[out] field_name Set to the field's key name
/// \param[out] field_offset Set to the byte offset of the field's value
/// \param[out] field_size Set to the byte size of the field's value
/// \return Status::OK on success, Status::Invalid if not an object or
///         index is out of range
ARROW_EXPORT Status GetObjectFieldAt(const VariantMetadata& metadata,
                                     const uint8_t* data, int64_t length,
                                     int32_t index,
                                     std::string_view* field_name,
                                     int64_t* field_offset,
                                     int64_t* field_size);
```

#### `FindMetadataKey` — Look up dictionary ID by key name

Go equivalent: `Metadata.IdFor()` in `variant.go`

```cpp
/// \brief Find the dictionary ID for a given key name.
///
/// Uses binary search if the metadata is sorted, otherwise linear scan.
///
/// \param[in] metadata Parsed metadata
/// \param[in] key The key to search for
/// \return The dictionary ID if found, or -1 if not present
ARROW_EXPORT int32_t FindMetadataKey(const VariantMetadata& metadata,
                                     std::string_view key);
```

### A.2 Tests to Add to `variant_internal_test.cc`

```
VariantValueSizeTest:
  - Null (1 byte)
  - Int32 (5 bytes)
  - Short string "hello" (6 bytes)
  - Long string (1 + 4 + N bytes)
  - Object with 2 fields
  - Nested array

VariantFieldLookupTest:
  - Find existing field by name
  - Field not found returns offset=-1
  - Binary search on sorted dictionary
  - Linear scan on unsorted dictionary
  - Empty object

VariantArrayAccessTest:
  - Access first element
  - Access last element
  - Access middle element
  - Index out of range

VariantObjectFieldAtTest:
  - Access by index 0, 1, 2
  - Index out of range

VariantMetadataKeyLookupTest:
  - Key exists (sorted)
  - Key exists (unsorted)
  - Key not found
```

### A.3 Why This Belongs in the Decoder PR

1. **These are read operations** — they decode/access variant data, they don't produce it
2. **Go puts them in `variant.go`** (the reader file), not `builder.go`
3. **A reviewer comparing to Go will expect them** in the decoding PR
4. **The encoder needs `ValueSize`** — but it's architecturally a read utility
5. **Clean separation**: decoder PR = "how to read variants", encoder PR = "how to write variants"

---

## Part B: Encoder PR (`variant-encoding`)

After the decoder additions above, the encoder PR focuses purely on writing.

### B.1 New Files

| File | Purpose |
|------|---------|
| `variant_builder.cc` | VariantBuilder implementation |
| `variant_builder_test.cc` | Encoder + round-trip tests |

### B.2 `VariantBuilder` API (in `variant_internal.h`)

```cpp
class ARROW_EXPORT VariantBuilder {
 public:
  VariantBuilder();

  /// Construct builder pre-populated with an existing dictionary.
  /// Useful when encoding multiple values sharing the same schema.
  explicit VariantBuilder(const VariantMetadata& existing_metadata);

  // --- Primitive setters ---
  Status Null();
  Status Bool(bool value);
  Status Int(int64_t value);      // auto-selects smallest int type
  Status Int8(int8_t value);
  Status Int16(int16_t value);
  Status Int32(int32_t value);
  Status Int64(int64_t value);
  Status Float(float value);
  Status Double(double value);
  Status Decimal4(uint8_t scale, const uint8_t* value_bytes);
  Status Decimal8(uint8_t scale, const uint8_t* value_bytes);
  Status Decimal16(uint8_t scale, const uint8_t* value_bytes);
  Status Date(int32_t days_since_epoch);
  Status TimestampMicros(int64_t micros);
  Status TimestampMicrosNTZ(int64_t micros);
  Status TimeNTZ(int64_t micros);
  Status TimestampNanos(int64_t nanos);
  Status TimestampNanosNTZ(int64_t nanos);
  Status String(std::string_view value);  // auto short-string optimization
  Status Binary(std::string_view value);
  Status UUID(const uint8_t* bytes);

  // --- Container construction ---
  /// Record current buffer offset (start of a container)
  int64_t Offset() const;

  /// Record element position for arrays
  int64_t NextElement(int64_t start) const;

  /// Record field entry for objects (adds key to dictionary)
  struct FieldEntry {
    std::string key;
    uint32_t id;
    int64_t offset;
  };
  FieldEntry NextField(int64_t start, std::string_view key);

  /// Finalize an array value
  Status FinishArray(int64_t start, const std::vector<int64_t>& offsets);

  /// Finalize an object value (sorts fields by key, detects duplicates)
  Status FinishObject(int64_t start, std::vector<FieldEntry>& fields);

  // --- Output ---
  struct EncodedVariant {
    std::vector<uint8_t> metadata;
    std::vector<uint8_t> value;
  };

  /// Finalize and produce encoded buffers.
  Result<EncodedVariant> Finish();

  /// Reset for reuse.
  void Reset();

 private:
  std::vector<uint8_t> buffer_;
  std::unordered_map<std::string, uint32_t> dict_;
  std::vector<std::string> dict_keys_;
};
```

### B.3 Key Implementation Details

**Integer auto-sizing** (matches Go's `AppendInt`):
```cpp
Status VariantBuilder::Int(int64_t value) {
  if (value >= INT8_MIN && value <= INT8_MAX) return Int8(value);
  if (value >= INT16_MIN && value <= INT16_MAX) return Int16(value);
  if (value >= INT32_MIN && value <= INT32_MAX) return Int32(value);
  return Int64(value);
}
```

**Short string optimization** (matches Go's `AppendString`):
```cpp
Status VariantBuilder::String(std::string_view value) {
  if (value.size() <= 63) {
    // short string: header encodes length
    buffer_.push_back(shortStrHeader(value.size()));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
  } else {
    // long string: primitive_type=kString + 4-byte length + data
    ...
  }
}
```

**FinishObject** (matches Go's approach):
1. Sort `fields` by key name lexicographically
2. Check for duplicate keys (error if found)
3. Compute `field_id_size`, `field_offset_size`, `is_large`
4. Shift existing data to make room for header
5. Write: header + num_elements + field_ids + offsets

**FinishArray**: Same as FinishObject but simpler (no field IDs, no sorting).

**Metadata encoding in `Finish()`**:
1. Compute `offset_size` = min bytes to address total string data
2. Set sorted flag if dictionary keys happen to be in lexicographic order
3. Write: header + dict_size + offsets + string data

### B.4 Test Strategy

Round-trip tests are the primary validation:
```
encode → decode → verify events match input
```

Additional encoder-specific tests:
- Integer auto-sizing (Int(42) → encodes as Int8)
- Short string threshold (63 bytes → short, 64 → long)
- Object field ordering (verify IDs in lex order)
- Duplicate key detection
- is_large flag (>255 elements)
- Offset size optimization
- Builder reuse after Reset()
- Builder from existing metadata

---

## Part C: Execution Plan

```
1. Write decoder additions plan (this document) ✅
2. Switch to variant-decoding branch
3. Add ValueSize, FindObjectField, GetArrayElement, GetObjectFieldAt, FindMetadataKey
4. Add tests for all new functions
5. Build + test (Docker, 70+ tests → ~85+ tests)
6. Amend commit, force-push to variant-decoding
7. Switch to variant-encoding branch
8. Rebase on updated variant-decoding
9. Implement VariantBuilder in variant_builder.cc
10. Implement tests in variant_builder_test.cc
11. Build + full test suite
12. Push variant-encoding
```

---

## Part D: Go Parity Summary After Both PRs

| Go Feature | After Decoder PR | After Encoder PR |
|---|---|---|
| Metadata decode | ✅ | ✅ |
| Full value traversal (visitor) | ✅ | ✅ |
| Type/size peek | ✅ | ✅ |
| Object field lookup by name | ✅ | ✅ |
| Object field-at-index | ✅ | ✅ |
| Array random access | ✅ | ✅ |
| Metadata key lookup | ✅ | ✅ |
| Value size calculation | ✅ | ✅ |
| Builder (all primitives) | — | ✅ |
| Integer auto-sizing | — | ✅ |
| Short string optimization | — | ✅ |
| Object construction + sorting | — | ✅ |
| Array construction | — | ✅ |
| Duplicate key handling | — | ✅ |
| Metadata encoding | — | ✅ |
| Builder from existing metadata | — | ✅ |
| Builder reset/reuse | — | ✅ |
| JSON parse/serialize | ❌ (out of scope) | ❌ (out of scope) |
| Generic Append with reflection | ❌ (N/A in C++) | ❌ (N/A in C++) |
| Struct tag support | ❌ (N/A in C++) | ❌ (N/A in C++) |

**Result**: Full functional parity with Go for encode/decode operations. Only JSON and Go-specific features (reflection, struct tags) are omitted.


---

## Part E: Verified 1:1 Mapping with Go Implementation

### Exact Matches (Line-Level Verification)

| Go Code | C++ Code | Verified |
|---|---|---|
| `basicTypeFromHeader(hdr)` = `BasicType(hdr & 0x3)` | `GetBasicType(header)` = `static_cast<BasicType>(header & 0x03)` | ✅ |
| `primitiveTypeFromHeader(hdr)` = `PrimitiveType((hdr >> 2) & 0x3F)` | `GetPrimitiveType(header)` = `static_cast<PrimitiveType>((header >> 2) & 0x3F)` | ✅ |
| `PrimitiveType` enum values 0-20 | `PrimitiveType` enum values 0-20 | ✅ Identical |
| `BasicType` enum values 0-3 | `BasicType` enum values 0-3 | ✅ Identical |
| `supportedVersion = 1` | `kVariantVersion = 1` | ✅ |
| `maxShortStringSize = 0x3F` (63) | Short string: `(header >> 2) & 0x3F` max = 63 | ✅ |
| `Metadata.Version()` = `data[0] & 0x0F` | `header & 0x0F` | ✅ |
| `Metadata.SortedAndUnique()` = `data[0] & 0b10000 != 0` | `((header >> 4) & 0x01) != 0` | ✅ |
| `Metadata.OffsetSize()` = `((data[0] >> 6) & 0b11) + 1` | `((header >> 6) & 0x03) + 1` | ✅ |
| `objectHeader(large, idSize, offsetSize)` bit layout | `DecodeObject` reads bits 2-3=offsetSz, 4-5=idSz, 6=large | ✅ |
| `arrayHeader(large, offsetSize)` bit layout | `DecodeArray` reads bits 2-3=offsetSz, 4=large | ✅ |
| `readLEU32(b)`: copy into uint32, `endian.FromLE()` | `ReadUnsignedLE(data, n)`: memcpy into uint32, `FromLittleEndian()`, mask | ✅ |
| Decimal encoding: `WriteByte(scale)` then `binary.Write(value)` | `data[pos]` = scale, `data[pos+1:]` = value bytes | ✅ |
| `intSize(v)`: MaxUint8→1, MaxUint16→2, 0xFFFFFF→3, else→4 | Same logic planned for encoder | ✅ |

### Architectural Differences (Language-Driven, Not Spec Differences)

| Aspect | Go | C++ | Why Different |
|---|---|---|---|
| Value access pattern | `Value.Value() any` returns materialized Go value | `DecodeVariantValue(visitor)` fires callbacks | C++ has no boxed `any` type with runtime dispatch. Visitor is idiomatic C++ and avoids heap allocation per value. Both provide full traversal. |
| Random access | `ObjectValue.ValueByKey()` on a returned struct | Standalone `FindObjectField()` function | Go returns value objects that hold references. C++ uses explicit pointer + length pairs. Same semantics, different ownership. |
| Iterators | `iter.Seq[Value]` / `iter.Seq2[string, Value]` | Visitor pattern | Go 1.23 iterators vs C++ visitor. Both are lazy. |
| Generic append | `Append(v any)` with runtime reflection | Separate typed methods: `Int32()`, `String()`, etc. | C++ has no runtime reflection. Template specialization is possible but overkill here. |
| Error model | `error` interface (single return) | `Status` / `Result<T>` | Standard Arrow pattern vs Go idiom. |
| Memory | GC manages lifetimes | Explicit: `string_view` borrows, builder owns | Fundamental language difference. |
| JSON | `MarshalJSON()` / `ParseJSON()` built-in | Out of scope for now | Can be added as separate utility later. Not core encode/decode. |
| Struct tags | `variant:"fieldname,nanos,utc"` | N/A | Go-specific language feature. |
| Clone | `Value.Clone()` deep copy | Not needed | C++ doesn't implicitly share buffers. API returns owned vectors or borrows via string_view with documented lifetime. |

### Functional Gaps (To Be Closed)

| Go Function | Planned C++ Equivalent | PR |
|---|---|---|
| `Metadata.IdFor(key)` | `FindMetadataKey(metadata, key)` | Decoder |
| `ObjectValue.ValueByKey(key)` | `FindObjectField(metadata, data, len, key, &offset, &size)` | Decoder |
| `ObjectValue.FieldAt(i)` | `GetObjectFieldAt(metadata, data, len, i, &name, &offset, &size)` | Decoder |
| `ArrayValue.Value(i)` | `GetArrayElement(data, len, i, &offset, &size)` | Decoder |
| `valueSize(v)` | `ValueSize(data, length)` | Decoder |
| `Builder.AppendNull/Bool/Int/etc.` | `VariantBuilder::Null/Bool/Int/etc.` | Encoder |
| `Builder.AppendInt(v)` auto-size | `VariantBuilder::Int(int64_t)` auto-size | Encoder |
| `Builder.AppendString(v)` short-string opt | `VariantBuilder::String(sv)` short-string opt | Encoder |
| `Builder.NextField/FinishObject` | `VariantBuilder::NextField/FinishObject` | Encoder |
| `Builder.NextElement/FinishArray` | `VariantBuilder::NextElement/FinishArray` | Encoder |
| `Builder.Build()` | `VariantBuilder::Finish()` | Encoder |
| `Builder.Reset()` | `VariantBuilder::Reset()` | Encoder |
| `NewBuilderFromMeta(m)` | `VariantBuilder(existing_metadata)` | Encoder |
| `Builder.SetAllowDuplicates(bool)` | Duplicate detection in `FinishObject` | Encoder |

---

## Part F: Potential Bug in Go's `valueSize` for Arrays

### The Issue

In `apache/arrow-go` file `parquet/variant/utils.go`, the `valueSize()` function has this code for arrays:

```go
case byte(BasicArray):
    var szBytes uint8 = 1
    if ((typeInfo >> 4) & 0x1) != 0 {   // ← checks bit 4 of typeInfo
        szBytes = 4
    }
```

But in the same repo, `variant.go`'s `Value.Value()` for arrays uses:

```go
case BasicArray:
    valueHdr := (v.value[0] >> basicTypeBits)
    fieldOffsetSz := (valueHdr & 0b11) + 1
    isLarge := ((valueHdr >> 2) & 0b1) == 1   // ← checks bit 2 of valueHdr
```

And the `arrayHeader()` constructor in `utils.go`:

```go
func arrayHeader(large bool, offsetSize uint8) byte {
    return (largeBit << (basicTypeBits + 2)) |    // largeBit at bit 4 of full byte = bit 2 of valueHdr
           ((offsetSize - 1) << basicTypeBits) |
           byte(BasicArray)
}
```

### Analysis

- `typeInfo` = `valueHdr` = `(header >> 2) & 0x3F` (the 6-bit value_header)
- For arrays, `is_large` is placed at bit 2 of the 6-bit value_header by `arrayHeader()`
- `Value.Value()` correctly reads it as `(valueHdr >> 2) & 0x1` ✅
- `valueSize()` reads it as `(typeInfo >> 4) & 0x1` — that's bit 4 of valueHdr = **bit 6 of the full byte** ❌

**Bit 6 of the full byte is the object's `is_large` position, NOT the array's.** The array's `is_large` is at bit 4 of the full byte (bit 2 of valueHdr).

### Impact Assessment

This bug in `valueSize()` means:
- For arrays with `is_large=true` (>255 elements), `valueSize()` would incorrectly read `szBytes=1` instead of `szBytes=4`
- This would cause incorrect size calculation, leading to silent data corruption when `valueSize()` is used in `FinishObject()` for duplicate key compaction
- The bug does NOT affect normal reading/decoding (which uses `Value.Value()` with the correct bit shift)
- It only manifests when an object contains a duplicate-keyed field whose value is a large array (>255 elements)

### Why It Hasn't Been Caught

1. `valueSize()` is only called from `FinishObject()` during duplicate key handling
2. Duplicate keys are disallowed by default (`allowDuplicates=false`)
3. Even with duplicates enabled, the field value would need to be a large array (>255 elements) — an uncommon test case
4. The Go tests likely don't have a test case combining duplicates + large arrays

### Validation Strategy

To confirm this is a real bug, write a Go test that:

```go
func TestValueSizeLargeArray(t *testing.T) {
    // Build a variant with a large array (>255 elements)
    var b variant.Builder
    start := b.Offset()
    offsets := make([]int, 0, 300)
    for i := 0; i < 300; i++ {
        offsets = append(offsets, b.NextElement(start))
        b.AppendNull()
    }
    b.FinishArray(start, offsets)

    // Get the raw bytes and compute valueSize
    raw := b.BuildWithoutMeta()
    
    // The array header should have is_large=true (4-byte num_elements)
    // valueSize should return the correct total size
    expected := 1 + 4 + (300+1)*2 + 300  // header + numElem(4) + offsets + data
    // But with the bug, valueSize reads szBytes=1, getting wrong num_elements
    
    got := valueSize(raw)  // if this != expected, bug confirmed
    if got != expected {
        t.Errorf("valueSize bug: got %d, want %d", got, expected)
    }
}
```

An even more targeted reproduction:

```go
func TestDuplicateKeyWithLargeArrayCorruption(t *testing.T) {
    var b variant.Builder
    b.SetAllowDuplicates(true)
    
    // Build object: {"key": [300 nulls], "key": "override"}
    start := b.Offset()
    fields := make([]variant.FieldEntry, 0)
    
    // First "key" → large array
    fields = append(fields, b.NextField(start, "key"))
    arrStart := b.Offset()
    arrOffsets := make([]int, 0, 300)
    for i := 0; i < 300; i++ {
        arrOffsets = append(arrOffsets, b.NextElement(arrStart))
        b.AppendNull()
    }
    b.FinishArray(arrStart, arrOffsets)
    
    // Second "key" → override value (duplicate)
    fields = append(fields, b.NextField(start, "key"))
    b.AppendString("override")
    
    // FinishObject uses valueSize internally for compaction
    // If valueSize is wrong for the large array, this corrupts data
    err := b.FinishObject(start, fields)
    if err != nil {
        t.Fatal(err)
    }
    
    val, err := b.Build()
    if err != nil {
        t.Fatal(err)
    }
    
    // Verify the object has the override value
    obj := val.Value().(variant.ObjectValue)
    field, err := obj.ValueByKey("key")
    if err != nil {
        t.Fatal(err)
    }
    if field.Value.Value().(string) != "override" {
        t.Errorf("expected 'override', got %v", field.Value.Value())
    }
}
```

### Recommended Action

1. **Write the test locally** against the Go repo to confirm the bug
2. **If confirmed**: File a GitHub issue on `apache/arrow-go` with:
   - Title: `[Go][Parquet] Variant valueSize() uses wrong bit shift for array is_large`
   - Description: The analysis above with the reproducer test
   - Suggested fix: Change `(typeInfo >> 4)` to `(typeInfo >> 2)` in the array case of `valueSize()`
3. **In our C++ PR**: Implement `ValueSize()` correctly per spec (using bit 2 of typeInfo for arrays), add a comment noting the divergence from Go:
   ```cpp
   // Note: Go's valueSize() in arrow-go uses (typeInfo >> 4) for arrays,
   // which appears to be a bug (should be >> 2 per spec). We follow the spec.
   ```
4. **Optionally**: Submit a one-line fix PR to arrow-go alongside the bug report

### Our C++ Implementation (Correct)

```cpp
Result<int64_t> ValueSize(const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("ValueSize: buffer is null or empty");
  }

  uint8_t header = data[0];
  auto basic_type = GetBasicType(header);
  uint8_t type_info = (header >> 2) & 0x3F;

  switch (basic_type) {
    case BasicType::kShortString:
      return 1 + static_cast<int64_t>(type_info);

    case BasicType::kObject: {
      // is_large is bit 4 of type_info (bit 6 of full byte)
      bool is_large = ((type_info >> 4) & 0x01) != 0;
      int32_t sz_bytes = is_large ? 4 : 1;
      if (1 + sz_bytes > length) return Status::Invalid("truncated");
      auto num_elements = static_cast<int64_t>(
          ReadUnsignedLE(data + 1, sz_bytes));
      int32_t id_size = ((type_info >> 2) & 0x03) + 1;
      int32_t offset_size = (type_info & 0x03) + 1;
      int64_t id_start = 1 + sz_bytes;
      int64_t offset_start = id_start + num_elements * id_size;
      int64_t data_start = offset_start + (num_elements + 1) * offset_size;
      // Last offset = total data size
      int64_t last_offset_pos = offset_start + num_elements * offset_size;
      if (last_offset_pos + offset_size > length) return Status::Invalid("truncated");
      auto total_data = static_cast<int64_t>(
          ReadUnsignedLE(data + last_offset_pos, offset_size));
      return data_start + total_data;
    }

    case BasicType::kArray: {
      // is_large is bit 2 of type_info (bit 4 of full byte) — per spec
      bool is_large = ((type_info >> 2) & 0x01) != 0;
      int32_t sz_bytes = is_large ? 4 : 1;
      if (1 + sz_bytes > length) return Status::Invalid("truncated");
      auto num_elements = static_cast<int64_t>(
          ReadUnsignedLE(data + 1, sz_bytes));
      int32_t offset_size = (type_info & 0x03) + 1;
      int64_t offset_start = 1 + sz_bytes;
      int64_t data_start = offset_start + (num_elements + 1) * offset_size;
      // Last offset = total data size
      int64_t last_offset_pos = offset_start + num_elements * offset_size;
      if (last_offset_pos + offset_size > length) return Status::Invalid("truncated");
      auto total_data = static_cast<int64_t>(
          ReadUnsignedLE(data + last_offset_pos, offset_size));
      return data_start + total_data;
    }

    case BasicType::kPrimitive: {
      auto ptype = static_cast<PrimitiveType>(type_info);
      int32_t payload_size = PrimitiveValueSize(ptype);
      if (payload_size >= 0) {
        return 1 + static_cast<int64_t>(payload_size);
      }
      // Variable-length: Binary or String (4-byte length prefix)
      if (1 + 4 > length) return Status::Invalid("truncated");
      uint32_t var_len;
      std::memcpy(&var_len, data + 1, 4);
      var_len = bit_util::FromLittleEndian(var_len);
      return 1 + 4 + static_cast<int64_t>(var_len);
    }

    default:
      return Status::Invalid("ValueSize: unknown basic type");
  }
}
```
