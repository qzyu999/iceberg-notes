# GH-45947 Solution: Variant Binary Encoding

> **Status**: Implementation complete, 114/114 tests passing (87 decoder + 27 encoder)
> **Branch**: `variant-encoding` on `qzyu999/arrow`
> **Depends on**: GH-45946 (decoder, in parent commit)

---

## 1. What Was Built

A `VariantBuilder` class that encodes structured data into the two-buffer Variant binary format (metadata + value) per the Variant Encoding Spec. The encoder is validated by round-trip tests against the decoder from GH-45946.

---

## 2. Parity with Go Implementation

### Method-by-Method Mapping

| Go (`builder.go`) | C++ (ours) | Match? |
|---|---|---|
| `Builder{}` (zero-value) | `VariantBuilder()` | ✅ |
| `NewBuilderFromMeta(m)` | `VariantBuilder(metadata)` | ✅ |
| `AddKey(key) uint32` | `AddKey(key) → uint32` (private) | ✅ |
| `AppendNull()` | `Null()` | ✅ |
| `AppendBool(v)` | `Bool(v)` | ✅ |
| `AppendInt(v)` auto-sizes | `Int(v)` auto-sizes int8/16/32/64 | ✅ |
| `AppendFloat32(v)` | `Float(v)` | ✅ |
| `AppendFloat64(v)` | `Double(v)` | ✅ |
| `AppendDate(v)` | `Date(v)` | ✅ |
| `AppendTimeMicro(v)` | `TimeNTZ(v)` | ✅ |
| `AppendTimestamp(v, useMicros, useUTC)` | `TimestampMicros/MicrosNTZ/Nanos/NanosNTZ(v)` | ✅ (explicit methods vs options) |
| `AppendString(v)` with short-string opt | `String(v)` with short-string opt (≤63 → short) | ✅ |
| `AppendBinary(v)` | `Binary(v)` | ✅ |
| `AppendUUID(v)` | `UUID(bytes)` | ✅ |
| `AppendDecimal4/8/16(scale, v)` | `Decimal4/8/16(scale, bytes)` | ✅ |
| `Offset() int` | `Offset() → int64_t` | ✅ |
| `NextElement(start) int` | `NextElement(start) → int64_t` | ✅ |
| `NextField(start, key) FieldEntry` | `NextField(start, key) → FieldEntry` | ✅ |
| `FinishArray(start, offsets)` | `FinishArray(start, offsets)` | ✅ |
| `FinishObject(start, fields)` sorts + dedup check | `FinishObject(start, fields)` sorts + dup error | ✅ |
| `Build() (Value, error)` | `Finish() → Result<EncodedVariant>` | ✅ |
| `Reset()` | `Reset()` | ✅ |
| `SetAllowDuplicates(bool)` | Duplicates always rejected (error) | ⚠️ See below |
| `BuildWithoutMeta() []byte` | Not implemented | ⚠️ See below |
| `UnsafeAppendEncoded(v)` | Not implemented | ⚠️ See below |
| `Append(v any, opts...)` reflection | N/A (C++ has no runtime reflection) | N/A |
| `ParseJSON/ParseJSONBytes/Unmarshal` | Not implemented (out of scope) | N/A |
| Struct tag support | N/A (Go-specific) | N/A |

### Deliberate Omissions

| Go Feature | Why Omitted | Future? |
|---|---|---|
| `SetAllowDuplicates(true)` — keep last value | Spec says objects must not contain duplicate keys. Our impl enforces this strictly. Can be added if needed. | Optional |
| `BuildWithoutMeta()` | Used for shredded variants. Will be needed for GH-45948. Trivial to add (just return `buffer_`). | GH-45948 |
| `UnsafeAppendEncoded(v)` | For appending pre-encoded variant bytes. Useful for composition. Simple to add later. | Future |
| JSON parse/serialize | Separate concern, not core encode/decode. | Future utility |

### Architecture Match

| Aspect | Go | C++ | Match? |
|---|---|---|---|
| Internal buffer type | `bytes.Buffer` | `std::vector<uint8_t>` | ✅ Equivalent |
| Dictionary storage | `map[string]uint32` + `[][]byte` | `unordered_map<string,uint32>` + `vector<string>` | ✅ Equivalent |
| Container pattern | Write data first, then shift + insert header at Finish | Same: data at `start`, shift + insert header | ✅ Identical algorithm |
| Object key sorting | `slices.SortFunc` by key at `FinishObject` | `std::sort` by key at `FinishObject` | ✅ |
| Offset size optimization | `intSize(v)` → 1/2/3/4 bytes | `IntSize(v)` → 1/2/3/4 bytes | ✅ |
| Metadata sorted flag | Set if keys happen to be in order at Build time | Set if `dict_keys_` is sorted at Finish time | ✅ |
| Short string threshold | `maxShortStringSize = 0x3F` (63) | `value.size() <= 63` | ✅ |
| is_large threshold | `num > math.MaxUint8` (255) | `num > 255` | ✅ |

---

## 3. Files Changed

| File | Action | Purpose |
|------|--------|---------|
| `variant_internal.h` | Modified | Added `VariantBuilder` class declaration + `<string>`, `<unordered_map>` includes |
| `variant_builder.cc` | **New** | Builder implementation (~280 lines) |
| `variant_builder_test.cc` | **New** | 27 encoder + round-trip tests |
| `cpp/src/arrow/CMakeLists.txt` | Modified | Added `variant_builder.cc` |
| `cpp/src/arrow/extension/CMakeLists.txt` | Modified | Added `variant_builder_test.cc` |
| `cpp/src/arrow/meson.build` | Modified | Added `variant_builder.cc` |
| `cpp/src/arrow/extension/meson.build` | Modified | Added `variant_builder_test.cc` |

---

## 4. Test Coverage (27 tests)

| Category | Tests | What's Verified |
|---|---|---|
| Primitive round-trip | 12 | Null, Bool×2, Int auto-size×5, String×4, Date, Double |
| Array round-trip | 3 | Empty, simple [1,2,3], nested [[10,20],30] |
| Object round-trip | 5 | Empty, simple, nested, duplicate key error, field sorting |
| Builder features | 4 | Reset/reuse, from-existing-metadata, sorted flag, unsorted flag |
| Integration | 1 | Complex: `{name:"Alice", scores:[95,87,92], active:true}` |

All tests use the **round-trip invariant**: `decode(encode(v)) == v`.

---

## 5. Key Implementation Details

### Integer auto-sizing (matches Go's `AppendInt`)
```cpp
Status VariantBuilder::Int(int64_t value) {
  if (value >= INT8_MIN && value <= INT8_MAX) return Int8(...);
  if (value >= INT16_MIN && value <= INT16_MAX) return Int16(...);
  if (value >= INT32_MIN && value <= INT32_MAX) return Int32(...);
  return Int64(value);
}
```

### Container "shift" algorithm (matches Go exactly)
Both Go and C++ use the same approach:
1. Write child values into the buffer starting at `start`
2. At `Finish`, compute header size (header byte + sizes + IDs + offsets)
3. `memmove` the child data right by `header_size` bytes
4. Write the header into the gap at `[start, start+header_size)`

This avoids a two-pass approach and matches Go's `FinishArray`/`FinishObject` line-for-line.

### Object field ordering (spec requirement)
Fields are sorted by key name at `FinishObject`. The offsets in the encoded buffer point into the child data which remains in insertion order — only the IDs and offsets in the header are reordered. This matches the spec: "field IDs and offsets must be listed in the order of the corresponding field names, sorted lexicographically."

---

## 6. What's NOT in This PR (Scope Boundaries)

- **No duplicate-key tolerance** — Go has `SetAllowDuplicates` which keeps the last value via compaction using `valueSize`. We reject duplicates with an error, which is spec-compliant ("An object may not contain duplicate keys"). Can be added later if Go-compat is needed.
- **No `BuildWithoutMeta`** — needed for shredding (GH-45948), not for basic encoding.
- **No JSON** — separate concern.
- **No reflection/generic Append** — C++ language limitation, not a feature gap.
