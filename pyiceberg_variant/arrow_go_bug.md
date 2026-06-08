# Bug Report: arrow-go `valueSize()` uses wrong bit for array `is_large`

> **Repository**: https://github.com/apache/arrow-go
> **File**: `parquet/variant/utils.go`, line ~119
> **Severity**: Data corruption under specific conditions
> **Discovered during**: C++ Variant encoding implementation (GH-45946/45947 on apache/arrow)

---

## Summary

The `valueSize()` function in `parquet/variant/utils.go` uses `(typeInfo >> 4) & 0x1` to check the `is_large` flag for **both** objects and arrays. This is correct for objects but **incorrect for arrays**. The array `is_large` bit is at position 2 of the value_header (bit 4 of the full byte), not position 4 (bit 6).

The correct code in `variant.go` (`Value.Value()`) uses `(valueHdr >> 2) & 0b1` for arrays, confirming the discrepancy.

---

## Root Cause

The Variant Encoding Spec defines different header layouts for objects and arrays:

**Object value_header (6 bits):**
```
bit 0-1: field_offset_size_minus_one
bit 2-3: field_id_size_minus_one
bit 4:   is_large
bit 5:   unused
```

**Array value_header (6 bits):**
```
bit 0-1: field_offset_size_minus_one
bit 2:   is_large
bit 3-5: unused
```

The `valueSize()` function treats both the same way, checking bit 4 for `is_large` in both cases.

---

## Evidence

### Incorrect code (`utils.go` line ~119):

```go
func valueSize(v []byte) int {
    basicType, typeInfo := v[0]&basicTypeMask, (v[0]>>basicTypeBits)&typeInfoMask
    switch basicType {
    // ...
    case byte(BasicArray):
        var szBytes uint8 = 1
        if ((typeInfo >> 4) & 0x1) != 0 {   // ❌ checks bit 4 of typeInfo
            szBytes = 4
        }
        // ...
```

### Correct code (`variant.go` line ~688):

```go
case BasicArray:
    valueHdr := (v.value[0] >> basicTypeBits)
    fieldOffsetSz := (valueHdr & 0b11) + 1
    isLarge := ((valueHdr >> 2) & 0b1) == 1   // ✅ checks bit 2 of valueHdr
```

### Header constructor confirms bit 2 (`utils.go` line ~69):

```go
func arrayHeader(large bool, offsetSize uint8) byte {
    // ...
    return (largeBit << (basicTypeBits + 2)) |    // largeBit at bit 4 of full byte = bit 2 of valueHdr
           ((offsetSize - 1) << basicTypeBits) | byte(BasicArray)
}
```

`largeBit << (basicTypeBits + 2)` = `largeBit << 4` in the full byte, which is `largeBit << 2` in the 6-bit value_header (after shifting out the 2 basic_type bits). So `is_large` is at bit 2 of `typeInfo`/`valueHdr`, not bit 4.

---

## Impact

**Affected code path**: `valueSize()` is called from `Builder.FinishObject()` (line ~735 of `builder.go`) during duplicate key compaction:

```go
fieldSize := valueSize(buf[start+oldOffset:])
copy(buf[start+curOffset:], buf[start+oldOffset:start+oldOffset+fieldSize])
```

**Trigger conditions** (all must be true simultaneously):
1. `Builder.SetAllowDuplicates(true)` is called
2. An object has duplicate keys
3. One of the duplicate-keyed field values is an array with >255 elements (triggering `is_large=true`)

**Result**: `valueSize()` returns an incorrect (too small) size for the large array, causing `FinishObject()` to copy fewer bytes than the actual value occupies. This silently corrupts the resulting variant binary.

**Why it hasn't been caught**: The combination of allowed duplicates + large arrays as field values is uncommon in tests.

---

## Suggested Fix

```diff
 func valueSize(v []byte) int {
     basicType, typeInfo := v[0]&basicTypeMask, (v[0]>>basicTypeBits)&typeInfoMask
     switch basicType {
     // ...
     case byte(BasicArray):
         var szBytes uint8 = 1
-        if ((typeInfo >> 4) & 0x1) != 0 {
+        if ((typeInfo >> 2) & 0x1) != 0 {
             szBytes = 4
         }
```

One line change: `>> 4` → `>> 2` in the array case only. The object case (`>> 4`) is correct and should remain unchanged.

---

## Reproducer Test

```go
package variant

import (
    "testing"
)

func TestValueSizeLargeArray(t *testing.T) {
    // Build a large array with >255 elements to trigger is_large=true
    var b Builder
    start := b.Offset()
    offsets := make([]int, 0, 300)
    for i := 0; i < 300; i++ {
        offsets = append(offsets, b.NextElement(start))
        b.AppendNull()
    }
    if err := b.FinishArray(start, offsets); err != nil {
        t.Fatal(err)
    }

    raw := b.BuildWithoutMeta()

    // Verify the header has is_large set
    typeInfo := (raw[0] >> basicTypeBits) & typeInfoMask
    isLargeBit2 := ((typeInfo >> 2) & 0x1)  // correct position
    isLargeBit4 := ((typeInfo >> 4) & 0x1)  // position valueSize checks

    if isLargeBit2 != 1 {
        t.Fatal("expected is_large to be set at bit 2 of typeInfo")
    }
    if isLargeBit4 != 0 {
        // bit 4 should be zero (it's unused for arrays)
        // but valueSize checks this bit, so it would miss is_large
        t.Log("bit 4 is zero as expected — valueSize will incorrectly read szBytes=1")
    }

    // Now verify valueSize returns incorrect result
    got := valueSize(raw)

    // Manually compute the correct size:
    // header(1) + numElements(4, because is_large) + offsets((300+1)*1) + data(300*1)
    // offsetSize should be 2 (300 > 255)
    // Actually let's just check the header byte's offset_size
    offsetSize := int((typeInfo & 0b11) + 1)
    expected := 1 + 4 + (300+1)*offsetSize + 300

    if got != expected {
        t.Errorf("valueSize bug confirmed: got %d, expected %d (diff=%d)",
            got, expected, expected-got)
    }
}

func TestDuplicateKeyWithLargeArrayCorruption(t *testing.T) {
    var b Builder
    b.SetAllowDuplicates(true)

    // Build: {"key": [300 nulls], "key": "override"}
    start := b.Offset()
    fields := make([]FieldEntry, 0)

    // First "key" → large array (>255 elements)
    fields = append(fields, b.NextField(start, "key"))
    arrStart := b.Offset()
    arrOffsets := make([]int, 0, 300)
    for i := 0; i < 300; i++ {
        arrOffsets = append(arrOffsets, b.NextElement(arrStart))
        b.AppendNull()
    }
    if err := b.FinishArray(arrStart, arrOffsets); err != nil {
        t.Fatal(err)
    }

    // Second "key" → this should override (duplicate)
    fields = append(fields, b.NextField(start, "key"))
    if err := b.AppendString("override"); err != nil {
        t.Fatal(err)
    }

    // FinishObject calls valueSize internally for compaction.
    // With the bug, it miscalculates the array size and corrupts data.
    if err := b.FinishObject(start, fields); err != nil {
        t.Fatal(err)
    }

    val, err := b.Build()
    if err != nil {
        t.Fatal(err)
    }

    // Try to read back — should get "override" for "key"
    obj := val.Value().(ObjectValue)
    field, err := obj.ValueByKey("key")
    if err != nil {
        t.Fatalf("failed to find key: %v", err)
    }
    result := field.Value.Value()
    if str, ok := result.(string); !ok || str != "override" {
        t.Errorf("expected 'override', got %v (type %T) — data likely corrupted", result, result)
    }
}
```

---

## GitHub Issue Template

**Title**: `[Go][Parquet] Variant valueSize() uses wrong bit shift for array is_large flag`

**Labels**: `Component: Go`, `Type: bug`

**Body**:

The `valueSize()` function in `parquet/variant/utils.go` checks `(typeInfo >> 4) & 0x1` for the array `is_large` flag, but the array header places `is_large` at bit 2 of the value_header (not bit 4). This matches the object layout but not the array layout.

The correct code in `variant.go` (`Value.Value()`) uses `(valueHdr >> 2) & 0b1` for arrays.

This causes `valueSize()` to return an incorrect size for arrays with >255 elements, which leads to silent data corruption when `FinishObject()` compacts duplicate keys whose values are large arrays.

**Fix**: Change `(typeInfo >> 4)` to `(typeInfo >> 2)` in the `BasicArray` case of `valueSize()`.

**Reproducer**: [include test above]

---

## Notes

- Discovered while implementing the C++ Variant encoder/decoder for `apache/arrow` (GH-45946, GH-45947)
- The C++ implementation follows the spec correctly using `(type_info >> 2) & 0x01` for arrays
- Credit: @zeroshade authored the Go implementation; this is a subtle copy-paste from the object case
