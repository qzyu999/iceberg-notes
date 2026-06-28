misiek1984 reviewed 10 hours ago
misiek1984
left a comment
Some initial comments.

cpp/src/arrow/extension/variant_internal.h
/// Searches the field IDs in the object, resolving each against the
/// metadata dictionary. Per spec, field IDs are in lexicographic order
/// of their corresponding key names, enabling binary search for large
/// objects (>=32 fields). For smaller objects, linear scan is used.
@misiek1984
misiek1984
4 days ago
How was the 32 threshold determined?

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.h
Outdated

/// \brief Basic type codes from bits 0-1 of the value header byte.
///
/// Variant Encoding Spec §3: "Value encoding"
@misiek1984
misiek1984
4 days ago
nit: The current version of the spec does not contain paragraph §3 and §3.1. I would just add a link to the section with tables: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.h
/// Implements parsing logic per the Variant Encoding Spec:
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
///
/// The "internal" in the filename refers to the binary encoding internals
@misiek1984
misiek1984
4 days ago
I don't have a strong opinion here. But maybe instead of explaining in the comment what "internal" means it would be better to rename a file e.g. to variant_binary_encoding, variant_internal_encoding etc.

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal_test.cc

class VariantIntegrationTest : public ::testing::Test {};

TEST_F(VariantIntegrationTest, FullRoundTrip) {
@misiek1984
misiek1984
4 days ago
I would also add more tests demonstrating how to use all these new functions together. For example, let's assume we have the following Variant:

{
  "name": "Alice",
  "age": 30,
  "addresses": {
    "postal": {
      "country": "USA",
      "city": "New York"
    },
    "billing": {
      "country": "USA",
      "city": "Chicago"
    }
  }
}
If we want to find the city for the postal address, we would first need to use FindObjectField to find "addresses", then "postal", and finally "city". After that, we would read the value of the "city" field.

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.cc
///        the visitor. Returns the number of bytes consumed.
///
/// This is the core recursive function.
Status DecodeValueAt(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
@misiek1984
misiek1984
4 days ago
I think this function should be public. Let's assume I want to read the value of a specific nested field from a Variant using a path (e.g., field_1.field_2.field_3).

My current understanding is that I would first need to call FindObjectField to locate "field_1". If it exists, I then have to find "field_2", and finally "field_3". However, I have to implement the last step—reading the actual value—on my own because DecodeValueAt is not public, and DecodeVariantValue only allows for decoding the entire Variant.

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.h
/// \return Status::OK on success, Status::Invalid on malformed input
///
/// \note The data buffer must remain valid for the duration of the call.
ARROW_EXPORT Status DecodeVariantValue(const VariantMetadata& metadata,
@misiek1984
misiek1984
13 hours ago
Do you have a plan to also support reading/decoding shredded variants?

@qzyu999	Reply...
