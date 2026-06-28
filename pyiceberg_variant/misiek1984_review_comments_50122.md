misiek1984 reviewed 10 hours ago
misiek1984
left a comment
Some initial comments.

cpp/src/arrow/extension/variant_builder_test.cc
                       DecodeMetadata(encoded1.metadata.data(),
                                      static_cast<int64_t>(encoded1.metadata.size())));

  // Build a new variant reusing the same metadata
@misiek1984
misiek1984
3 days ago
I would add a test where we reuse existing metadata but make a mistake with the data types. For example, according to the metadata, we should write a string but we write an integer instead. I think there is currently no validation for this case in VariantBuilder—is that on purpose?

Either way, the final Variant will be malformed, so the round-trip should fail.

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.h
class ARROW_EXPORT VariantBuilder {
 public:
  VariantBuilder();
  explicit VariantBuilder(const VariantMetadata& existing_metadata);
@misiek1984
misiek1984
3 days ago
It might also be useful to pass a value buffer to VariantBuilder to initialize buffer_. This way, it will be possible to continue building an existing Variant value.

@qzyu999	Reply...
cpp/src/arrow/extension/variant_internal.h
///   builder.Int(30);
///   builder.FinishObject(start, fields);
///   ARROW_ASSIGN_OR_RAISE(auto result, builder.Finish());
class ARROW_EXPORT VariantBuilder {
@misiek1984
misiek1984
3 days ago
• 
This API is great for building new variants. Did you also consider adding an API that allows modifying existing Variant values? We would need to add a function to VariantBuilder similar to FindObjectField from the decoding PR, which would "move"| the context of VariantBuilder to a specific place/field. Once called, you would then be able to override the existing value.

@qzyu999	Reply...
