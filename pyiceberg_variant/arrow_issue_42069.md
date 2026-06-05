# [Format] Consider adding an official variant type to Arrow

**State:** closed
**Created by:** @CurtHagenlocher
**Created at:** 2024-06-10 16:14:18.000 UTC

### Describe the enhancement requested

This could be aligned with the new [Spark variant type](https://github.com/apache/spark/blob/master/common/variant/README.md) or it could not be.



### Component(s)

Format

---

### Comment by @zeroshade at 2024-06-10 19:01:16.000 UTC

What would be the benefit of this over the current Union types? Is it just to alleviate the need to specify all the types up front?

---

### Comment by @CurtHagenlocher at 2024-06-10 19:26:52.000 UTC

> What would be the benefit of this over the current Union types? Is it just to alleviate the need to specify all the types up front?

That's part of it, yes, but many sources also support variants containing semistructured types where you could conceivably need a combinatorial explosion of unions to reflect all the data. (See [https://docs.snowflake.com/en/sql-reference/data-types-semistructured](https://docs.snowflake.com/en/sql-reference/data-types-semistructured), for instance.)

---

### Comment by @wjones127 at 2024-06-12 01:29:31.000 UTC

I’m investigating using the Spark spec as an extension type in DataFusion. I’ll report back here whether it turns out to work well with Arrow layouts.



---

### Comment by @wjones127 at 2024-06-13 16:05:05.000 UTC

## Notes from discussion with original developers

I talked to developers at Databricks who worked on adding this feature to Spark and Delta Lake. Here a few notes from that.

- This is being added as a data type in Spark and Delta Lake. They intend to add this data type to Iceberg as well.
- They called it the “Open Variant Data Type” with the intention that this data type would proliferate to other systems.
    - They have a standalone Java library that implements the data type. That’s the Java code at https://github.com/apache/spark/tree/master/common/variant
- The data is stored as two binary fields: one to hold a string dictionary, the other to hold the binary representation of the values. It is generally kept as binary data in memory, but engines are free to manipulate it as they wish.
- The eventual plan is to support record shredding, where fields that have dense values will be split out into their own columns. This allows row group / page pruning to happen with normal Parquet statistics / indices.
    - Record shredding will have to be the same per Parquet file, but could be different between files.
    - Once in memory, variants will be either recombined into the two binary columns or else have been selected back into their fully shredded forms. This is because most engines will require a common schema across files. The good news here is that means by the time it might be exported into Arrow data, we wouldn’t have to worry about the shredding.
- Performance justification: JSON and BSON are not designed for OLAP queries.
    - The canonical pathological case is where you are extracting the last field in a large object. JSON has to do `O(n)` string comparisons, the variant form replaces them with integer comparisons.
    - The main performance optimization is that object keys (and other common strings) are pulled out into a common string dictionary. This reduces the size, but also replaces all the string comparisons needed in field lookups with integer comparisons.

---

### Comment by @wjones127 at 2024-06-13 16:13:49.000 UTC

## An Arrow extension type?

In the near term, I think this would make a good Arrow extension type. This would be:

```
struct<
  metadata: dictionary<binary>,
  data: binary
>
```

The metadata will usually be a single binary shared across all rows, but could be multiple. (Multiple might happen if two different batches are concatenated together, for example.) Either dictionary or REE encoded array would be appropriate.

The data could be either binary, large binary, or binary view.

Binary view isn’t widely supported right now, but could be very useful for this data type. This is because sub-objects can be sliced out of variants. From the spec [^1]:

> Another motivation for the representation is that (aside from metadata) each inner Variant value is contiguous and self-contained. For example, in a Variant containing an Array of Variant values, the representation of an inner Variant value, when paired with the metadata of the full variant, is itself a valid Variant.

[^1]: https://github.com/apache/spark/blob/master/common/variant/README.md

## Where could this be useful?

A few immediate places I think this extension type could be useful:

- Roundtrip variant Arrow ↔ Spark
    - Spark Connect (and any ADBC connector to that) would benefit from this
- Extension type in PyArrow, roundtrip PySpark ↔ PyArrow
- DataFusion function library (I’m experimenting with that now)
  * There's been substantial interest in DataFusion community for a way to handle semi-structured data efficiently.

## Extension type pitfalls

The main pitfall of using an extension type for this is the storage type is meaningless to users. They need to have special libraries to interpret the bytes if pulled into a system that doesn't understand the variant extension type.

In addition, most existing Arrow systems I've worked with don't have a way to customize how extension arrays are printed. I think this is something we should fix. A reasonable workaround in the meantime is providing functions that convert these back to JSON strings for the purpose of printing.

---

### Comment by @emkornfield at 2024-06-14 01:19:58.000 UTC

I think this makes sense as a extension type.  I think given subcolumnarization work happening one might also want to store a union in the type as well for columns that have been split out

---

### Comment by @emkornfield at 2024-06-14 03:55:46.000 UTC

One other thought, I think the variant type in spark has a more limited type surface then Arrow, that is potentially something that might need reconciling

---

### Comment by @CurtHagenlocher at 2024-06-14 14:58:47.000 UTC

Yeah, I think there are really two different requests possible here: an Arrow-native variant type and a Spark-compatible variant type.   The surface area thing works both ways: like Parquet, the Spark variant supports 32-bit and 64-bit decimal values while Arrow does not. 

---

### Comment by @wjones127 at 2024-06-15 04:39:44.000 UTC

> Yeah, I think there are really two different requests possible here: an Arrow-native variant type and a Spark-compatible variant type.

I think it’s too early to say whether an Arrow-native one makes sense. The Spark / delta lake teams have intentions that their standard will proliferate to other engines. At which point it will not be a Spark specific thing and might make more sense to align with these types. If that succeeds, it would make sense for us to align with the format. The Open Variant Data Type has a `version` field, so it could be amenable to expansions of the types in there. 

If this standard doesn’t proliferate to other engines and ends up being Spark specific while other engines maintain different standards, we will have to have a conversation about what kind of variant type would make a good interchange format. That would be a point where Arrow designing its own format would make sense.

Either way, it’s much too early to know which direction to go. Spark 4.0 isn’t even release yet. I think this is the stage where we should experiment with this type as a non-canonical extension type and keep an eye on the data types adoption in the wider ecosystem.



---

### Comment by @emkornfield at 2024-06-15 14:56:33.000 UTC

Yeah, fwiw there is an iceberg proposal to also support variant type and if IIUC the current incarnation is to support spark with iceberg types but it hasn't made it very far yet

---

### Comment by @CurtHagenlocher at 2024-06-15 15:06:32.000 UTC

Even if there were no Spark (or Iceberg) variant type there would still be variants stored in databases and it would be nice for ADBC to be able to return those in a somewhat-consistent fashion. I suppose ADBC could define its own extension type for this purpose.

---

### Comment by @CurtHagenlocher at 2024-06-15 15:12:02.000 UTC

For curious observers, there's a thread about the Iceberg proposal at [https://lists.apache.org/thread/xnyo1k66dxh0ffpg7j9f04xgos0kwc34](https://lists.apache.org/thread/xnyo1k66dxh0ffpg7j9f04xgos0kwc34) and the proposal itself at [https://docs.google.com/document/d/1QjhpG_SVNPZh3anFcpicMQx90ebwjL7rmzFYfUP89Iw/edit#heading=h.rt0cvesdzsj7](https://docs.google.com/document/d/1QjhpG_SVNPZh3anFcpicMQx90ebwjL7rmzFYfUP89Iw/edit#heading=h.rt0cvesdzsj7).

---

### Comment by @alamb at 2024-06-19 10:09:41.000 UTC

> Binary view isn’t widely supported right now, but could be very useful for this data type. This is because sub-objects can be sliced out of variants. From the spec [1](https://github.com/apache/arrow/issues/42069#user-content-fn-1-04106abe6698b8139e7deb45de91480f):

BTW we are actively working on implementing StringView / BinaryView support in arrow-rs https://github.com/apache/arrow-rs/issues/5374 and DataFusion https://github.com/apache/datafusion/issues/10918 and thanks to @XiangpengXao, @Weijun-H  and other we are making good progress

---

### Comment by @ajantha-bhat at 2024-11-15 16:49:16.000 UTC

Parquet recently added the variant spec and variant data type. (Iceberg folks decided that it would be better to maintain the spec in parquet instead of Iceberg)
https://github.com/apache/parquet-format/blob/master/VariantEncoding.md

Is there any plans in arrow to adopt it soon?

---

### Comment by @alamb at 2024-11-15 20:51:06.000 UTC

@julienledem and I were just talking about this. I agree it would be nice to add the variant type to arrow -- I think the challenge will be finding people willing to help implement it in two languages. I don't think I will have time to help with Rust anytime soon, though I can help coordinate and I'll see if I can muster anyone to help. 

I filed this to track the ideae
-  https://github.com/apache/arrow-rs/issues/6736



---

### Comment by @laurentgo at 2024-11-15 21:19:36.000 UTC

What would be required on the Arrow side of things? Just an extension, or would we also need methods to access/manipulate the content as well?

---

### Comment by @emkornfield at 2024-11-18 18:21:48.000 UTC

I think the extension type is potentially not super useful without some methods to manipulate it.  Note, the Parquet spec is still in experimental stage.  I think having an extension type that mirrors the parquet spec once it is ready makes sense.

---

### Comment by @adriangb at 2025-03-04 02:16:58.000 UTC

> The main pitfall of using an extension type for this is the storage type is meaningless to users. They need to have special libraries to interpret the bytes if pulled into a system that doesn't understand the variant extension type.

FWIW two tricks we've employed with relatively good results:
- Add metadata to the Field to say "treat this as json" and when serializing out it gets converted to plain utf8 json.
- Keep this optimized data as a duplicate/private column so that `select json_column` pulls through the plain json data but `where json_data->'foo' = 1` gets rewritten to `where variant_get(_private_variant_col, 'foo') == 1`

The latter is particularly helpful to keep backward compatibility.

---

### Comment by @adriangb at 2025-03-04 02:55:42.000 UTC

A thought on data shredding / subcolumarizarion.

My understanding is that in parquet it is being stored as truly as individual columns. Should the arrow type need to collapse / collect them? In particular it seems important to me that a query like `variant_get(col, 'shredded-key')` never have to touch the potentially much larger unshredded data. There is little value in shredding if the data gets recombined at the arrow layer, you already had to pay the price of downloading the data, decoding parquet, etc. Since it seems to me the query engine will always have to be aware of the shredding, could arrow just avoid dealing with shredding altogether and leave that up to the query engine? The query engine would have to know to rewrite queries to hit the shredded data or reconstitute it if needed. I think that would make it easier to filter push down, stats pruning, etc to "just work".

---

### Comment by @gszadovszky at 2025-04-30 09:21:14.000 UTC

> (...) could arrow just avoid dealing with shredding altogether and leave that up to the query engine?

What does it mean exactly? Arrow want to avoid representing the shredded values altogether or represent as is (according to the variant specification)?
For the first approach, a variant vector would be similar to a struct vector with varbinary fields for `metadata` and `value`. For the latter approach we would need an additional arbitrary typed field of `typed_value`.
I think handling `typed_value` makes handling the values quite complicated. For example two different Parquet files might use different schema for shredding so two variant vectors would have different schema as well. 



---

### Comment by @adriangb at 2025-04-30 12:35:53.000 UTC

>  I think handling `typed_value` makes handling the values quite complicated. For example two different Parquet files might use different schema for shredding so two variant vectors would have different schema as well.

Yes that's precisely my point.

I think each query engine will have to play a game of:
- My predicate is `variant_get(col, 'key')` (assume this is some SQL written by a user)
- For each file, does `col.typed_value.key` exist?
- If so my predicate to `col.typed_value.key`
- Do query engine stuff like stats pruning, etc.

In other words, I think `variant_get` could handle variant shredding as a nice to have / fallback but I'd guess query engines will have to special case variant shredding anyway to get stats pruning, late materialization, etc. Otherwise they'd be forced to always read the entire column and hand that to `variant_get` which pretty much defeats the point of shredding.

---

### Comment by @gszadovszky at 2025-04-30 12:58:21.000 UTC

Sounds good to me. Thanks for clarifying.


---

### Comment by @alamb at 2025-06-25 16:38:10.000 UTC

Here is a related mailing list discussion as well:
- https://lists.apache.org/thread/w06cxdojjcmry4m9vb0bo7owd1jsbtz5

---

### Comment by @alamb at 2025-10-14 12:15:20.000 UTC

Given we have added a canonical extension type, it might make sense to close this issue:
- https://github.com/apache/arrow/issues/46908

---

