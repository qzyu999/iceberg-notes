# [Format] Add an Arrow Canonical Extension Type for Parquet Variant

**State:** closed
**Created by:** @alamb
**Created at:** 2025-06-25 18:55:34.000 UTC

### Describe the enhancement requested

Parquet has added a new type for semi-structured data called `Variant` which is defined here:
* Variant encoding spec: https://github.com/apache/parquet-format/blob/main/VariantEncoding.md
* Variant shredding spec: https://github.com/apache/parquet-format/blob/main/VariantShredding.md

As it is common for engines to read data from Parquet into Arrow for in memory processing it is useful to have support for Variant in Arrow. @CurtHagenlocher  proposes adding native Variant support in the Arrow format itself here:
- https://github.com/apache/arrow/issues/42069

An alternate approach is to add a [Canonical Extension Type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#canonical-extension-types) 

@zeroshade wrote up a proposal
- Mailing List Discussion: https://lists.apache.org/thread/w06cxdojjcmry4m9vb0bo7owd1jsbtz5
- Google Document: https://docs.google.com/document/d/1pw0AWoMQY3SjD7R4LgbPvMjG_xSCtXp3rZHkVp9jpZ4/edit?usp=sharing

And implemented an implementation in Go
- https://github.com/apache/arrow-go/commit/5240503993cc0aa47554b932c341e4940ce42348

This ticket tracks the idea of adding Variant as an official extension type

See also @neilechao 's PR to add variant read support to parquet
- https://github.com/apache/arrow/issues/45937

### Component(s)

Format

---

### Comment by @alamb at 2025-06-25 18:55:59.000 UTC

Most recent update from @zeroshade  on the mailing list is:

> I've
> been waiting for one of the other implementations to implement the proposal
> [2] before I go make a PR to add it to the docs in full. If you think it's
> worthwhile for me to start drafting up a PR to add to the Canonical
> Extensions right now then I'm happy to do so. I think most of the
> objections to using an extension type instead of a real type are answered
> or managed by the Proposal [2] and ensuring the extension type has
> appropriate functional support and methods.

---

### Comment by @ianmcook at 2025-09-17 20:55:09.000 UTC

Closed by https://github.com/apache/arrow/pull/47456

---

