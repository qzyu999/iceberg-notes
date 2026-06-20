
Michał Komorowski <notifications@github.com> Unsubscribe
Jun 19, 2026, 1:50 AM (1 day ago)
to apache/arrow, me, Author

@misiek1984 commented on this pull request.

In cpp/src/arrow/extension/variant_internal.h:

> +#include <string_view>
+#include <vector>
+
+#include "arrow/result.h"
+#include "arrow/status.h"
+#include "arrow/util/visibility.h"
+
+namespace arrow::extension::variant_internal {
+
+/// \file variant_internal.h
+/// \brief Utilities for Variant binary encoding/decoding.
+///
+/// Implements parsing logic per the Variant Encoding Spec:
+/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
+///
+/// The "internal" in the filename refers to the binary encoding internals
I don't have a strong opinion here. But maybe instead of explaining in the comment what "internal" means it would be better to rename a file e.g. to variant_binary_encoding, variant_internal_encoding etc.

—
Reply to this email directly, view it on GitHub, or unsubscribe.
You are receiving this because you authored the thread.


Michał Komorowski <notifications@github.com>
Jun 19, 2026, 1:56 AM (1 day ago)
to apache/arrow, me, Author

@misiek1984 commented on this pull request.

In cpp/src/arrow/extension/variant_internal.h:

> +// ---------------------------------------------------------------------------
+
+/// Variant encoding spec version 1.
+constexpr uint8_t kVariantVersion = 1;
+
+/// Maximum nesting depth for recursive value decoding.
+/// Prevents stack overflow on deeply nested (possibly malicious) input.
+constexpr int32_t kMaxNestingDepth = 128;
+
+// ---------------------------------------------------------------------------
+// Enumerations
+// ---------------------------------------------------------------------------
+
+/// \brief Basic type codes from bits 0-1 of the value header byte.
+///
+/// Variant Encoding Spec §3: "Value encoding"
nit: The current version of the spec does not contain paragraph §3 and §3.1. I would just add a link to the section with tables: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types

—
Reply to this email directly, view it on GitHub, or unsubscribe.
You are receiving this because you authored the thread.

