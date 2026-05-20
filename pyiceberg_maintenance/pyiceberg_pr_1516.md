# Deletion Vectors read support

**State:** closed
**Created by:** @Fokko
**Created at:** 2025-01-15 10:18:55.000 UTC

First see if we can produce some deletion vectors.

Resolves #1549 

---

### Comment by @kevinjqliu at 2025-01-17 17:50:44.000 UTC

Sidenote, we dont have a way to read puffin files in python. I saw this PR from iceberg-rust, https://github.com/apache/iceberg-rust/pull/892
Perhaps this will be a good candidate for pyiceberg_core

---

### Comment by @Fokko at 2025-01-17 19:59:17.000 UTC

I want to try using the [Python `roaringbitmap`](https://pypi.org/project/roaringbitmap/) API first, otherwise, we can go the Iceberg-Rust route.

---

### Comment by @kevinjqliu at 2025-01-20 16:54:50.000 UTC

Looks like V3 is not supported on the pyiceberg side

```
=========================== short test summary info ============================
FAILED tests/integration/test_reads.py::test_read_table_with_deletion_vector[session_catalog_hive] - pyiceberg.exceptions.ValidationError: 1 validation error for TableMetadataWrapper
  Input tag '3' found using 'format_version' | 'format-version' does not match any of the expected tags: 1, 2 [type=union_tag_invalid, input_value={'format-version': 3, 'ta...c474ec.metadata.json'}]}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.10/v/union_tag_invalid
FAILED tests/integration/test_reads.py::test_read_table_with_deletion_vector[session_catalog] - pydantic_core._pydantic_core.ValidationError: 1 validation error for TableResponse
metadata
  Input tag '3' found using 'format_version' | 'format-version' does not match any of the expected tags: 1, 2 [type=union_tag_invalid, input_value={'format-version': 3, 'ta...26ab8a.metadata.json'}]}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.10/v/union_tag_invalid
==== 2 failed, 970 passed, 8 skipped, 2757 deselected in 346.14s (0:05:46) =====
make: *** [Makefile:55: test-integration] Error 1
Error: Process completed with exit code 2.
```

---

### Comment by @Fokko at 2025-01-20 17:54:23.000 UTC

@kevinjqliu That's correct, I've split that out here: https://github.com/apache/iceberg-python/issues/1540

---

### Comment by @Fokko at 2025-01-21 14:01:26.000 UTC

@kevinjqliu PR is out here: https://github.com/apache/iceberg-python/pull/1554 :)

---

### Comment by @kevinjqliu at 2025-02-01 00:33:04.000 UTC

do we want to include this as part of 0.9.0? the ability to read puffin files would be great! 

---

### Comment by @Fokko at 2025-02-03 10:27:48.000 UTC

@kevinjqliu I agree, but I don't think there is a lot of value in just supporting Puffin without having the ability to actually understand the content of the Puffin files. For the CI we're blocked on Iceberg-Java 1.8.0, until then I don't think there is much value of getting this in

---

### Comment by @kevinjqliu at 2025-03-26 20:39:42.000 UTC

except for the `MAX_JAVA_SIGNED` variable, everything else LGTM! 

---

