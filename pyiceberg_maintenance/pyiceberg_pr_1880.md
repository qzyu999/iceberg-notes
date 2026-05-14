# Added ExpireSnapshots Feature

**State:** closed
**Created by:** @ForeverAngry
**Created at:** 2025-04-03 19:18:23.000 UTC





## Summary

This PR Closes issue #516 by implementing support for the `ExpireSnapshot` table metadata action.

## Rationale

The `ExpireSnapshot` action is a core part of Iceberg’s table maintenance APIs. Adding support for this action in PyIceberg helps ensure feature parity with other language implementations (e.g., Java) and supports users who want to programmatically manage snapshot retention using PyIceberg’s public API.

## Testing

- Unit tests have been added to cover the initial expected usage paths.
- Additional feedback on edge cases, missing test scenarios or corrections to the setup test logic is greatly welcome during the review process.

## User-facing changes

- This change introduces a new public API: `ExpireSnapshot`.
- No breaking changes or modifications to existing APIs were made.

---

---

### Comment by @ForeverAngry at 2025-04-13 06:17:33.000 UTC

After looking at the way the action [here](https://github.com/apache/iceberg-python/blob/15887011cb6bae9a4408eedc2824133efa3e1599/pyiceberg/table/update/__init__.py#L471

```python
def _(update: RemoveSnapshotsUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
```) was implemented, I refined the changes.  Let me know if these make sense :)

---

### Comment by @ForeverAngry at 2025-05-11 00:45:51.000 UTC

> @ForeverAngry Could you see if you can get the linters/tests passing? Thanks!



> @ForeverAngry Could you see if you can get the linters/tests passing? Thanks!

I was able to get the linting figured out.  I dont have docker on this machine, so im going to give the integration tests a try in codespaces to see if i can get away doing the checks there. 

---

### Comment by @Fokko at 2025-05-16 21:04:50.000 UTC

@ForeverAngry Sorry for the late reply, it looks like that there is a test failing now 👀 

---

### Comment by @ForeverAngry at 2025-05-17 18:48:10.000 UTC

> @ForeverAngry Sorry for the late reply, it looks like that there is a test failing now 👀

I think this [commit](https://github.com/apache/iceberg-python/pull/1880/commits/348831485a626e1483e131077e8f6a1892d72993)  should fix the test error, i also added additional tests.  All passed  - and appear to be in-good-order.  🤞 this time is the charm.

---

### Comment by @zschumacher at 2025-06-11 01:41:31.000 UTC

this would be great - whats the status of this?

---

### Comment by @smaheshwar-pltr at 2025-06-11 10:49:41.000 UTC

I see that in https://github.com/apache/iceberg-python/pull/1958, for orphaned file removal, we decided to have a `table.maintenance` API returning a `MaintenanceTable`. As a user, if I have to do orphaned file removal via that `table.maintenance` API, but snapshot expiration instead via `table.expire_snapshots`, I might be confused (they both feel like table maintenance to me, though snapshot expiration is admittedly a bit stronger). Curious about people's thoughts here.

---

### Comment by @ForeverAngry at 2025-06-11 16:23:01.000 UTC

Well, right now this pr doesn't do anything with the newly orphaned files.  It just handles the metadata operation.

---

### Comment by @Fokko at 2025-06-11 18:04:58.000 UTC

> I see that in https://github.com/apache/iceberg-python/pull/1958, for orphaned file removal, we decided to have a table.maintenance API returning a MaintenanceTable. As a user, if I have to do orphaned file removal via that table.maintenance API, but snapshot expiration instead via table.expire_snapshots, I might be confused (they both feel like table maintenance to me, though snapshot expiration is admittedly a bit stronger). Curious about people's thoughts here.

Yes, I agree with you there. Before doing the 0.10.0 release, we need to ensure we align on this and make proper docs. I have a slight preference towards `.mainenance` to have a clear distinction between maintenance and the regular operations (such as creating a tag or branch).

---

### Comment by @ForeverAngry at 2025-06-11 21:44:28.000 UTC

I'm happy to follow the '.maintenance' api design if there is a strong preference toward it.

---

### Comment by @ForeverAngry at 2025-06-19 16:13:58.000 UTC

> Thanks @ForeverAngry for working on this, and I think it is ready to go 👍

Great!  I think @kevinjqliu is still listed as needing approval.  @kevinjqliu can you put your stamp on this as well?

---

### Comment by @Fokko at 2025-06-19 19:49:42.000 UTC

@ForeverAngry I think we can move this one forward. Before the release, we need to follow up on two things:

- Add a new Maintenance doc section with a subsection that explains the expire snapshots operation.
- Move the expire snapshots operation under maintenance: `tbl.maintenance.expire_snapshots()`

---

### Comment by @Fokko at 2025-06-19 19:50:11.000 UTC

Thanks again @ForeverAngry for working on this 🚀 

---

### Comment by @ForeverAngry at 2025-06-19 22:02:40.000 UTC

> Thanks again @ForeverAngry for working on this 🚀 

Thank you, for being such a supportive and inspiring member to work with!

---

### Comment by @ForeverAngry at 2025-06-19 22:04:19.000 UTC

> @ForeverAngry I think we can move this one forward. Before the release, we need to follow up on two things:
> 
> - Add a new Maintenance doc section with a subsection that explains the expire snapshots operation.
> - Move the expire snapshots operation under maintenance: `tbl.maintenance.expire_snapshots()`

I'll work on this, this weekend!

---

### Comment by @Fokko at 2025-06-20 21:48:40.000 UTC

@ForeverAngry Appreciate that, thanks! 🙌 

---

### Comment by @greenlaw at 2025-06-24 19:09:52.000 UTC

@ForeverAngry  Thank you for this feature ❤️   

Just one question/comment:  It seems this only supports expiration time/age, and does not support other [retention policies](https://iceberg.apache.org/docs/1.9.1/java-api-quickstart/#updating-retention-properties). For example, the Java API's ExpireSnapshots supports [retainLast](https://iceberg.apache.org/javadoc/0.12.0/org/apache/iceberg/ExpireSnapshots.html#retainLast(int)), and ManageSnapshots supports [setMinSnapshotsToKeep](https://iceberg.apache.org/javadoc/0.14.0/org/apache/iceberg/ManageSnapshots.html#setMinSnapshotsToKeep(java.lang.String,int)).  Any plans to add support for these features, by chance?

---

### Comment by @ForeverAngry at 2025-06-24 20:53:14.000 UTC

> @ForeverAngry  Thank you for this feature ❤️   
> 
> Just one question/comment:  It seems this only supports expiration time/age, and does not support other [retention policies](https://iceberg.apache.org/docs/1.9.1/java-api-quickstart/#updating-retention-properties). For example, the Java API's ExpireSnapshots supports [retainLast](https://iceberg.apache.org/javadoc/0.12.0/org/apache/iceberg/ExpireSnapshots.html#retainLast(int)), and ManageSnapshots supports [setMinSnapshotsToKeep](https://iceberg.apache.org/javadoc/0.14.0/org/apache/iceberg/ManageSnapshots.html#setMinSnapshotsToKeep(java.lang.String,int)).  Any plans to add support for these features, by chance?

Yeah, those slipped my mind when I originally did it. I'd be happy to implement those. :)



---

