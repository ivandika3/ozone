<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Recursive Bucket Forks Design

## Summary

Recursive bucket forks let a fork bucket be used as the source for another
fork. The feature must be explicit: a child fork should not depend on accidental
snapshot behavior of the parent fork bucket. Instead, OM should model fork
lineage as retained base views, where each fork has local metadata and
tombstones over a base view, and that base view can itself reference another
base view.

This keeps recursive forks cheap and copy-on-write, while making retention,
listing, deletion, and restart behavior predictable.

## Goals

- Support `fork -> fork -> fork` creation through the same bucket fork RPCs.
- Preserve isolation: writes, deletes, ACL/tag/time mutations, and renames in a
  child fork must not affect any parent fork or original source bucket.
- Preserve point-in-time behavior: a child fork sees the parent fork exactly as
  it looked when the child was created.
- Retain all metadata needed by descendants even after an ancestor fork bucket
  is deleted.
- Keep existing non-recursive fork behavior unchanged.
- Avoid full metadata or block copies when creating recursive forks.

## Non-Goals

- Merge, rebase, and conflict resolution remain outside this design.
- Cross-cluster or cross-OM fork lineage is not included.
- Recursive fork flattening is not required for the first implementation.
- Snapshot diff across recursive fork lineage is future work unless existing
  APIs work naturally after lineage is represented.

## Current Gap

The current MVP records one `baseSnapshotId` in `BucketForkInfo` and assumes
that the fork base is a snapshot of a non-fork source bucket. A source bucket
that is itself a fork is not explicitly rejected, but it is also not modeled as
a supported recursive lineage.

The risk is that a normal snapshot of a fork bucket can miss inherited entries,
because a fork bucket physically stores fork-local deltas and tombstones rather
than every visible key from its base. Recursive forks need to preserve the
overlay relationship, not just the target bucket table entry.

## Alternatives

### Reject Fork-of-Fork

Reject source buckets that are active forks.

This is the smallest safe MVP guard, but it does not meet the desired recursive
fork behavior and diverges from systems such as Tigris and Neon that treat
recursive branching as a normal operation.

### Materialize Parent View

At child fork creation, copy all parent-visible metadata into the child bucket
namespace and use the child as a flat fork.

This makes reads simple but defeats the cheap zero-copy design. It also makes
fork creation proportional to bucket size and creates expensive metadata churn.

### Preserve Lineage With Base Views

Represent each fork base as a durable base-view node. A base view captures a
snapshot layer plus an optional parent base view. Child forks point to a base
view instead of assuming a single flat snapshot.

This is the chosen design. It keeps fork creation metadata-only, makes
retention explicit, and matches the existing overlay architecture.

## Data Model

Add a new OM table:

```text
bucketForkBaseViewTable:
  baseViewId -> BucketForkBaseViewInfo
```

`BucketForkBaseViewInfo` fields:

- `baseViewId`: UUID for this base view.
- `sourceVolumeName`, `sourceBucketName`: bucket whose visible state was forked.
- `sourceBucketObjectId`: source bucket object ID at fork creation time.
- `snapshotId`: snapshot for this lineage layer.
- `snapshotName`: named or internal snapshot used for this layer.
- `parentBaseViewId`: optional UUID of the source fork's base view.
- `sourceForkId`: optional UUID when the source bucket is an active fork.
- `creationTime`, `updateId`, `status`.
- `refCount` or equivalent reverse-reference metadata for retention.

Update `BucketForkInfo`:

- Keep `baseSnapshotId` and `baseSnapshotName` for compatibility and simple
  non-recursive display.
- Add `baseViewId`.
- Add `parentForkId` or `sourceForkId` for lineage inspection.
- Add `lineageDepth` for bounds and operational diagnostics.

The source of truth for overlay traversal is `baseViewId`.

## Fork Creation

When creating a fork from a non-fork bucket:

1. If the request names a snapshot, use that snapshot.
2. If the request uses the active bucket, create the hidden internal base
   snapshot.
3. Create a base view with no `parentBaseViewId`.
4. Create the target bucket and `BucketForkInfo` pointing to that base view.

When creating a fork from an active fork bucket:

1. Resolve bucket links before fork creation, as in the current MVP.
2. Detect that the resolved source bucket is an active fork.
3. Create a snapshot layer that captures the source fork's current
   fork-local metadata and tombstones.
4. Create a base view with `parentBaseViewId` set to the source fork's
   `baseViewId`.
5. Create the target bucket and `BucketForkInfo` pointing to the new base view.

This captures the source fork's visible point-in-time state as:

```text
new child local state
  -> source fork local snapshot layer
  -> source fork parent base view
```

The parent fork bucket itself is not required for child reads after the child
base view is created.

## Read Overlay

`BucketForkManager` should resolve reads through a bounded base-view chain.

Lookup:

1. Check current fork-local key/file/directory tables.
2. Check current fork tombstones.
3. Read from the current base view snapshot layer.
4. If unresolved, repeat against `parentBaseViewId`.
5. Rewrite any returned metadata into the child fork namespace before returning.

List:

1. Read fork-local entries for the requested page.
2. Walk base views from child to parent.
3. Merge by logical path in sorted order.
4. Child entries shadow parent entries.
5. Child tombstones hide parent entries.
6. Pagination must overfetch enough from each layer to pass dense tombstones and
   shadowed names.

The implementation should keep a configurable maximum lineage depth. The first
implementation can default to a conservative value, such as 32, and return a
clear error if a request would exceed it.

## Writes and Mutations

Writes always land in the active target fork bucket.

For base-visible entries found through any ancestor base view:

- Overwrite creates a fork-local shadow in the current fork only.
- Delete writes a tombstone in the current fork only.
- Metadata mutations perform copy-on-write into the current fork namespace.
- Rename of a base-visible entry clones metadata into the current fork,
  tombstones the old logical path in the current fork, and writes the renamed
  metadata in the current fork.

No mutation is written into ancestor fork buckets or ancestor base views.

## Deletion and Retention

Snapshots and base views referenced by descendants must be retained.

Deleting a fork bucket:

1. Deletes the active target bucket metadata.
2. Deletes the fork's local tombstones.
3. Deletes the `BucketForkInfo`.
4. Releases the fork's reference to its `baseViewId`.
5. Does not delete base views or snapshots still referenced by child forks.

Deleting a snapshot:

- Reject deletion when the snapshot is referenced by any active or retained
  base view.

Garbage collection:

- A base view can be removed only when no active fork or descendant base view
  references it.
- Removing a base view releases its snapshot reference and then recursively
  releases its parent base view reference.

## Quota Accounting

Fork quota continues to represent the visible namespace of the active fork, not
physical block ownership.

Creation baseline:

- Non-recursive fork: initialize from the source snapshot referenced size and
  source used namespace, matching current behavior.
- Recursive fork: initialize from the visible usage of the source fork at the
  child creation point. This may require storing visible usage on the source
  fork and carrying it into the child base view.

Mutation deltas:

- Fork-local writes and deletes use existing quota paths.
- Tombstones for base-visible entries decrement visible namespace and bytes in
  the current fork only.
- Deleting a fork-local shadow over a tombstoned base entry reclaims fork-local
  blocks but does not remove the tombstone.

## Error Handling

- If source lineage depth would exceed the configured maximum, fail fork
  creation with a clear OMException.
- If a base view references a missing snapshot, reads and writes fail with a
  metadata corruption style error rather than silently falling through.
- If a source fork is being deleted concurrently, lock ordering must make fork
  creation either see the active fork and retain its base view, or fail cleanly.

## Compatibility and Migration

Existing non-recursive forks can be migrated lazily:

- If `BucketForkInfo.baseViewId` is absent, synthesize a base view from
  `baseSnapshotId` on OM startup or during metadata upgrade.
- Keep `baseSnapshotId` populated for CLI/API compatibility.
- New fork creation always writes a base view.

The CLI and RPC shape can stay the same for creation. `GetBucketForkInfo` and
`ListBucketForks` should expose lineage fields when present.

## Testing

Unit tests:

- `BucketForkBaseViewInfo` codec and protobuf conversion.
- OM DB table definition and upgrade/migration handling.
- Base-view reference acquire/release behavior.
- Lineage depth limit.

Request tests:

- Create fork from fork active state.
- Create fork from named snapshot of a fork.
- Reject deletion of snapshots referenced only through descendant base views.
- Delete parent fork while child fork still reads inherited data.
- Delete child fork releases base-view references without affecting parent.

OBS behavior tests:

- Child fork sees parent fork local writes at child creation time.
- Child fork does not see parent fork writes after child creation.
- Child fork inherits original source base entries through parent lineage.
- Child fork tombstone hides inherited ancestor entry.
- Parent fork deletion does not break child lookup/list.

FSO behavior tests:

- File lookup across two or more lineage layers.
- Directory listing merge across child, parent, and original base.
- Directory tombstone hides ancestor subtree.
- Rename and metadata copy-on-write from ancestor-visible files.

Integration tests:

- Multi-level fork chain survives OM restart.
- Multiple children share the same parent base view.
- Base-view cleanup occurs after deleting all descendants.
- Snapshot deletion is rejected while referenced by any retained base view.

## References

- Tigris supports fork-of-fork semantics for object storage forks.
- Neon supports creating branches from previously created branches.
