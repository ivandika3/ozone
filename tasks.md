# Apache Ozone OM Bucket Fork Tasks

Tracking document for the HDDS-15120 bucket fork MVP branch.

## Current Status

- Branch: `research-bucket-forks-feasibility`
- Remote: `origin/research-bucket-forks-feasibility`
- Last completed slice: active-source fork creation and MiniOzoneCluster
  integration coverage.
- Capability state: MVP request/client/overlay flow is end-to-end covered for
  OBS and FSO basics; merge/rebase/conflict resolution remains unsupported.

## Completed

- [x] Add fork metadata request classes and OM Ratis wiring.
- [x] Add `BucketForkInfo` metadata and bucket fork DB table definitions.
- [x] Add experimental config gate for bucket forks.
- [x] Add fork tombstone metadata and tombstone table definitions.
- [x] Add client protocol, translator, `ObjectStore`, and `RpcClient` APIs.
- [x] Add shell commands for create, delete, info, and list bucket forks.
- [x] Create target fork buckets from active buckets and named snapshots.
- [x] Reject deletion of snapshots referenced by active forks.
- [x] Add key lookup overlay for fork buckets.
- [x] Add `getKeyInfo` overlay for fork buckets.
- [x] Add `listKeys` overlay merge for fork-local and base snapshot keys.
- [x] Add key delete tombstones for base-visible fork keys.
- [x] Add multi-key delete tombstones for base-visible fork keys.
- [x] Add OBS copy-on-write metadata mutations for base-visible fork keys.
- [x] Add overwrite-shadowing coverage for base-visible fork keys.
- [x] Add OBS rename support for base-visible fork keys.
- [x] Add FSO exact file lookup/status fallback to base snapshots.
- [x] Add FSO file listing overlay merge for fork-local and base statuses.
- [x] Add subtree hiding for base-visible entries under fork directory/prefix
  tombstones.
- [x] Add FSO single-key delete tombstones for base-visible fork files.
- [x] Add FSO rename support for base-visible fork files.
- [x] Decrement fork visible namespace quota when OBS/FSO deletes hide
  base-visible keys with fork tombstones.
- [x] Decrement fork visible byte quota when OBS/FSO deletes hide
  base-visible keys with fork tombstones.
- [x] Preserve fork visible namespace and byte quota when OBS/FSO overwrites
  shadow base-visible keys.
- [x] Add OBS/FSO quota coverage for fork-local writes and deletes.
- [x] Harden OBS/FSO overlay listing pagination when base pages contain dense
  tombstones or fork-shadowed entries.
- [x] Cover bucket fork deletion cleanup releasing snapshot references and
  removing target bucket/tombstone metadata.
- [x] Add active-source fork creation with hidden internal base snapshots.
- [x] Hide internal active-source base snapshots from normal snapshot listings.
- [x] Classify bucket fork info/list RPCs as read-only and merge fork listing
  results from OM table cache plus RocksDB.
- [x] Add MiniOzoneCluster integration coverage for named-snapshot forks,
  active-source forks, multi-fork sharing, restart reload, and fork deletion.
- [x] Add unit/request tests for completed metadata, RPC, client, shell, lookup,
  list overlay, delete/tombstone, metadata mutation, OBS rename, FSO exact file
  lookup, FSO file listing, and FSO rename slices.

## Next Major Slices

- [x] Implement fork delete/tombstone write path.
  - [x] Detect deletes against base-visible keys in fork buckets.
  - [x] Write `BucketForkTombstoneInfo` instead of queuing base blocks for
    physical deletion.
  - [x] Preserve existing deleted-table flow for fork-local keys.
  - [x] Add OBS tests for deleting fork-local keys and tombstoning base keys.

- [x] Implement copy-on-write metadata mutations.
  - [x] Clone base-visible key metadata into the fork namespace before mutation.
  - [x] Apply set-times, tags, ACL, and similar mutations to fork-owned metadata.
  - [x] Ensure overwrites shadow base entries without copying base data blocks.
  - [x] Add request tests for COW mutation paths.

- [x] Implement rename behavior for fork buckets.
  - [x] Rename fork-local entries through existing OM flows.
  - [x] For base-visible entries, copy metadata into the fork namespace, create
    tombstones for old logical paths, then apply rename semantics.
  - [x] Add OBS rename tests.

- [x] Extend overlay support to FSO/file APIs.
  - [x] Add `lookupFile` and `getFileStatus` fall-through behavior.
  - [x] Add directory/file listing merge behavior.
  - [x] Define directory tombstone behavior for base-visible directories.
  - [x] Add FSO tests for file lookup, directory listing, and delete.
  - [x] Add FSO rename tests and behavior.

- [x] Implement quota accounting for visible fork namespace.
  - [x] Initialize fork usage from base snapshot visible usage.
  - [x] Adjust usage for fork-local writes and deletes.
  - [x] Adjust visible namespace usage for base-entry tombstones.
  - [x] Adjust visible byte usage for base-entry tombstones.
  - [x] Adjust visible usage for base-visible overwrite shadowing.
  - [x] Add quota tests for OBS and FSO flows.

- [x] Harden listing and pagination semantics.
  - [x] Add tests for duplicate names, tombstoned entries near page boundaries,
    non-empty `startKey`, non-empty `keyPrefix`, and truncated base/fork pages.
  - [x] Revisit overfetch strategy if one extra item is insufficient for dense
    tombstone or duplicate cases.

- [x] Add MiniOzoneCluster integration coverage.
  - [x] Source and fork mutate independently.
  - [x] Multiple forks share the same base snapshot.
  - [x] OM restart reloads fork metadata and snapshot references.
  - [x] Fork deletion releases base snapshot references.
  - [x] Internal active-source base snapshots are hidden from normal listings.

- [x] Complete fork deletion cleanup.
  - [x] Release base snapshot reference by deleting the active fork metadata.
  - [x] Delete fork metadata and tombstone metadata.
  - [x] Delete the target bucket as mandatory MVP fork deletion behavior.

## Design Follow-Ups

- [ ] Confirm whether `getKeyInfo` fallback should call snapshot `getKeyInfo`
  instead of `lookupKey` for exact parity with non-fork behavior.
- [ ] Decide how much of the overlay should live in `BucketForkManager` versus
  request-specific helpers as FSO and mutation paths grow.
- [ ] Document admin/user semantics for fork bucket lifecycle.
- [ ] Document unsupported MVP behavior: merge, rebase, conflict resolution,
  and snapshot diff for forks.

## Verification Checklist Per Slice

- [ ] Add or update focused unit/request tests first.
- [ ] Run focused Maven test for the touched class or request path.
- [ ] Run broader bucket fork test selection.
- [ ] Run checkstyle for touched modules.
- [ ] Run `git diff --check`.
- [ ] Commit with an `HDDS-15120` message.
- [ ] Push to `origin/research-bucket-forks-feasibility`.
