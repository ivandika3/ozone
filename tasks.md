# Apache Ozone OM Bucket Fork Tasks

Tracking document for the HDDS-15120 bucket fork MVP branch.

## Current Status

- Branch: `research-bucket-forks-feasibility`
- Remote: `origin/research-bucket-forks-feasibility`
- Last completed slice: bucket fork directory tombstone subtree handling.
- Capability state: early MVP skeleton, not yet end-to-end ready.

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
- [x] Add unit/request tests for completed metadata, RPC, client, shell, lookup,
  list overlay, delete/tombstone, metadata mutation, OBS rename, FSO exact file
  lookup, and FSO file listing slices.

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

- [ ] Extend overlay support to FSO/file APIs.
  - [x] Add `lookupFile` and `getFileStatus` fall-through behavior.
  - [x] Add directory/file listing merge behavior.
  - [x] Define directory tombstone behavior for base-visible directories.
  - [ ] Add FSO tests for file lookup, directory listing, delete, and rename.

- [ ] Implement quota accounting for visible fork namespace.
  - [ ] Initialize fork usage from base snapshot visible usage.
  - [ ] Adjust usage for fork-local writes and deletes.
  - [ ] Adjust visible usage for base-entry tombstones.
  - [ ] Add quota tests for OBS and FSO flows.

- [ ] Harden listing and pagination semantics.
  - [ ] Add tests for duplicate names, tombstoned entries near page boundaries,
    non-empty `startKey`, non-empty `keyPrefix`, and truncated base/fork pages.
  - [ ] Revisit overfetch strategy if one extra item is insufficient for dense
    tombstone or duplicate cases.

- [ ] Add MiniOzoneCluster integration coverage.
  - [ ] Source and fork mutate independently.
  - [ ] Multiple forks share the same base snapshot.
  - [ ] OM restart reloads fork metadata and snapshot references.
  - [ ] Fork deletion releases base snapshot references.
  - [ ] Internal active-source base snapshots are hidden from normal listings.

- [ ] Complete fork deletion cleanup.
  - [ ] Release base snapshot reference.
  - [ ] Delete fork metadata and tombstone metadata.
  - [ ] Decide whether target bucket deletion is mandatory, deferred, or
    separately controlled.

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
