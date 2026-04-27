# Apache Ozone OM Bucket Fork Tasks

Tracking document for the HDDS-15120 bucket fork MVP branch.

## Current Status

- Branch: `research-bucket-forks-feasibility`
- Remote: `origin/research-bucket-forks-feasibility`
- Last completed slice: `1535dfe58a9 HDDS-15120. Add bucket fork key list overlay`
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
- [x] Add unit/request tests for completed metadata, RPC, client, shell, lookup,
  and list overlay slices.

## Next Major Slices

- [ ] Implement fork delete/tombstone write path.
  - [ ] Detect deletes against base-visible keys in fork buckets.
  - [ ] Write `BucketForkTombstoneInfo` instead of queuing base blocks for
    physical deletion.
  - [ ] Preserve existing deleted-table flow for fork-local keys.
  - [ ] Add OBS tests for deleting fork-local keys and tombstoning base keys.

- [ ] Implement copy-on-write metadata mutations.
  - [ ] Clone base-visible key metadata into the fork namespace before mutation.
  - [ ] Apply set-times, tags, ACL, and similar mutations to fork-owned metadata.
  - [ ] Ensure overwrites shadow base entries without copying base data blocks.
  - [ ] Add request tests for COW mutation paths.

- [ ] Implement rename behavior for fork buckets.
  - [ ] Rename fork-local entries through existing OM flows.
  - [ ] For base-visible entries, copy metadata into the fork namespace, create
    tombstones for old logical paths, then apply rename semantics.
  - [ ] Add OBS rename tests.

- [ ] Extend overlay support to FSO/file APIs.
  - [ ] Add `lookupFile` and `getFileStatus` fall-through behavior.
  - [ ] Add directory/file listing merge behavior.
  - [ ] Define directory tombstone behavior for base-visible directories.
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
