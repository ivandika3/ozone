/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests BucketForkManager.
 */
public class TestBucketForkManager {

  @Test
  @SuppressWarnings("unchecked")
  public void testActiveForkLookupIgnoresDeletedForks() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkInfo> forkTable = Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTable()).thenReturn(forkTable);

    BucketForkInfo activeFork = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    BucketForkInfo deletedFork = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_DELETED);
    Mockito.when(forkTable.get("/vol/fork")).thenReturn(activeFork);
    Mockito.when(forkTable.get("/vol/deleted")).thenReturn(deletedFork);

    BucketForkManager manager = new BucketForkManager(metadataManager);

    assertSame(activeFork, manager.getActiveForkInfo("vol", "fork"));
    assertTrue(manager.isForkBucket("vol", "fork"));
    assertFalse(manager.isForkBucket("vol", "deleted"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTombstoneLookupUsesTypedMetadata() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);

    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    BucketForkTombstoneInfo tombstone = BucketForkTombstoneInfo.newBuilder()
        .setForkId(forkInfo.getForkId())
        .setTargetVolumeName("vol")
        .setTargetBucketName("fork")
        .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
        .setLogicalPath("dir/key")
        .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
        .setCreationTime(100L)
        .setUpdateId(200L)
        .build();
    Mockito.when(tombstoneTable.get("/vol/fork/dir/key"))
        .thenReturn(tombstone);

    BucketForkManager manager = new BucketForkManager(metadataManager);

    assertEquals(tombstone,
        manager.getTombstoneInfo(forkInfo, "dir/key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLookupBaseKeyRewritesSnapshotInfoToForkNamespace()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    OmKeyInfo baseKeyInfo = createKeyInfo("vol", "source",
        ".snapshot/snap/dir/key");
    Mockito.when(baseReader.lookupKey(Mockito.argThat(args ->
        "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir/key".equals(args.getKeyName()))))
        .thenReturn(baseKeyInfo);

    OmKeyInfo result = new BucketForkManager(metadataManager)
        .lookupBaseKey(forkInfo, createKeyArgs("vol", "fork", "dir/key"),
            baseReader);

    assertEquals("vol", result.getVolumeName());
    assertEquals("fork", result.getBucketName());
    assertEquals("dir/key", result.getKeyName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLookupBaseKeyHonorsForkTombstone() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/dir/key"))
        .thenReturn(BucketForkTombstoneInfo.newBuilder()
            .setForkId(forkInfo.getForkId())
            .setTargetVolumeName("vol")
            .setTargetBucketName("fork")
            .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
            .setLogicalPath("dir/key")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
            .build());
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);

    OMException ex = assertThrows(OMException.class,
        () -> new BucketForkManager(metadataManager)
            .lookupBaseKey(forkInfo, createKeyArgs("vol", "fork", "dir/key"),
                baseReader));

    assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex.getResult());
    Mockito.verifyNoInteractions(baseReader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLookupBaseKeyHonorsForkDirectoryTombstone()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/dir"))
        .thenReturn(BucketForkTombstoneInfo.newBuilder()
            .setForkId(forkInfo.getForkId())
            .setTargetVolumeName("vol")
            .setTargetBucketName("fork")
            .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
            .setLogicalPath("dir")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.DIRECTORY)
            .build());
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);

    OMException ex = assertThrows(OMException.class,
        () -> new BucketForkManager(metadataManager)
            .lookupBaseKey(forkInfo,
                createKeyArgs("vol", "fork", "dir/child/file"),
                baseReader));

    assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex.getResult());
    Mockito.verifyNoInteractions(baseReader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetBaseKeyInfoUsesBaseGetKeyInfo() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    OmKeyInfo baseKeyInfo = createKeyInfo("vol", "source",
        ".snapshot/snap/dir/key");
    Mockito.when(baseReader.getKeyInfo(Mockito.argThat(args ->
        "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir/key".equals(args.getKeyName())),
        Mockito.eq(true)))
        .thenReturn(KeyInfoWithVolumeContext.newBuilder()
            .setKeyInfo(baseKeyInfo)
            .setUserPrincipal("alice")
            .build());

    KeyInfoWithVolumeContext result = new BucketForkManager(metadataManager)
        .getBaseKeyInfo(forkInfo, createKeyArgs("vol", "fork", "dir/key"),
            baseReader, true);

    Mockito.verify(baseReader, Mockito.never())
        .lookupKey(Mockito.any(OmKeyArgs.class));
    assertEquals("vol", result.getKeyInfo().getVolumeName());
    assertEquals("fork", result.getKeyInfo().getBucketName());
    assertEquals("dir/key", result.getKeyInfo().getKeyName());
    assertEquals("alice", result.getUserPrincipal().orElse(null));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLookupBaseFileStatusRewritesSnapshotInfoToForkNamespace()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    OmKeyInfo baseKeyInfo = createKeyInfo("vol", "source",
        ".snapshot/snap/dir/file");
    Mockito.when(baseReader.getFileStatus(Mockito.argThat(args ->
        "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir/file".equals(args.getKeyName()))))
        .thenReturn(new OzoneFileStatus(baseKeyInfo, 1024L, false));

    OzoneFileStatus result = new BucketForkManager(metadataManager)
        .lookupBaseFileStatus(forkInfo,
            createKeyArgs("vol", "fork", "dir/file"), baseReader);

    assertTrue(result.isFile());
    assertEquals(1024L, result.getBlockSize());
    assertEquals("vol", result.getKeyInfo().getVolumeName());
    assertEquals("fork", result.getKeyInfo().getBucketName());
    assertEquals("dir/file", result.getKeyInfo().getKeyName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLookupBaseFileRewritesSnapshotInfoToForkNamespace()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    OmKeyInfo baseKeyInfo = createKeyInfo("vol", "source",
        ".snapshot/snap/dir/file");
    Mockito.when(baseReader.lookupFile(Mockito.argThat(args ->
        "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir/file".equals(args.getKeyName()))))
        .thenReturn(baseKeyInfo);

    OmKeyInfo result = new BucketForkManager(metadataManager)
        .lookupBaseFile(forkInfo, createKeyArgs("vol", "fork", "dir/file"),
            baseReader);

    assertEquals("vol", result.getVolumeName());
    assertEquals("fork", result.getBucketName());
    assertEquals("dir/file", result.getKeyName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListStatusMergesForkLocalAndBaseSnapshotStatuses()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/dir/base-deleted"))
        .thenReturn(BucketForkTombstoneInfo.newBuilder()
            .setForkId(forkInfo.getForkId())
            .setTargetVolumeName("vol")
            .setTargetBucketName("fork")
            .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
            .setLogicalPath("dir/base-deleted")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
            .build());
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    Mockito.when(baseReader.listStatus(Mockito.argThat(args ->
        "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir".equals(args.getKeyName())),
        Mockito.eq(false), Mockito.eq(""), Mockito.eq(4L),
        Mockito.eq(false)))
        .thenReturn(Arrays.asList(
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-a"),
            createFileStatus("vol", "source", ".snapshot/snap/dir/local-b"),
            createFileStatus("vol", "source",
                ".snapshot/snap/dir/base-deleted")));
    List<OzoneFileStatus> forkLocalStatuses = Arrays.asList(
        createFileStatus("vol", "fork", "dir/local-b"),
        createFileStatus("vol", "fork", "dir/local-c"));

    List<OzoneFileStatus> result = new BucketForkManager(metadataManager)
        .listStatus(forkInfo, forkLocalStatuses,
            new BucketForkManager.ListStatusContext(
                createKeyArgs("vol", "fork", "dir"), false, "", 3L, false,
                false),
            baseReader);

    assertEquals(Arrays.asList("dir/base-a", "dir/local-b", "dir/local-c"),
        statusKeyNames(result));
    assertEquals("fork", result.get(0).getKeyInfo().getBucketName());
    assertEquals("fork", result.get(1).getKeyInfo().getBucketName());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListStatusFetchesPastHiddenBasePage() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/dir/base-a"))
        .thenReturn(createTombstone(forkInfo, "dir/base-a"));
    Mockito.when(tombstoneTable.get("/vol/fork/dir/base-b"))
        .thenReturn(createTombstone(forkInfo, "dir/base-b"));
    Mockito.when(tombstoneTable.get("/vol/fork/dir/base-c"))
        .thenReturn(createTombstone(forkInfo, "dir/base-c"));
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    Mockito.when(baseReader.listStatus(Mockito.argThat(args -> args != null
            && "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir".equals(args.getKeyName())),
        Mockito.eq(false), Mockito.eq(""), Mockito.eq(3L),
        Mockito.eq(false)))
        .thenReturn(Arrays.asList(
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-a"),
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-b"),
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-c")));
    Mockito.when(baseReader.listStatus(Mockito.argThat(args -> args != null
            && "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir".equals(args.getKeyName())),
        Mockito.eq(false), Mockito.eq("dir/base-c"), Mockito.eq(3L),
        Mockito.eq(false)))
        .thenReturn(Arrays.asList(
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-d"),
            createFileStatus("vol", "source", ".snapshot/snap/dir/base-e")));

    List<OzoneFileStatus> result = new BucketForkManager(metadataManager)
        .listStatus(forkInfo, Arrays.asList(),
            new BucketForkManager.ListStatusContext(
                createKeyArgs("vol", "fork", "dir"), false, "", 2L, false,
                false),
            baseReader);

    assertEquals(Arrays.asList("dir/base-d", "dir/base-e"),
        statusKeyNames(result));
    Mockito.verify(baseReader).listStatus(Mockito.argThat(args -> args != null
            && "vol".equals(args.getVolumeName())
            && "source".equals(args.getBucketName())
            && "dir".equals(args.getKeyName())),
        Mockito.eq(false), Mockito.eq("dir/base-c"), Mockito.eq(3L),
        Mockito.eq(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListKeysMergesForkLocalAndBaseSnapshotKeys()
      throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/c"))
        .thenReturn(BucketForkTombstoneInfo.newBuilder()
            .setForkId(forkInfo.getForkId())
            .setTargetVolumeName("vol")
            .setTargetBucketName("fork")
            .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
            .setLogicalPath("c")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
            .build());
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    Mockito.when(baseReader.listKeys("vol", "source", "", "", 4))
        .thenReturn(new ListKeysResult(Arrays.asList(
            createKeyInfo("vol", "source", ".snapshot/snap/a"),
            createKeyInfo("vol", "source", ".snapshot/snap/b"),
            createKeyInfo("vol", "source", ".snapshot/snap/c")), false));
    ListKeysResult forkLocalKeys = new ListKeysResult(Arrays.asList(
        createKeyInfo("vol", "fork", "b"),
        createKeyInfo("vol", "fork", "d")), false);

    ListKeysResult result = new BucketForkManager(metadataManager)
        .listKeys(forkInfo, forkLocalKeys, "", "", 3, baseReader);

    assertEquals(Arrays.asList("a", "b", "d"), keyNames(result.getKeys()));
    assertEquals("fork", result.getKeys().get(0).getBucketName());
    assertEquals("fork", result.getKeys().get(1).getBucketName());
    assertFalse(result.isTruncated());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListKeysReturnsStableTruncatedPage() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    Mockito.when(baseReader.listKeys("vol", "source", "a", "dir/", 3))
        .thenReturn(new ListKeysResult(Arrays.asList(
            createKeyInfo("vol", "source", ".snapshot/snap/dir/b"),
            createKeyInfo("vol", "source", ".snapshot/snap/dir/d")), true));
    ListKeysResult forkLocalKeys = new ListKeysResult(Arrays.asList(
        createKeyInfo("vol", "fork", "dir/c"),
        createKeyInfo("vol", "fork", "dir/e")), false);

    ListKeysResult result = new BucketForkManager(metadataManager)
        .listKeys(forkInfo, forkLocalKeys, "a", "dir/", 2, baseReader);

    assertEquals(Arrays.asList("dir/b", "dir/c"), keyNames(result.getKeys()));
    assertTrue(result.isTruncated());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListKeysFetchesPastHiddenBasePage() throws IOException {
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    Table<String, BucketForkTombstoneInfo> tombstoneTable =
        Mockito.mock(Table.class);
    Mockito.when(metadataManager.getBucketForkTombstoneTable())
        .thenReturn(tombstoneTable);
    BucketForkInfo forkInfo = createForkInfo(
        BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE);
    Mockito.when(tombstoneTable.get("/vol/fork/a"))
        .thenReturn(createTombstone(forkInfo, "a"));
    Mockito.when(tombstoneTable.get("/vol/fork/b"))
        .thenReturn(createTombstone(forkInfo, "b"));
    Mockito.when(tombstoneTable.get("/vol/fork/c"))
        .thenReturn(createTombstone(forkInfo, "c"));
    IOmMetadataReader baseReader = Mockito.mock(IOmMetadataReader.class);
    Mockito.when(baseReader.listKeys("vol", "source", "", "", 3))
        .thenReturn(new ListKeysResult(Arrays.asList(
            createKeyInfo("vol", "source", ".snapshot/snap/a"),
            createKeyInfo("vol", "source", ".snapshot/snap/b"),
            createKeyInfo("vol", "source", ".snapshot/snap/c")), true));
    Mockito.when(baseReader.listKeys("vol", "source", "c", "", 3))
        .thenReturn(new ListKeysResult(Arrays.asList(
            createKeyInfo("vol", "source", ".snapshot/snap/d"),
            createKeyInfo("vol", "source", ".snapshot/snap/e")), false));

    ListKeysResult result = new BucketForkManager(metadataManager)
        .listKeys(forkInfo, new ListKeysResult(Arrays.asList(), false),
            "", "", 2, baseReader);

    assertEquals(Arrays.asList("d", "e"), keyNames(result.getKeys()));
    assertFalse(result.isTruncated());
    Mockito.verify(baseReader).listKeys("vol", "source", "c", "", 3);
  }

  private static BucketForkInfo createForkInfo(
      BucketForkInfo.BucketForkStatus status) {
    return BucketForkInfo.newBuilder()
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName("vol")
        .setSourceBucketName("source")
        .setTargetVolumeName("vol")
        .setTargetBucketName(
            status == BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE
                ? "fork" : "deleted")
        .setBaseSnapshotId(UUID.randomUUID())
        .setBaseSnapshotName("snap")
        .setStatus(status)
        .build();
  }

  private static OmKeyArgs createKeyArgs(String volumeName,
      String bucketName, String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
  }

  private static OmKeyInfo createKeyInfo(String volumeName,
      String bucketName, String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
  }

  private static OzoneFileStatus createFileStatus(String volumeName,
      String bucketName, String keyName) {
    return new OzoneFileStatus(createKeyInfo(volumeName, bucketName, keyName),
        1024L, false);
  }

  private static BucketForkTombstoneInfo createTombstone(
      BucketForkInfo forkInfo, String logicalPath) {
    return BucketForkTombstoneInfo.newBuilder()
        .setForkId(forkInfo.getForkId())
        .setTargetVolumeName(forkInfo.getTargetVolumeName())
        .setTargetBucketName(forkInfo.getTargetBucketName())
        .setBaseSnapshotId(forkInfo.getBaseSnapshotId())
        .setLogicalPath(logicalPath)
        .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
        .build();
  }

  private static List<String> keyNames(List<OmKeyInfo> keyInfos) {
    return keyInfos.stream()
        .map(OmKeyInfo::getKeyName)
        .collect(Collectors.toList());
  }

  private static List<String> statusKeyNames(List<OzoneFileStatus> statuses) {
    return statuses.stream()
        .map(status -> status.getKeyInfo().getKeyName())
        .collect(Collectors.toList());
  }
}
