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
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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

  private static List<String> keyNames(List<OmKeyInfo> keyInfos) {
    return keyInfos.stream()
        .map(OmKeyInfo::getKeyName)
        .collect(Collectors.toList());
  }
}
