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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
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
}
