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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_FORK_ENABLED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketForkBaseViewInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotDeleteRequest;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketForkDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketForkRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketForkRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests bucket fork creation request handling.
 */
public class TestOMBucketForkCreateRequest extends TestBucketRequest {
  private static final String SOURCE_VOLUME = "source-vol";
  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String TARGET_VOLUME = "target-vol";
  private static final String TARGET_BUCKET = "target-bucket";
  private static final String SNAPSHOT_NAME = "base-snapshot";

  @Test
  public void testCreateBucketForkFailsWhenFeatureDisabled() {
    OMRequest omRequest = OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.CreateBucketFork)
        .setCreateBucketForkRequest(CreateBucketForkRequest.newBuilder()
            .setSourceVolumeName("source-vol")
            .setSourceBucketName("source-bucket")
            .setTargetVolumeName("target-vol")
            .setTargetBucketName("target-bucket")
            .setBaseSnapshotName("base-snapshot")
            .build())
        .build();

    OMBucketForkCreateRequest request =
        new OMBucketForkCreateRequest(omRequest);
    OMResponse response = request.validateAndUpdateCache(ozoneManager, 1L)
        .getOMResponse();

    assertEquals(FEATURE_NOT_ENABLED, response.getStatus());
  }

  @Test
  public void testCreateBucketForkFromNamedSnapshot() throws Exception {
    ozoneManager.getConfiguration().setBoolean(OZONE_OM_BUCKET_FORK_ENABLED,
        true);
    OMRequestTestUtils.addVolumeToDB(SOURCE_VOLUME, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(TARGET_VOLUME, omMetadataManager);
    OmBucketInfo sourceBucket = OMRequestTestUtils.addBucketToDB(
        SOURCE_VOLUME, SOURCE_BUCKET, omMetadataManager, BucketLayout.DEFAULT);
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(SOURCE_VOLUME,
        SOURCE_BUCKET, SNAPSHOT_NAME, UUID.randomUUID(), Time.now())
        .toBuilder()
        .setReferencedSize(1024L)
        .setReferencedReplicatedSize(3072L)
        .build();
    omMetadataManager.getSnapshotInfoTable().put(snapshotInfo.getTableKey(),
        snapshotInfo);

    OMBucketForkCreateRequest request =
        new OMBucketForkCreateRequest(createForkRequest());
    OMResponse response = request.validateAndUpdateCache(ozoneManager, 11L)
        .getOMResponse();

    assertEquals(OK, response.getStatus());
    BucketForkInfo forkInfo = omMetadataManager.getBucketForkTable().get(
        BucketForkInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET));
    assertNotNull(forkInfo);
    assertEquals(snapshotInfo.getSnapshotId(), forkInfo.getBaseSnapshotId());
    assertEquals(sourceBucket.getObjectID(), forkInfo.getSourceBucketObjectId());
    assertEquals(ozoneManager.getObjectIdFromTxId(11L),
        forkInfo.getTargetBucketObjectId());
    assertNotNull(forkInfo.getBaseViewId());
    assertNull(forkInfo.getSourceForkId());
    assertEquals(1, forkInfo.getLineageDepth());

    BucketForkBaseViewInfo baseViewInfo =
        omMetadataManager.getBucketForkBaseViewTable().get(
            BucketForkBaseViewInfo.getTableKey(forkInfo.getBaseViewId()));
    assertNotNull(baseViewInfo);
    assertEquals(SOURCE_VOLUME, baseViewInfo.getSourceVolumeName());
    assertEquals(SOURCE_BUCKET, baseViewInfo.getSourceBucketName());
    assertEquals(sourceBucket.getObjectID(),
        baseViewInfo.getSourceBucketObjectId());
    assertEquals(snapshotInfo.getSnapshotId(), baseViewInfo.getSnapshotId());
    assertEquals(snapshotInfo.getName(), baseViewInfo.getSnapshotName());
    assertNull(baseViewInfo.getParentBaseViewId());
    assertNull(baseViewInfo.getSourceForkId());
    assertEquals(1L, baseViewInfo.getReferenceCount());
    assertTrue(baseViewInfo.isActive());

    OmBucketInfo targetBucket = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(TARGET_VOLUME, TARGET_BUCKET));
    assertNotNull(targetBucket);
    assertEquals(sourceBucket.getBucketLayout(), targetBucket.getBucketLayout());
    assertEquals(1024L, targetBucket.getUsedBytes());
    assertEquals(0L, targetBucket.getUsedNamespace());
  }

  @Test
  public void testCreateBucketForkFromActiveBucketCreatesInternalSnapshot()
      throws Exception {
    ozoneManager.getConfiguration().setBoolean(OZONE_OM_BUCKET_FORK_ENABLED,
        true);
    OMRequestTestUtils.addVolumeToDB(SOURCE_VOLUME, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(TARGET_VOLUME, omMetadataManager);
    OmBucketInfo sourceBucket = OMRequestTestUtils.addBucketToDB(
        SOURCE_VOLUME, SOURCE_BUCKET, omMetadataManager, BucketLayout.DEFAULT)
        .toBuilder()
        .setUsedBytes(1024L)
        .setUsedNamespace(3L)
        .build();
    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(SOURCE_VOLUME, SOURCE_BUCKET),
        sourceBucket);
    omMetadataManager.getBucketTable().addCacheEntry(new CacheKey<>(
            omMetadataManager.getBucketKey(SOURCE_VOLUME, SOURCE_BUCKET)),
        CacheValue.get(11L, sourceBucket));

    OMBucketForkCreateRequest request =
        new OMBucketForkCreateRequest(createActiveBucketForkRequest());
    OMResponse response = request.validateAndUpdateCache(ozoneManager, 12L)
        .getOMResponse();

    assertEquals(OK, response.getStatus());
    BucketForkInfo forkInfo = omMetadataManager.getBucketForkTable().get(
        BucketForkInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET));
    assertNotNull(forkInfo);
    assertTrue(forkInfo.isCreatedFromActiveBucket());
    assertTrue(BucketForkInfo.isInternalBaseSnapshotName(
        forkInfo.getBaseSnapshotName()));

    SnapshotInfo snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(
        SnapshotInfo.getTableKey(SOURCE_VOLUME, SOURCE_BUCKET,
            forkInfo.getBaseSnapshotName()));
    assertNotNull(snapshotInfo);
    assertEquals(forkInfo.getBaseSnapshotId(), snapshotInfo.getSnapshotId());
    assertEquals(sourceBucket.getUsedBytes(), snapshotInfo.getReferencedSize());
    assertEquals(sourceBucket.getUsedNamespace(),
        forkInfo.getQuotaBaselineNamespace());

    BucketForkBaseViewInfo baseViewInfo =
        omMetadataManager.getBucketForkBaseViewTable().get(
            BucketForkBaseViewInfo.getTableKey(forkInfo.getBaseViewId()));
    assertNotNull(baseViewInfo);
    assertEquals(snapshotInfo.getSnapshotId(), baseViewInfo.getSnapshotId());
    assertEquals(snapshotInfo.getName(), baseViewInfo.getSnapshotName());
    assertEquals(1L, baseViewInfo.getReferenceCount());
    assertTrue(baseViewInfo.isActive());
  }

  @Test
  public void testSnapshotDeleteRejectedWhenReferencedByActiveFork()
      throws Exception {
    OMRequestTestUtils.addVolumeToDB(SOURCE_VOLUME, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(SOURCE_VOLUME, SOURCE_BUCKET,
        omMetadataManager, BucketLayout.DEFAULT);
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(SOURCE_VOLUME,
        SOURCE_BUCKET, SNAPSHOT_NAME, UUID.randomUUID(), Time.now());
    omMetadataManager.getSnapshotInfoTable().put(snapshotInfo.getTableKey(),
        snapshotInfo);
    omMetadataManager.getBucketForkTable().put(
        BucketForkInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET),
        BucketForkInfo.newBuilder()
            .setForkId(UUID.randomUUID())
            .setSourceVolumeName(SOURCE_VOLUME)
            .setSourceBucketName(SOURCE_BUCKET)
            .setTargetVolumeName(TARGET_VOLUME)
            .setTargetBucketName(TARGET_BUCKET)
            .setBaseSnapshotId(snapshotInfo.getSnapshotId())
            .setBaseSnapshotName(snapshotInfo.getName())
            .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
            .build());

    OMRequest deleteSnapshotRequest = OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.DeleteSnapshot)
        .setDeleteSnapshotRequest(DeleteSnapshotRequest.newBuilder()
            .setVolumeName(SOURCE_VOLUME)
            .setBucketName(SOURCE_BUCKET)
            .setSnapshotName(SNAPSHOT_NAME)
            .setDeletionTime(Time.now())
            .build())
        .build();

    OMResponse response = new OMSnapshotDeleteRequest(deleteSnapshotRequest)
        .validateAndUpdateCache(ozoneManager, 12L)
        .getOMResponse();

    assertEquals(CONTAINS_SNAPSHOT, response.getStatus());
  }

  @Test
  public void testDeleteBucketForkRemovesForkMetadataAndTargetBucket()
      throws Exception {
    ozoneManager.getConfiguration().setBoolean(OZONE_OM_BUCKET_FORK_ENABLED,
        true);
    OMRequestTestUtils.addVolumeToDB(TARGET_VOLUME, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(TARGET_VOLUME, TARGET_BUCKET,
        omMetadataManager, BucketLayout.DEFAULT);
    OmVolumeArgs volumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(TARGET_VOLUME));
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(TARGET_VOLUME),
        volumeArgs.toBuilder().setUsedNamespace(1L).build());

    BucketForkInfo bucketForkInfo = BucketForkInfo.newBuilder()
        .setBaseViewId(UUID.randomUUID())
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotId(UUID.randomUUID())
        .setBaseSnapshotName(SNAPSHOT_NAME)
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .setLineageDepth(1)
        .build();
    omMetadataManager.getBucketForkTable().put(bucketForkInfo.getTableKey(),
        bucketForkInfo);
    omMetadataManager.getBucketForkBaseViewTable().put(
        BucketForkBaseViewInfo.getTableKey(bucketForkInfo.getBaseViewId()),
        BucketForkBaseViewInfo.newBuilder()
            .setBaseViewId(bucketForkInfo.getBaseViewId())
            .setSourceVolumeName(SOURCE_VOLUME)
            .setSourceBucketName(SOURCE_BUCKET)
            .setSourceBucketObjectId(1L)
            .setSnapshotId(bucketForkInfo.getBaseSnapshotId())
            .setSnapshotName(bucketForkInfo.getBaseSnapshotName())
            .setCreationTime(Time.now())
            .setUpdateId(12L)
            .setReferenceCount(1L)
            .build());
    omMetadataManager.getBucketForkTombstoneTable().put(
        BucketForkTombstoneInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET,
            "deleted-key"),
        BucketForkTombstoneInfo.newBuilder()
            .setForkId(bucketForkInfo.getForkId())
            .setTargetVolumeName(TARGET_VOLUME)
            .setTargetBucketName(TARGET_BUCKET)
            .setBaseSnapshotId(bucketForkInfo.getBaseSnapshotId())
            .setLogicalPath("deleted-key")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
            .setCreationTime(Time.now())
            .setUpdateId(12L)
            .build());

    OMRequest deleteForkRequest = OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.DeleteBucketFork)
        .setDeleteBucketForkRequest(DeleteBucketForkRequest.newBuilder()
            .setVolumeName(TARGET_VOLUME)
            .setBucketName(TARGET_BUCKET)
            .setDeletionTime(Time.now())
            .build())
        .build();

    OMBucketForkDeleteResponse clientResponse =
        (OMBucketForkDeleteResponse) new OMBucketForkDeleteRequest(deleteForkRequest)
            .validateAndUpdateCache(ozoneManager, 13L);
    OMResponse response = clientResponse.getOMResponse();

    assertEquals(OK, response.getStatus());
    try (BatchOperation batchOperation =
             omMetadataManager.getStore().initBatchOperation()) {
      clientResponse.addToDBBatch(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    assertEquals(null, omMetadataManager.getBucketForkTable().get(
        bucketForkInfo.getTableKey()));
    assertEquals(null, omMetadataManager.getBucketForkBaseViewTable().get(
        BucketForkBaseViewInfo.getTableKey(bucketForkInfo.getBaseViewId())));
    assertEquals(null, omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(TARGET_VOLUME, TARGET_BUCKET)));
    assertEquals(null, omMetadataManager.getBucketForkTombstoneTable().get(
        bucketForkInfo.getTableKey() + "/deleted-key"));
    assertEquals(0L, omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(TARGET_VOLUME)).getUsedNamespace());
  }

  @Test
  public void testDeleteBucketForkReleasesSnapshotReference()
      throws Exception {
    ozoneManager.getConfiguration().setBoolean(OZONE_OM_BUCKET_FORK_ENABLED,
        true);
    OMRequestTestUtils.addVolumeToDB(SOURCE_VOLUME, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(TARGET_VOLUME, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(SOURCE_VOLUME, SOURCE_BUCKET,
        omMetadataManager, BucketLayout.DEFAULT);
    OMRequestTestUtils.addBucketToDB(TARGET_VOLUME, TARGET_BUCKET,
        omMetadataManager, BucketLayout.DEFAULT);
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(SOURCE_VOLUME,
        SOURCE_BUCKET, SNAPSHOT_NAME, UUID.randomUUID(), Time.now());
    omMetadataManager.getSnapshotInfoTable().put(snapshotInfo.getTableKey(),
        snapshotInfo);
    BucketForkInfo bucketForkInfo = BucketForkInfo.newBuilder()
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotId(snapshotInfo.getSnapshotId())
        .setBaseSnapshotName(snapshotInfo.getName())
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .build();
    omMetadataManager.getBucketForkTable().put(bucketForkInfo.getTableKey(),
        bucketForkInfo);
    omMetadataManager.getBucketForkTombstoneTable().put(
        BucketForkTombstoneInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET,
            "deleted-key"),
        BucketForkTombstoneInfo.newBuilder()
            .setForkId(bucketForkInfo.getForkId())
            .setTargetVolumeName(TARGET_VOLUME)
            .setTargetBucketName(TARGET_BUCKET)
            .setBaseSnapshotId(bucketForkInfo.getBaseSnapshotId())
            .setLogicalPath("deleted-key")
            .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
            .setCreationTime(Time.now())
            .setUpdateId(12L)
            .build());

    OMResponse blockedDeleteSnapshot =
        new OMSnapshotDeleteRequest(createDeleteSnapshotRequest())
            .validateAndUpdateCache(ozoneManager, 13L)
            .getOMResponse();

    assertEquals(CONTAINS_SNAPSHOT, blockedDeleteSnapshot.getStatus());

    OMBucketForkDeleteResponse deleteForkResponse =
        (OMBucketForkDeleteResponse) new OMBucketForkDeleteRequest(
            createDeleteForkRequest())
            .validateAndUpdateCache(ozoneManager, 14L);
    assertEquals(OK, deleteForkResponse.getOMResponse().getStatus());
    try (BatchOperation batchOperation =
             omMetadataManager.getStore().initBatchOperation()) {
      deleteForkResponse.addToDBBatch(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    assertNull(omMetadataManager.getBucketForkTable().get(
        bucketForkInfo.getTableKey()));
    assertNull(omMetadataManager.getBucketForkTombstoneTable().get(
        bucketForkInfo.getTableKey() + "/deleted-key"));

    OMResponse allowedDeleteSnapshot =
        new OMSnapshotDeleteRequest(createDeleteSnapshotRequest())
            .validateAndUpdateCache(ozoneManager, 15L)
            .getOMResponse();

    assertEquals(OK, allowedDeleteSnapshot.getStatus());
  }

  private OMRequest createForkRequest() {
    return OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.CreateBucketFork)
        .setCreateBucketForkRequest(CreateBucketForkRequest.newBuilder()
            .setSourceVolumeName(SOURCE_VOLUME)
            .setSourceBucketName(SOURCE_BUCKET)
            .setTargetVolumeName(TARGET_VOLUME)
            .setTargetBucketName(TARGET_BUCKET)
            .setBaseSnapshotName(SNAPSHOT_NAME)
            .build())
        .build();
  }

  private OMRequest createActiveBucketForkRequest() {
    return OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.CreateBucketFork)
        .setCreateBucketForkRequest(CreateBucketForkRequest.newBuilder()
            .setSourceVolumeName(SOURCE_VOLUME)
            .setSourceBucketName(SOURCE_BUCKET)
            .setTargetVolumeName(TARGET_VOLUME)
            .setTargetBucketName(TARGET_BUCKET)
            .build())
        .build();
  }

  private OMRequest createDeleteForkRequest() {
    return OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.DeleteBucketFork)
        .setDeleteBucketForkRequest(DeleteBucketForkRequest.newBuilder()
            .setVolumeName(TARGET_VOLUME)
            .setBucketName(TARGET_BUCKET)
            .setDeletionTime(Time.now())
            .build())
        .build();
  }

  private OMRequest createDeleteSnapshotRequest() {
    return OMRequest.newBuilder()
        .setClientId("client")
        .setCmdType(Type.DeleteSnapshot)
        .setDeleteSnapshotRequest(DeleteSnapshotRequest.newBuilder()
            .setVolumeName(SOURCE_VOLUME)
            .setBucketName(SOURCE_BUCKET)
            .setSnapshotName(SNAPSHOT_NAME)
            .setDeletionTime(Time.now())
            .build())
        .build();
  }
}
