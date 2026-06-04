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

package org.apache.hadoop.ozone.om.request.key;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Tests OmKeyDelete request.
 */
public class TestOMKeyDeleteRequest extends TestOMKeyRequest {

  @ParameterizedTest
  @ValueSource(strings = {"keyName", "a/b/keyName", "a/.snapshot/keyName", "a.snapshot/b/keyName"})
  public void testPreExecute(String testKeyName) throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable(testKeyName);
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);

    doPreExecute(createDeleteKeyRequest(testKeyName));
  }

  @ParameterizedTest
  @CsvSource(value = {".snapshot,Cannot delete key with reserved name: .snapshot",
      ".snapshot/snapName,Cannot delete key under path reserved for snapshot: .snapshot/",
      ".snapshot/snapName/keyName,Cannot delete key under path reserved for snapshot: .snapshot/"})
  public void testPreExecuteFailure(String testKeyName,
                                    String expectedExceptionMessage) {
    OMKeyDeleteRequest deleteKeyRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest(testKeyName));
    OMException omException = assertThrows(OMException.class,
        () -> deleteKeyRequest.preExecute(ozoneManager));
    assertEquals(expectedExceptionMessage, omException.getMessage());
    assertEquals(OMException.ResultCodes.INVALID_KEY_NAME, omException.getResult());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    OMRequest modifiedOmRequest =
            doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // Now after calling validateAndUpdateCache, it should be deleted.

    omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {
    // Add only volume and bucket entry to DB.
    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMKeyDeleteRequest omKeyDeleteRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testDeleteBaseVisibleForkKeyCreatesTombstone() throws Exception {
    String sourceBucketName = bucketName + "-source";
    String snapshotName = "snap";
    UUID snapshotId = UUID.randomUUID();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setUsedBytes(100L)
            .setUsedNamespace(5L));
    BucketForkInfo forkInfo = BucketForkInfo.newBuilder()
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName(volumeName)
        .setSourceBucketName(sourceBucketName)
        .setTargetVolumeName(volumeName)
        .setTargetBucketName(bucketName)
        .setBaseSnapshotId(snapshotId)
        .setBaseSnapshotName(snapshotName)
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .build();
    omMetadataManager.getBucketForkTable().put(
        BucketForkInfo.getTableKey(volumeName, bucketName), forkInfo);

    OmSnapshot baseSnapshot = Mockito.mock(OmSnapshot.class);
    OmKeyInfo baseKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(sourceBucketName)
        .setKeyName(".snapshot/" + snapshotName + "/" + keyName)
        .setObjectID(123L)
        .setReplicationConfig(replicationConfig)
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0L,
            Collections.singletonList(new OmKeyLocationInfo.Builder()
                .setBlockID(new BlockID(1L, 1L))
                .setLength(100L)
                .build()), false))
        .build();
    Mockito.when(baseSnapshot.lookupKey(Mockito.argThat(args ->
        args != null
            && volumeName.equals(args.getVolumeName())
            && sourceBucketName.equals(args.getBucketName())
            && keyName.equals(args.getKeyName()))))
        .thenReturn(baseKeyInfo);
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier =
        Mockito.mock(UncheckedAutoCloseableSupplier.class);
    Mockito.when(snapshotSupplier.get()).thenReturn(baseSnapshot);
    OmSnapshotManager snapshotManager = Mockito.mock(OmSnapshotManager.class);
    Mockito.when(snapshotManager.getSnapshot(snapshotId))
        .thenReturn(snapshotSupplier);
    Mockito.when(ozoneManager.getOmSnapshotManager()).thenReturn(
        snapshotManager);
    Mockito.when(baseSnapshot.getFileStatus(Mockito.argThat(args ->
        args != null
            && volumeName.equals(args.getVolumeName())
            && sourceBucketName.equals(args.getBucketName())
            && keyName.equals(args.getKeyName()))))
        .thenReturn(new OzoneFileStatus(baseKeyInfo, 0L, false));

    OMKeyDeleteRequest omKeyDeleteRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    BucketForkTombstoneInfo tombstone = omMetadataManager
        .getBucketForkTombstoneTable()
        .get(BucketForkTombstoneInfo.getTableKey(volumeName, bucketName,
            keyName));
    assertNotNull(tombstone);
    assertEquals(keyName, tombstone.getLogicalPath());
    assertEquals(snapshotId, tombstone.getBaseSnapshotId());
    assertEquals(100L, tombstone.getUpdateId());
    assertEquals(4L, omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedNamespace());
    assertEquals(0L, omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedBytes());
    assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));
    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      Mockito.verify(baseSnapshot).getFileStatus(Mockito.any(OmKeyArgs.class));
    } else {
      Mockito.verify(baseSnapshot).lookupKey(Mockito.any(OmKeyArgs.class));
    }
  }

  @Test
  public void testDeleteForkLocalKeyUpdatesQuotaWithoutTombstone()
      throws Exception {
    setupForkBucketWithEmptyBaseSnapshot(bucketName + "-source", "snap",
        UUID.randomUUID());

    OmBucketInfo forkBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(getBucketLayout())
        .setUsedNamespace(5L)
        .build();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        forkBucketInfo.toBuilder());
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, forkBucketInfo));
    String ozoneKey = addKeyToTable();

    OMKeyDeleteRequest omKeyDeleteRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    assertNull(omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey));
    assertNull(omMetadataManager.getBucketForkTombstoneTable().get(
        BucketForkTombstoneInfo.getTableKey(volumeName, bucketName, keyName)));
    assertEquals(4L, omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedNamespace());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest = getOmKeyDeleteRequest(
        createDeleteKeyRequest());

    OMClientResponse omClientResponse = omKeyDeleteRequest
        .validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMClientResponse omClientResponse = omKeyDeleteRequest
        .validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  protected OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  protected OMRequest createDeleteKeyRequest() {
    return createDeleteKeyRequest(keyName);
  }

  protected OMRequest createDeleteKeyRequest(String testKeyName) {
    KeyArgs keyArgs = KeyArgs.newBuilder().setBucketName(bucketName)
        .setVolumeName(volumeName).setKeyName(testKeyName).build();

    DeleteKeyRequest deleteKeyRequest =
        DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setDeleteKeyRequest(deleteKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  protected String addKeyToTable() throws Exception {
    return addKeyToTable(keyName);
  }

  protected String addKeyToTable(String key) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, volumeName,
        bucketName, key, clientID, replicationConfig,
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName, key);
  }

  protected OMKeyDeleteRequest getOmKeyDeleteRequest(
      OMRequest modifiedOmRequest) {
    return new OMKeyDeleteRequest(modifiedOmRequest, BucketLayout.DEFAULT);
  }
}
