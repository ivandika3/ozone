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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Class tests OMKeysDeleteRequest.
 */
public class TestOMKeysDeleteRequest extends TestOMKeyRequest {

  private List<String> deleteKeyList;
  private OMRequest omRequest;

  @Test
  public void testKeysDeleteRequest() throws Exception {

    createPreRequisites();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest);
  }

  protected void checkDeleteKeysResponse(
      OMKeysDeleteRequest omKeysDeleteRequest) throws java.io.IOException {
    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertTrue(omClientResponse.getOMResponse().getSuccess());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    assertTrue(omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getStatus());
    DeleteKeyArgs unDeletedKeys =
        omClientResponse.getOMResponse().getDeleteKeysResponse()
            .getUnDeletedKeys();
    assertEquals(0, unDeletedKeys.getKeysCount());
    List<DeleteKeyError> keyErrors =  omClientResponse.getOMResponse().getDeleteKeysResponse()
        .getErrorsList();
    assertEquals(0, keyErrors.size());

    // Check all keys are deleted.
    for (String deleteKey : deleteKeyList) {
      assertNull(omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              deleteKey)));
    }
  }

  @Test
  public void testKeysDeleteRequestFail() throws Exception {

    createPreRequisites();

    // Add a key which not exist, which causes batch delete to fail.

    omRequest = omRequest.toBuilder()
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                    .addAllKeys(deleteKeyList).addKeys("dummy"))).build();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest);
  }

  @Test
  public void testDeleteBaseVisibleForkKeysCreatesTombstones()
      throws Exception {
    String sourceBucketName = bucketName + "-source";
    String snapshotName = "snap";
    UUID snapshotId = UUID.randomUUID();
    List<String> baseKeys = Arrays.asList("base-a", "base-b");

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
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
    for (String baseKey : baseKeys) {
      Mockito.when(baseSnapshot.lookupKey(Mockito.argThat(args ->
          args != null
              && volumeName.equals(args.getVolumeName())
              && sourceBucketName.equals(args.getBucketName())
              && baseKey.equals(args.getKeyName()))))
          .thenReturn(new OmKeyInfo.Builder()
              .setVolumeName(volumeName)
              .setBucketName(sourceBucketName)
              .setKeyName(".snapshot/" + snapshotName + "/" + baseKey)
              .setObjectID(123L)
              .build());
    }
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier =
        Mockito.mock(UncheckedAutoCloseableSupplier.class);
    Mockito.when(snapshotSupplier.get()).thenReturn(baseSnapshot);
    OmSnapshotManager snapshotManager = Mockito.mock(OmSnapshotManager.class);
    Mockito.when(snapshotManager.getSnapshot(snapshotId))
        .thenReturn(snapshotSupplier);
    Mockito.when(ozoneManager.getOmSnapshotManager()).thenReturn(
        snapshotManager);

    DeleteKeyArgs deleteKeyArgs = DeleteKeyArgs.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .addAllKeys(baseKeys)
        .build();
    OMRequest deleteKeysRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(DeleteKeys)
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setDeleteKeys(deleteKeyArgs).build())
        .build();

    OMClientResponse omClientResponse =
        new OMKeysDeleteRequest(deleteKeysRequest, getBucketLayout())
            .validateAndUpdateCache(ozoneManager, 100L);

    assertTrue(omClientResponse.getOMResponse().getSuccess());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    for (String baseKey : baseKeys) {
      BucketForkTombstoneInfo tombstone = omMetadataManager
          .getBucketForkTombstoneTable()
          .get(BucketForkTombstoneInfo.getTableKey(volumeName, bucketName,
              baseKey));
      assertNotNull(tombstone);
      assertEquals(baseKey, tombstone.getLogicalPath());
      assertEquals(snapshotId, tombstone.getBaseSnapshotId());
    }
    assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));
    Mockito.verify(baseSnapshot, Mockito.times(baseKeys.size()))
        .lookupKey(Mockito.any(OmKeyArgs.class));
  }

  protected void checkDeleteKeysResponseForFailure(
      OMKeysDeleteRequest omKeysDeleteRequest) throws java.io.IOException {
    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(PARTIAL_DELETE,
        omClientResponse.getOMResponse().getStatus());

    assertFalse(omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getStatus());

    // Check keys are deleted and in response check unDeletedKey.
    for (String deleteKey : deleteKeyList) {
      assertNull(omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              deleteKey)));
    }

    DeleteKeyArgs unDeletedKeys = omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getUnDeletedKeys();
    assertEquals(1,
        unDeletedKeys.getKeysCount());
    List<DeleteKeyError> keyErrors =  omClientResponse.getOMResponse().getDeleteKeysResponse()
        .getErrorsList();
    assertEquals(1, keyErrors.size());
    assertEquals("dummy", unDeletedKeys.getKeys(0));
  }

  protected void createPreRequisites() throws Exception {

    deleteKeyList = new ArrayList<>();
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    int count = 10;

    DeleteKeyArgs.Builder deleteKeyArgs = DeleteKeyArgs.newBuilder()
        .setBucketName(bucketName).setVolumeName(volumeName);

    // Create 10 keys
    String parentDir = "/user";
    String key;


    for (int i = 0; i < count; i++) {
      key = parentDir.concat("/key" + i);
      OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName,
          parentDir.concat("/key" + i), RatisReplicationConfig.getInstance(THREE), omMetadataManager);
      deleteKeyArgs.addKeys(key);
      deleteKeyList.add(key);
    }

    omRequest =
        OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(DeleteKeys)
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setDeleteKeys(deleteKeyArgs).build()).build();
  }

  public List<String> getDeleteKeyList() {
    return deleteKeyList;
  }

  public void setDeleteKeyList(List<String> deleteKeyList) {
    this.deleteKeyList = deleteKeyList;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  public void setOmRequest(OMRequest omRequest) {
    this.omRequest = omRequest;
  }
}
