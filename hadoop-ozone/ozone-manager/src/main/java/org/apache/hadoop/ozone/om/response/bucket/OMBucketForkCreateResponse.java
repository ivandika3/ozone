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

package org.apache.hadoop.ozone.om.response.bucket;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_FORK_BASE_VIEW_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_FORK_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.BucketForkBaseViewInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for CreateBucketFork request.
 */
@CleanupTableInfo(cleanupTables = {
    BUCKET_FORK_TABLE, BUCKET_FORK_BASE_VIEW_TABLE, BUCKET_TABLE,
    VOLUME_TABLE, DELETED_TABLE, SNAPSHOT_RENAMED_TABLE, SNAPSHOT_INFO_TABLE})
public class OMBucketForkCreateResponse extends OMClientResponse {
  private final BucketForkInfo bucketForkInfo;
  private final OmBucketInfo forkBucketInfo;
  private final OmVolumeArgs targetVolumeArgs;
  private final SnapshotInfo internalBaseSnapshotInfo;
  private final BucketForkBaseViewInfo baseViewInfo;

  public OMBucketForkCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketForkInfo bucketForkInfo,
      @Nonnull OmBucketInfo forkBucketInfo,
      @Nonnull OmVolumeArgs targetVolumeArgs) {
    this(omResponse, bucketForkInfo, forkBucketInfo, targetVolumeArgs, null,
        null);
  }

  public OMBucketForkCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketForkInfo bucketForkInfo,
      @Nonnull OmBucketInfo forkBucketInfo,
      @Nonnull OmVolumeArgs targetVolumeArgs,
      @Nullable SnapshotInfo internalBaseSnapshotInfo,
      @Nullable BucketForkBaseViewInfo baseViewInfo) {
    super(omResponse);
    this.bucketForkInfo = bucketForkInfo;
    this.forkBucketInfo = forkBucketInfo;
    this.targetVolumeArgs = targetVolumeArgs;
    this.internalBaseSnapshotInfo = internalBaseSnapshotInfo;
    this.baseViewInfo = baseViewInfo;
  }

  /**
   * For unsuccessful requests.
   */
  public OMBucketForkCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.bucketForkInfo = null;
    this.forkBucketInfo = null;
    this.targetVolumeArgs = null;
    this.internalBaseSnapshotInfo = null;
    this.baseViewInfo = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (internalBaseSnapshotInfo != null) {
      omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
          internalBaseSnapshotInfo.getTableKey(), internalBaseSnapshotInfo);
      OmSnapshotManager.createOmSnapshotCheckpoint(omMetadataManager,
          internalBaseSnapshotInfo, batchOperation);
    }

    String bucketKey = omMetadataManager.getBucketKey(
        forkBucketInfo.getVolumeName(), forkBucketInfo.getBucketName());
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        bucketKey, forkBucketInfo);
    omMetadataManager.getBucketForkTable().putWithBatch(batchOperation,
        bucketForkInfo.getTableKey(), bucketForkInfo);
    if (baseViewInfo != null) {
      omMetadataManager.getBucketForkBaseViewTable().putWithBatch(
          batchOperation, baseViewInfo.getTableKey(), baseViewInfo);
    }
    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        omMetadataManager.getVolumeKey(targetVolumeArgs.getVolume()),
        targetVolumeArgs);
  }
}
