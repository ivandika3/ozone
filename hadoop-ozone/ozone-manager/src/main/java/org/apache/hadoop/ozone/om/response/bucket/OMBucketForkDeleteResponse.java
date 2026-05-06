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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_FORK_TOMBSTONE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketForkBaseViewInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for DeleteBucketFork request.
 */
@CleanupTableInfo(cleanupTables = {
    BUCKET_FORK_TABLE, BUCKET_FORK_BASE_VIEW_TABLE,
    BUCKET_FORK_TOMBSTONE_TABLE, BUCKET_TABLE, VOLUME_TABLE})
public class OMBucketForkDeleteResponse extends OMClientResponse {
  private final BucketForkInfo bucketForkInfo;
  private final OmVolumeArgs targetVolumeArgs;
  private final BucketForkBaseViewInfo baseViewInfo;
  private final boolean deleteBaseView;

  public OMBucketForkDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketForkInfo bucketForkInfo,
      @Nonnull OmVolumeArgs targetVolumeArgs) {
    this(omResponse, bucketForkInfo, targetVolumeArgs, null, false);
  }

  public OMBucketForkDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketForkInfo bucketForkInfo,
      @Nonnull OmVolumeArgs targetVolumeArgs,
      BucketForkBaseViewInfo baseViewInfo, boolean deleteBaseView) {
    super(omResponse);
    this.bucketForkInfo = bucketForkInfo;
    this.targetVolumeArgs = targetVolumeArgs;
    this.baseViewInfo = baseViewInfo;
    this.deleteBaseView = deleteBaseView;
  }

  /**
   * For unsuccessful requests.
   */
  public OMBucketForkDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.bucketForkInfo = null;
    this.targetVolumeArgs = null;
    this.baseViewInfo = null;
    this.deleteBaseView = false;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(
        bucketForkInfo.getTargetVolumeName(),
        bucketForkInfo.getTargetBucketName());
    omMetadataManager.getBucketTable().deleteWithBatch(batchOperation,
        bucketKey);
    omMetadataManager.getBucketForkTable().deleteWithBatch(batchOperation,
        bucketForkInfo.getTableKey());
    omMetadataManager.getBucketForkTombstoneTable().deleteBatchWithPrefix(
        batchOperation, bucketForkInfo.getTableKey());
    if (baseViewInfo != null) {
      if (deleteBaseView) {
        omMetadataManager.getBucketForkBaseViewTable().deleteWithBatch(
            batchOperation, baseViewInfo.getTableKey());
      } else {
        omMetadataManager.getBucketForkBaseViewTable().putWithBatch(
            batchOperation, baseViewInfo.getTableKey(), baseViewInfo);
      }
    }
    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        omMetadataManager.getVolumeKey(targetVolumeArgs.getVolume()),
        targetVolumeArgs);
  }
}
