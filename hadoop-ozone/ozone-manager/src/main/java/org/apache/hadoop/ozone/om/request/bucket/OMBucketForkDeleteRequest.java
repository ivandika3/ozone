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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_FORK_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketForkBaseViewInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketForkDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketForkRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketForkResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteBucketFork request.
 */
public class OMBucketForkDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketForkDeleteRequest.class);

  public OMBucketForkDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    DeleteBucketForkRequest request =
        getOmRequest().getDeleteBucketForkRequest();
    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    boolean acquiredVolumeLock = false;
    boolean acquiredBucketLock = false;

    try {
      if (!ozoneManager.getConfiguration().getBoolean(
          OZONE_OM_BUCKET_FORK_ENABLED,
          OZONE_OM_BUCKET_FORK_ENABLED_DEFAULT)) {
        throw new OMException("Bucket forks are disabled. Set "
            + OZONE_OM_BUCKET_FORK_ENABLED + " to true to enable them.",
            FEATURE_NOT_ENABLED);
      }

      String volumeName = request.getVolumeName();
      String bucketName = request.getBucketName();
      String volumeKey = metadataManager.getVolumeKey(volumeName);
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      String forkKey = BucketForkInfo.getTableKey(volumeName, bucketName);

      mergeOmLockDetails(metadataManager.getLock().acquireReadLock(
          VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();
      mergeOmLockDetails(metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().getReadCopy(volumeKey);
      if (volumeArgs == null) {
        throw new OMException("Target volume doesn't exist", VOLUME_NOT_FOUND);
      }
      BucketForkInfo bucketForkInfo =
          metadataManager.getBucketForkTable().get(forkKey);
      if (bucketForkInfo == null || !bucketForkInfo.isActive()) {
        throw new OMException("Bucket fork doesn't exist", BUCKET_NOT_FOUND);
      }
      if (!metadataManager.getBucketTable().isExist(bucketKey)) {
        throw new OMException("Fork target bucket doesn't exist",
            BUCKET_NOT_FOUND);
      }
      BucketForkBaseViewInfo baseViewInfo = null;
      boolean deleteBaseView = false;
      if (bucketForkInfo.getBaseViewId() != null) {
        String baseViewKey =
            BucketForkBaseViewInfo.getTableKey(bucketForkInfo.getBaseViewId());
        baseViewInfo =
            metadataManager.getBucketForkBaseViewTable().get(baseViewKey);
        if (baseViewInfo != null) {
          deleteBaseView = baseViewInfo.getReferenceCount() <= 1L;
          if (deleteBaseView) {
            metadataManager.getBucketForkBaseViewTable().addCacheEntry(
                new CacheKey<>(baseViewKey),
                CacheValue.get(transactionLogIndex));
          } else {
            baseViewInfo = baseViewInfo.toBuilder()
                .setReferenceCount(baseViewInfo.getReferenceCount() - 1L)
                .setUpdateId(transactionLogIndex)
                .build();
            metadataManager.getBucketForkBaseViewTable().addCacheEntry(
                new CacheKey<>(baseViewKey),
                CacheValue.get(transactionLogIndex, baseViewInfo));
          }
        }
      }

      volumeArgs = volumeArgs.toBuilder()
          .setUsedNamespace(Math.max(0L, volumeArgs.getUsedNamespace() - 1L))
          .build();

      metadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(volumeKey),
          CacheValue.get(transactionLogIndex, volumeArgs));
      metadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex));
      metadataManager.getBucketForkTable().addCacheEntry(
          new CacheKey<>(forkKey),
          CacheValue.get(transactionLogIndex));

      omResponse.setDeleteBucketForkResponse(
          DeleteBucketForkResponse.newBuilder().build());
      omClientResponse = new OMBucketForkDeleteResponse(omResponse.build(),
          bucketForkInfo, volumeArgs.copyObject(), baseViewInfo,
          deleteBaseView);
      LOG.info("Deleted bucket fork {}/{}", volumeName, bucketName);
    } catch (IOException | RuntimeException ex) {
      exception = ex;
      LOG.debug("Delete bucket fork failed for {}/{}",
          request.getVolumeName(), request.getBucketName(), ex);
      omClientResponse = new OMBucketForkDeleteResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseWriteLock(
            BUCKET_LOCK, request.getVolumeName(), request.getBucketName()));
      }
      if (acquiredVolumeLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseReadLock(
            VOLUME_LOCK, request.getVolumeName()));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
      markForAudit(ozoneManager.getAuditLogger(),
          buildAuditMessage(OMAction.DELETE_BUCKET_FORK,
              requestToAuditMap(request), exception,
              getOmRequest().getUserInfo()));
    }
    return omClientResponse;
  }

  private static Map<String, String> requestToAuditMap(
      DeleteBucketForkRequest request) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("volume", request.getVolumeName());
    auditMap.put("bucket", request.getBucketName());
    return auditMap;
  }
}
