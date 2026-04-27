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

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_FORK_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_FORK_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_SNAPSHOT_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.QUOTA_EXCEEDED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketForkCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketForkRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketForkResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CreateBucketFork request.
 */
public class OMBucketForkCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketForkCreateRequest.class);

  public OMBucketForkCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    super.preExecute(ozoneManager);

    CreateBucketForkRequest request =
        getOmRequest().getCreateBucketForkRequest();
    CreateBucketForkRequest.Builder builder = request.toBuilder();
    if (!request.hasCreationTime()) {
      builder.setCreationTime(Time.now());
    }
    if (!request.hasForkId()) {
      builder.setForkId(toProtobuf(UUID.randomUUID()));
    }
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setCreateBucketForkRequest(builder.build())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    CreateBucketForkRequest request =
        getOmRequest().getCreateBucketForkRequest();
    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    boolean acquiredSourceVolumeLock = false;
    boolean acquiredTargetVolumeLock = false;
    boolean acquiredSourceBucketLock = false;
    boolean acquiredTargetBucketLock = false;

    try {
      if (!ozoneManager.getConfiguration().getBoolean(
          OZONE_OM_BUCKET_FORK_ENABLED,
          OZONE_OM_BUCKET_FORK_ENABLED_DEFAULT)) {
        throw new OMException("Bucket forks are disabled. Set "
            + OZONE_OM_BUCKET_FORK_ENABLED + " to true to enable them.",
            OMException.ResultCodes.FEATURE_NOT_ENABLED);
      }

      String sourceVolumeName = request.getSourceVolumeName();
      String sourceBucketName = request.getSourceBucketName();
      String targetVolumeName = request.getTargetVolumeName();
      String targetBucketName = request.getTargetBucketName();
      String sourceVolumeKey = metadataManager.getVolumeKey(sourceVolumeName);
      String targetVolumeKey = metadataManager.getVolumeKey(targetVolumeName);
      String sourceBucketKey = metadataManager.getBucketKey(sourceVolumeName,
          sourceBucketName);
      String targetBucketKey = metadataManager.getBucketKey(targetVolumeName,
          targetBucketName);

      mergeOmLockDetails(metadataManager.getLock().acquireReadLock(
          VOLUME_LOCK, sourceVolumeName));
      acquiredSourceVolumeLock = getOmLockDetails().isLockAcquired();
      if (!sourceVolumeName.equals(targetVolumeName)) {
        mergeOmLockDetails(metadataManager.getLock().acquireReadLock(
            VOLUME_LOCK, targetVolumeName));
        acquiredTargetVolumeLock = getOmLockDetails().isLockAcquired();
      }
      mergeOmLockDetails(metadataManager.getLock().acquireReadLock(
          BUCKET_LOCK, sourceVolumeName, sourceBucketName));
      acquiredSourceBucketLock = getOmLockDetails().isLockAcquired();
      mergeOmLockDetails(metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, targetVolumeName, targetBucketName));
      acquiredTargetBucketLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs targetVolumeArgs =
          metadataManager.getVolumeTable().getReadCopy(targetVolumeKey);
      if (targetVolumeArgs == null) {
        throw new OMException("Target volume doesn't exist", VOLUME_NOT_FOUND);
      }
      if (metadataManager.getVolumeTable().get(sourceVolumeKey) == null) {
        throw new OMException("Source volume doesn't exist", VOLUME_NOT_FOUND);
      }
      OmBucketInfo sourceBucketInfo =
          metadataManager.getBucketTable().getReadCopy(sourceBucketKey);
      if (sourceBucketInfo == null) {
        throw new OMException("Source bucket doesn't exist", BUCKET_NOT_FOUND);
      }
      if (metadataManager.getBucketTable().isExist(targetBucketKey)) {
        throw new OMException("Target bucket already exists",
            BUCKET_ALREADY_EXISTS);
      }

      SnapshotInfo baseSnapshotInfo = getActiveBaseSnapshot(metadataManager,
          request);
      checkNamespaceQuota(targetVolumeArgs);

      long targetBucketObjectId =
          ozoneManager.getObjectIdFromTxId(transactionLogIndex);
      long quotaBaselineBytes = baseSnapshotInfo.getReferencedSize();
      long quotaBaselineNamespace = sourceBucketInfo.getUsedNamespace();
      long creationTime = request.hasCreationTime()
          ? request.getCreationTime() : transactionLogIndex;

      OmBucketInfo targetBucketInfo = sourceBucketInfo.toBuilder()
          .setVolumeName(targetVolumeName)
          .setBucketName(targetBucketName)
          .setSourceVolume(null)
          .setSourceBucket(null)
          .setObjectID(targetBucketObjectId)
          .setUpdateID(transactionLogIndex)
          .setCreationTime(creationTime)
          .setModificationTime(creationTime)
          .setUsedBytes(quotaBaselineBytes)
          .setUsedNamespace(quotaBaselineNamespace)
          .build();

      BucketForkInfo bucketForkInfo = BucketForkInfo.newBuilder()
          .setForkId(request.hasForkId()
              ? fromProtobuf(request.getForkId())
              : new UUID(0L, transactionLogIndex))
          .setSourceVolumeName(sourceVolumeName)
          .setSourceBucketName(sourceBucketName)
          .setTargetVolumeName(targetVolumeName)
          .setTargetBucketName(targetBucketName)
          .setBaseSnapshotId(baseSnapshotInfo.getSnapshotId())
          .setBaseSnapshotName(baseSnapshotInfo.getName())
          .setSourceBucketObjectId(sourceBucketInfo.getObjectID())
          .setTargetBucketObjectId(targetBucketObjectId)
          .setCreationTime(creationTime)
          .setDeletionTime(-1L)
          .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
          .setQuotaBaselineBytes(quotaBaselineBytes)
          .setQuotaBaselineNamespace(quotaBaselineNamespace)
          .setCreatedFromActiveBucket(false)
          .build();

      targetVolumeArgs = targetVolumeArgs.toBuilder()
          .incrUsedNamespace(1L)
          .build();

      metadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(targetVolumeKey),
          CacheValue.get(transactionLogIndex, targetVolumeArgs));
      metadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(targetBucketKey),
          CacheValue.get(transactionLogIndex, targetBucketInfo));
      metadataManager.getBucketForkTable().addCacheEntry(
          new CacheKey<>(bucketForkInfo.getTableKey()),
          CacheValue.get(transactionLogIndex, bucketForkInfo));

      omResponse.setCreateBucketForkResponse(
          CreateBucketForkResponse.newBuilder()
              .setBucketForkInfo(bucketForkInfo.getProtobuf())
              .build());
      omClientResponse = new OMBucketForkCreateResponse(omResponse.build(),
          bucketForkInfo, targetBucketInfo, targetVolumeArgs.copyObject());
      LOG.info("Created bucket fork {}/{} from snapshot {} of {}/{}",
          targetVolumeName, targetBucketName, request.getBaseSnapshotName(),
          sourceVolumeName, sourceBucketName);
    } catch (IOException | RuntimeException ex) {
      exception = ex;
      LOG.debug("Create bucket fork failed for {}/{} from {}/{}",
          request.getTargetVolumeName(), request.getTargetBucketName(),
          request.getSourceVolumeName(), request.getSourceBucketName(), ex);
      omClientResponse = new OMBucketForkCreateResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (acquiredTargetBucketLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseWriteLock(
            BUCKET_LOCK, request.getTargetVolumeName(),
            request.getTargetBucketName()));
      }
      if (acquiredSourceBucketLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseReadLock(
            BUCKET_LOCK, request.getSourceVolumeName(),
            request.getSourceBucketName()));
      }
      if (acquiredTargetVolumeLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseReadLock(
            VOLUME_LOCK, request.getTargetVolumeName()));
      }
      if (acquiredSourceVolumeLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseReadLock(
            VOLUME_LOCK, request.getSourceVolumeName()));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
      markForAudit(ozoneManager.getAuditLogger(),
          buildAuditMessage(OMAction.CREATE_BUCKET_FORK,
              requestToAuditMap(request), exception,
              getOmRequest().getUserInfo()));
    }
    return omClientResponse;
  }

  private static SnapshotInfo getActiveBaseSnapshot(
      OMMetadataManager metadataManager, CreateBucketForkRequest request)
      throws IOException {
    String snapshotKey = SnapshotInfo.getTableKey(request.getSourceVolumeName(),
        request.getSourceBucketName(), request.getBaseSnapshotName());
    SnapshotInfo snapshotInfo =
        metadataManager.getSnapshotInfoTable().get(snapshotKey);
    if (snapshotInfo == null) {
      throw new OMException("Base snapshot doesn't exist",
          INVALID_SNAPSHOT_ERROR);
    }
    if (snapshotInfo.getSnapshotStatus()
        != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
      throw new OMException("Base snapshot is not active",
          INVALID_SNAPSHOT_ERROR);
    }
    return snapshotInfo;
  }

  private static void checkNamespaceQuota(OmVolumeArgs targetVolumeArgs)
      throws IOException {
    if (targetVolumeArgs.getQuotaInNamespace() > 0
        && targetVolumeArgs.getUsedNamespace() + 1L
        > targetVolumeArgs.getQuotaInNamespace()) {
      throw new OMException("The namespace quota of Volume:"
          + targetVolumeArgs.getVolume() + " exceeded.",
          QUOTA_EXCEEDED);
    }
  }

  private static Map<String, String> requestToAuditMap(
      CreateBucketForkRequest request) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("sourceVolume", request.getSourceVolumeName());
    auditMap.put("sourceBucket", request.getSourceBucketName());
    auditMap.put("targetVolume", request.getTargetVolumeName());
    auditMap.put("targetBucket", request.getTargetBucketName());
    auditMap.put("baseSnapshot", request.getBaseSnapshotName());
    return auditMap;
  }
}
