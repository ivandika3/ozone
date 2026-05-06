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
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketForkBaseViewInfo;
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
    if (!request.hasBaseViewID()) {
      builder.setBaseViewID(toProtobuf(UUID.randomUUID()));
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
      String sourceBucketKey = metadataManager.getBucketKey(sourceVolumeName, sourceBucketName);
      String targetBucketKey = metadataManager.getBucketKey(targetVolumeName, targetBucketName);

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

      checkNamespaceQuota(targetVolumeArgs);
      SnapshotInfo baseSnapshotInfo = getOrCreateBaseSnapshot(ozoneManager,
          metadataManager, request, sourceBucketInfo, transactionLogIndex,
          context);

      long targetBucketObjectId =
          ozoneManager.getObjectIdFromTxId(transactionLogIndex);
      long creationTime = request.hasCreationTime()
          ? request.getCreationTime() : transactionLogIndex;

      OmBucketInfo targetBucketInfo = buildTargetBucketInfo(request,
          sourceBucketInfo, targetBucketObjectId, transactionLogIndex,
          creationTime, baseSnapshotInfo);
      BucketForkInfo bucketForkInfo = buildBucketForkInfo(request,
          sourceBucketInfo, baseSnapshotInfo, targetBucketObjectId,
          transactionLogIndex, creationTime);
      BucketForkBaseViewInfo baseViewInfo = buildBaseViewInfo(request,
          sourceBucketInfo, baseSnapshotInfo, transactionLogIndex,
          creationTime);

      targetVolumeArgs = targetVolumeArgs.toBuilder()
          .incrUsedNamespace(1L)
          .build();

      addTargetToCache(metadataManager, transactionLogIndex, targetVolumeKey,
          targetBucketKey, targetVolumeArgs, targetBucketInfo);
      addForkToCache(metadataManager, transactionLogIndex, bucketForkInfo,
          baseSnapshotInfo, baseViewInfo);

      omResponse.setCreateBucketForkResponse(
          CreateBucketForkResponse.newBuilder()
              .setBucketForkInfo(bucketForkInfo.getProtobuf())
              .build());
      omClientResponse = new OMBucketForkCreateResponse(omResponse.build(),
          bucketForkInfo, targetBucketInfo, targetVolumeArgs.copyObject(),
          bucketForkInfo.isCreatedFromActiveBucket() ? baseSnapshotInfo : null,
          baseViewInfo);
      LOG.info("Created bucket fork {}/{} from snapshot {} of {}/{}",
          targetVolumeName, targetBucketName, baseSnapshotInfo.getName(),
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

  private static OmBucketInfo buildTargetBucketInfo(
      CreateBucketForkRequest request, OmBucketInfo sourceBucketInfo,
      long targetBucketObjectId, long transactionLogIndex, long creationTime,
      SnapshotInfo baseSnapshotInfo) {
    return OmBucketInfo.newBuilder()
        .setVolumeName(request.getTargetVolumeName())
        .setBucketName(request.getTargetBucketName())
        .setAcls(sourceBucketInfo.getAcls())
        .setIsVersionEnabled(sourceBucketInfo.getIsVersionEnabled())
        .setStorageType(sourceBucketInfo.getStorageType())
        .setObjectID(targetBucketObjectId)
        .setUpdateID(transactionLogIndex)
        .setCreationTime(creationTime)
        .setModificationTime(creationTime)
        .setBucketEncryptionKey(sourceBucketInfo.getEncryptionKeyInfo())
        .setBucketLayout(sourceBucketInfo.getBucketLayout())
        .setOwner(sourceBucketInfo.getOwner())
        .setDefaultReplicationConfig(
            sourceBucketInfo.getDefaultReplicationConfig())
        .addAllMetadata(sourceBucketInfo.getMetadata())
        .setUsedBytes(baseSnapshotInfo.getReferencedSize())
        .setUsedNamespace(sourceBucketInfo.getUsedNamespace())
        .setQuotaInBytes(sourceBucketInfo.getQuotaInBytes())
        .setQuotaInNamespace(sourceBucketInfo.getQuotaInNamespace())
        .build();
  }

  private static BucketForkInfo buildBucketForkInfo(
      CreateBucketForkRequest request, OmBucketInfo sourceBucketInfo,
      SnapshotInfo baseSnapshotInfo, long targetBucketObjectId,
      long transactionLogIndex, long creationTime) {
    return BucketForkInfo.newBuilder()
        .setForkId(request.hasForkId()
            ? fromProtobuf(request.getForkId())
            : new UUID(0L, transactionLogIndex))
        .setSourceVolumeName(request.getSourceVolumeName())
        .setSourceBucketName(request.getSourceBucketName())
        .setTargetVolumeName(request.getTargetVolumeName())
        .setTargetBucketName(request.getTargetBucketName())
        .setBaseSnapshotId(baseSnapshotInfo.getSnapshotId())
        .setBaseSnapshotName(baseSnapshotInfo.getName())
        .setBaseViewId(baseViewId(request, transactionLogIndex))
        .setSourceBucketObjectId(sourceBucketInfo.getObjectID())
        .setTargetBucketObjectId(targetBucketObjectId)
        .setCreationTime(creationTime)
        .setDeletionTime(-1L)
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .setQuotaBaselineBytes(baseSnapshotInfo.getReferencedSize())
        .setQuotaBaselineNamespace(sourceBucketInfo.getUsedNamespace())
        .setCreatedFromActiveBucket(!hasBaseSnapshotName(request))
        .setLineageDepth(1)
        .build();
  }

  private static BucketForkBaseViewInfo buildBaseViewInfo(
      CreateBucketForkRequest request, OmBucketInfo sourceBucketInfo,
      SnapshotInfo baseSnapshotInfo, long transactionLogIndex,
      long creationTime) {
    return BucketForkBaseViewInfo.newBuilder()
        .setBaseViewId(baseViewId(request, transactionLogIndex))
        .setSourceVolumeName(request.getSourceVolumeName())
        .setSourceBucketName(request.getSourceBucketName())
        .setSourceBucketObjectId(sourceBucketInfo.getObjectID())
        .setSnapshotId(baseSnapshotInfo.getSnapshotId())
        .setSnapshotName(baseSnapshotInfo.getName())
        .setCreationTime(creationTime)
        .setUpdateId(transactionLogIndex)
        .setStatus(BucketForkBaseViewInfo.BucketForkBaseViewStatus.ACTIVE)
        .setReferenceCount(1L)
        .build();
  }

  private static void addTargetToCache(OMMetadataManager metadataManager,
      long transactionLogIndex, String targetVolumeKey, String targetBucketKey,
      OmVolumeArgs targetVolumeArgs, OmBucketInfo targetBucketInfo) {
    metadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(targetVolumeKey),
        CacheValue.get(transactionLogIndex, targetVolumeArgs));
    metadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(targetBucketKey),
        CacheValue.get(transactionLogIndex, targetBucketInfo));
  }

  private static void addForkToCache(OMMetadataManager metadataManager,
      long transactionLogIndex, BucketForkInfo bucketForkInfo,
      SnapshotInfo baseSnapshotInfo, BucketForkBaseViewInfo baseViewInfo) {
    if (bucketForkInfo.isCreatedFromActiveBucket()) {
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(baseSnapshotInfo.getTableKey()),
          CacheValue.get(transactionLogIndex, baseSnapshotInfo));
    }
    metadataManager.getBucketForkTable().addCacheEntry(
        new CacheKey<>(bucketForkInfo.getTableKey()),
        CacheValue.get(transactionLogIndex, bucketForkInfo));
    metadataManager.getBucketForkBaseViewTable().addCacheEntry(
        new CacheKey<>(baseViewInfo.getTableKey()),
        CacheValue.get(transactionLogIndex, baseViewInfo));
  }

  private static SnapshotInfo getOrCreateBaseSnapshot(
      OzoneManager ozoneManager, OMMetadataManager metadataManager,
      CreateBucketForkRequest request, OmBucketInfo sourceBucketInfo,
      long transactionLogIndex, ExecutionContext context)
      throws IOException {
    if (!hasBaseSnapshotName(request)) {
      return createInternalBaseSnapshot(ozoneManager, metadataManager, request,
          sourceBucketInfo, transactionLogIndex, context);
    }

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

  private static SnapshotInfo createInternalBaseSnapshot(
      OzoneManager ozoneManager, OMMetadataManager metadataManager,
      CreateBucketForkRequest request, OmBucketInfo sourceBucketInfo,
      long transactionLogIndex, ExecutionContext context) throws IOException {
    UUID snapshotId = deterministicSnapshotId(request, transactionLogIndex);
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(
        request.getSourceVolumeName(), request.getSourceBucketName(),
        BucketForkInfo.INTERNAL_BASE_SNAPSHOT_PREFIX + snapshotId, snapshotId,
        request.hasCreationTime() ? request.getCreationTime()
            : transactionLogIndex);
    snapshotInfo.setReferencedSize(sourceBucketInfo.getUsedBytes());
    snapshotInfo.setReferencedReplicatedSize(sourceBucketInfo.getUsedBytes());
    snapshotInfo.setCreateTransactionInfo(
        TransactionInfo.valueOf(context.getTermIndex()).toByteString());
    snapshotInfo.setLastTransactionInfo(
        TransactionInfo.valueOf(context.getTermIndex()).toByteString());

    SnapshotChainManager snapshotChainManager =
        ((OmMetadataManagerImpl) metadataManager).getSnapshotChainManager();
    synchronized (snapshotChainManager) {
      snapshotInfo.setPathPreviousSnapshotId(
          snapshotChainManager.getLatestPathSnapshotId(
              snapshotInfo.getSnapshotPath()));
      snapshotInfo.setGlobalPreviousSnapshotId(
          snapshotChainManager.getLatestGlobalSnapshotId());
      snapshotChainManager.addSnapshot(snapshotInfo);
    }
    ozoneManager.getMetrics().incNumSnapshotActive();
    return snapshotInfo;
  }

  private static UUID deterministicSnapshotId(CreateBucketForkRequest request,
      long transactionLogIndex) {
    String forkMaterial = request.hasForkId()
        ? fromProtobuf(request.getForkId()).toString()
        : String.valueOf(transactionLogIndex);
    return UUID.nameUUIDFromBytes((request.getSourceVolumeName() + "/"
        + request.getSourceBucketName() + "/" + request.getTargetVolumeName()
        + "/" + request.getTargetBucketName() + "/" + transactionLogIndex
        + "/" + forkMaterial).getBytes(StandardCharsets.UTF_8));
  }

  private static UUID baseViewId(CreateBucketForkRequest request,
      long transactionLogIndex) {
    if (request.hasBaseViewID()) {
      return fromProtobuf(request.getBaseViewID());
    }
    return UUID.nameUUIDFromBytes(("base-view/"
        + request.getSourceVolumeName() + "/" + request.getSourceBucketName()
        + "/" + request.getTargetVolumeName() + "/"
        + request.getTargetBucketName() + "/" + transactionLogIndex)
        .getBytes(StandardCharsets.UTF_8));
  }

  private static boolean hasBaseSnapshotName(CreateBucketForkRequest request) {
    return request.hasBaseSnapshotName()
        && StringUtils.isNotBlank(request.getBaseSnapshotName());
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
