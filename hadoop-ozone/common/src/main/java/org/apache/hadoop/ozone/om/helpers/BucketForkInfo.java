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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketForkStatusProto;

/**
 * Metadata for a mutable bucket fork.
 */
public final class BucketForkInfo implements Auditable, CopyObject<BucketForkInfo> {
  public static final String INTERNAL_BASE_SNAPSHOT_PREFIX = "bucket-fork-";

  private static final Codec<BucketForkInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.BucketForkInfo.getDefaultInstance()),
      BucketForkInfo::getFromProtobuf,
      BucketForkInfo::getProtobuf,
      BucketForkInfo.class);

  private final UUID forkId;
  private final String sourceVolumeName;
  private final String sourceBucketName;
  private final String targetVolumeName;
  private final String targetBucketName;
  private final UUID baseSnapshotId;
  private final String baseSnapshotName;
  private final long sourceBucketObjectId;
  private final long targetBucketObjectId;
  private final long creationTime;
  private final long deletionTime;
  private final BucketForkStatus status;
  private final long quotaBaselineBytes;
  private final long quotaBaselineNamespace;
  private final boolean createdFromActiveBucket;
  private final UUID baseViewId;
  private final UUID sourceForkId;
  private final int lineageDepth;

  private BucketForkInfo(Builder builder) {
    this.forkId = builder.forkId;
    this.sourceVolumeName = builder.sourceVolumeName;
    this.sourceBucketName = builder.sourceBucketName;
    this.targetVolumeName = builder.targetVolumeName;
    this.targetBucketName = builder.targetBucketName;
    this.baseSnapshotId = builder.baseSnapshotId;
    this.baseSnapshotName = builder.baseSnapshotName;
    this.sourceBucketObjectId = builder.sourceBucketObjectId;
    this.targetBucketObjectId = builder.targetBucketObjectId;
    this.creationTime = builder.creationTime;
    this.deletionTime = builder.deletionTime;
    this.status = builder.status;
    this.quotaBaselineBytes = builder.quotaBaselineBytes;
    this.quotaBaselineNamespace = builder.quotaBaselineNamespace;
    this.createdFromActiveBucket = builder.createdFromActiveBucket;
    this.baseViewId = builder.baseViewId;
    this.sourceForkId = builder.sourceForkId;
    this.lineageDepth = builder.lineageDepth;
  }

  public static Codec<BucketForkInfo> getCodec() {
    return CODEC;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setForkId(forkId)
        .setSourceVolumeName(sourceVolumeName)
        .setSourceBucketName(sourceBucketName)
        .setTargetVolumeName(targetVolumeName)
        .setTargetBucketName(targetBucketName)
        .setBaseSnapshotId(baseSnapshotId)
        .setBaseSnapshotName(baseSnapshotName)
        .setSourceBucketObjectId(sourceBucketObjectId)
        .setTargetBucketObjectId(targetBucketObjectId)
        .setCreationTime(creationTime)
        .setDeletionTime(deletionTime)
        .setStatus(status)
        .setQuotaBaselineBytes(quotaBaselineBytes)
        .setQuotaBaselineNamespace(quotaBaselineNamespace)
        .setCreatedFromActiveBucket(createdFromActiveBucket)
        .setBaseViewId(baseViewId)
        .setSourceForkId(sourceForkId)
        .setLineageDepth(lineageDepth);
  }

  public UUID getForkId() {
    return forkId;
  }

  public String getSourceVolumeName() {
    return sourceVolumeName;
  }

  public String getSourceBucketName() {
    return sourceBucketName;
  }

  public String getTargetVolumeName() {
    return targetVolumeName;
  }

  public String getTargetBucketName() {
    return targetBucketName;
  }

  public UUID getBaseSnapshotId() {
    return baseSnapshotId;
  }

  public String getBaseSnapshotName() {
    return baseSnapshotName;
  }

  public long getSourceBucketObjectId() {
    return sourceBucketObjectId;
  }

  public long getTargetBucketObjectId() {
    return targetBucketObjectId;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getDeletionTime() {
    return deletionTime;
  }

  public BucketForkStatus getStatus() {
    return status;
  }

  public long getQuotaBaselineBytes() {
    return quotaBaselineBytes;
  }

  public long getQuotaBaselineNamespace() {
    return quotaBaselineNamespace;
  }

  public boolean isCreatedFromActiveBucket() {
    return createdFromActiveBucket;
  }

  public UUID getBaseViewId() {
    return baseViewId;
  }

  public UUID getSourceForkId() {
    return sourceForkId;
  }

  public int getLineageDepth() {
    return lineageDepth;
  }

  public boolean isActive() {
    return status == BucketForkStatus.BUCKET_FORK_ACTIVE;
  }

  public String getTableKey() {
    return getTableKey(targetVolumeName, targetBucketName);
  }

  public static String getTableKey(String volumeName, String bucketName) {
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName;
  }

  public static boolean isInternalBaseSnapshotName(String snapshotName) {
    return snapshotName != null
        && snapshotName.startsWith(INTERNAL_BASE_SNAPSHOT_PREFIX);
  }

  public OzoneManagerProtocolProtos.BucketForkInfo getProtobuf() {
    OzoneManagerProtocolProtos.BucketForkInfo.Builder builder =
        OzoneManagerProtocolProtos.BucketForkInfo.newBuilder()
            .setForkID(toProtobuf(forkId))
            .setSourceVolumeName(sourceVolumeName)
            .setSourceBucketName(sourceBucketName)
            .setTargetVolumeName(targetVolumeName)
            .setTargetBucketName(targetBucketName)
            .setBaseSnapshotID(toProtobuf(baseSnapshotId))
            .setBaseSnapshotName(baseSnapshotName)
            .setSourceBucketObjectID(sourceBucketObjectId)
            .setTargetBucketObjectID(targetBucketObjectId)
            .setCreationTime(creationTime)
            .setDeletionTime(deletionTime)
            .setStatus(status.toProto())
            .setQuotaBaselineBytes(quotaBaselineBytes)
            .setQuotaBaselineNamespace(quotaBaselineNamespace)
            .setCreatedFromActiveBucket(createdFromActiveBucket);
    if (baseViewId != null) {
      builder.setBaseViewID(toProtobuf(baseViewId));
    }
    if (sourceForkId != null) {
      builder.setSourceForkID(toProtobuf(sourceForkId));
    }
    if (lineageDepth > 0) {
      builder.setLineageDepth(lineageDepth);
    }
    return builder.build();
  }

  public static Builder builderFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkInfo proto) {
    return newBuilder()
        .setForkId(fromProtobuf(proto.getForkID()))
        .setSourceVolumeName(proto.getSourceVolumeName())
        .setSourceBucketName(proto.getSourceBucketName())
        .setTargetVolumeName(proto.getTargetVolumeName())
        .setTargetBucketName(proto.getTargetBucketName())
        .setBaseSnapshotId(fromProtobuf(proto.getBaseSnapshotID()))
        .setBaseSnapshotName(proto.getBaseSnapshotName())
        .setSourceBucketObjectId(proto.getSourceBucketObjectID())
        .setTargetBucketObjectId(proto.getTargetBucketObjectID())
        .setCreationTime(proto.getCreationTime())
        .setDeletionTime(proto.getDeletionTime())
        .setStatus(BucketForkStatus.valueOf(proto.getStatus()))
        .setQuotaBaselineBytes(proto.getQuotaBaselineBytes())
        .setQuotaBaselineNamespace(proto.getQuotaBaselineNamespace())
        .setCreatedFromActiveBucket(proto.getCreatedFromActiveBucket())
        .setBaseViewId(proto.hasBaseViewID()
            ? fromProtobuf(proto.getBaseViewID()) : null)
        .setSourceForkId(proto.hasSourceForkID()
            ? fromProtobuf(proto.getSourceForkID()) : null)
        .setLineageDepth(proto.getLineageDepth());
  }

  public static BucketForkInfo getFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkInfo proto) {
    return builderFromProtobuf(proto).build();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, targetVolumeName);
    auditMap.put(OzoneConsts.BUCKET, targetBucketName);
    auditMap.put("sourceVolume", sourceVolumeName);
    auditMap.put("sourceBucket", sourceBucketName);
    auditMap.put("baseSnapshot", baseSnapshotName);
    return auditMap;
  }

  @Override
  public BucketForkInfo copyObject() {
    return toBuilder().build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BucketForkInfo that = (BucketForkInfo) o;
    return sourceBucketObjectId == that.sourceBucketObjectId
        && targetBucketObjectId == that.targetBucketObjectId
        && creationTime == that.creationTime
        && deletionTime == that.deletionTime
        && quotaBaselineBytes == that.quotaBaselineBytes
        && quotaBaselineNamespace == that.quotaBaselineNamespace
        && createdFromActiveBucket == that.createdFromActiveBucket
        && lineageDepth == that.lineageDepth
        && Objects.equals(forkId, that.forkId)
        && Objects.equals(sourceVolumeName, that.sourceVolumeName)
        && Objects.equals(sourceBucketName, that.sourceBucketName)
        && Objects.equals(targetVolumeName, that.targetVolumeName)
        && Objects.equals(targetBucketName, that.targetBucketName)
        && Objects.equals(baseSnapshotId, that.baseSnapshotId)
        && Objects.equals(baseSnapshotName, that.baseSnapshotName)
        && Objects.equals(baseViewId, that.baseViewId)
        && Objects.equals(sourceForkId, that.sourceForkId)
        && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(forkId, sourceVolumeName, sourceBucketName,
        targetVolumeName, targetBucketName, baseSnapshotId, baseSnapshotName,
        sourceBucketObjectId, targetBucketObjectId, creationTime, deletionTime,
        status, quotaBaselineBytes, quotaBaselineNamespace,
        createdFromActiveBucket, baseViewId, sourceForkId, lineageDepth);
  }

  @Override
  public String toString() {
    return "BucketForkInfo{" +
        "forkId=" + forkId +
        ", sourceVolumeName='" + sourceVolumeName + '\'' +
        ", sourceBucketName='" + sourceBucketName + '\'' +
        ", targetVolumeName='" + targetVolumeName + '\'' +
        ", targetBucketName='" + targetBucketName + '\'' +
        ", baseSnapshotId=" + baseSnapshotId +
        ", baseSnapshotName='" + baseSnapshotName + '\'' +
        ", baseViewId=" + baseViewId +
        ", sourceForkId=" + sourceForkId +
        ", lineageDepth=" + lineageDepth +
        ", status=" + status +
        '}';
  }

  /**
   * Bucket fork lifecycle status.
   */
  public enum BucketForkStatus {
    BUCKET_FORK_ACTIVE,
    BUCKET_FORK_DELETED;

    public static BucketForkStatus valueOf(BucketForkStatusProto proto) {
      switch (proto) {
      case BUCKET_FORK_ACTIVE:
        return BUCKET_FORK_ACTIVE;
      case BUCKET_FORK_DELETED:
        return BUCKET_FORK_DELETED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid bucket fork status, found status=" + proto);
      }
    }

    public BucketForkStatusProto toProto() {
      switch (this) {
      case BUCKET_FORK_ACTIVE:
        return BucketForkStatusProto.BUCKET_FORK_ACTIVE;
      case BUCKET_FORK_DELETED:
        return BucketForkStatusProto.BUCKET_FORK_DELETED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid bucket fork status, found status=" + this);
      }
    }
  }

  /**
   * Builder for BucketForkInfo.
   */
  public static final class Builder {
    private UUID forkId;
    private String sourceVolumeName;
    private String sourceBucketName;
    private String targetVolumeName;
    private String targetBucketName;
    private UUID baseSnapshotId;
    private String baseSnapshotName;
    private long sourceBucketObjectId;
    private long targetBucketObjectId;
    private long creationTime;
    private long deletionTime = -1L;
    private BucketForkStatus status = BucketForkStatus.BUCKET_FORK_ACTIVE;
    private long quotaBaselineBytes;
    private long quotaBaselineNamespace;
    private boolean createdFromActiveBucket;
    private UUID baseViewId;
    private UUID sourceForkId;
    private int lineageDepth;

    private Builder() {
    }

    public Builder setForkId(UUID forkId) {
      this.forkId = forkId;
      return this;
    }

    public Builder setSourceVolumeName(String sourceVolumeName) {
      this.sourceVolumeName = sourceVolumeName;
      return this;
    }

    public Builder setSourceBucketName(String sourceBucketName) {
      this.sourceBucketName = sourceBucketName;
      return this;
    }

    public Builder setTargetVolumeName(String targetVolumeName) {
      this.targetVolumeName = targetVolumeName;
      return this;
    }

    public Builder setTargetBucketName(String targetBucketName) {
      this.targetBucketName = targetBucketName;
      return this;
    }

    public Builder setBaseSnapshotId(UUID baseSnapshotId) {
      this.baseSnapshotId = baseSnapshotId;
      return this;
    }

    public Builder setBaseSnapshotName(String baseSnapshotName) {
      this.baseSnapshotName = baseSnapshotName;
      return this;
    }

    public Builder setSourceBucketObjectId(long sourceBucketObjectId) {
      this.sourceBucketObjectId = sourceBucketObjectId;
      return this;
    }

    public Builder setTargetBucketObjectId(long targetBucketObjectId) {
      this.targetBucketObjectId = targetBucketObjectId;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setDeletionTime(long deletionTime) {
      this.deletionTime = deletionTime;
      return this;
    }

    public Builder setStatus(BucketForkStatus status) {
      this.status = status;
      return this;
    }

    public Builder setQuotaBaselineBytes(long quotaBaselineBytes) {
      this.quotaBaselineBytes = quotaBaselineBytes;
      return this;
    }

    public Builder setQuotaBaselineNamespace(long quotaBaselineNamespace) {
      this.quotaBaselineNamespace = quotaBaselineNamespace;
      return this;
    }

    public Builder setCreatedFromActiveBucket(boolean createdFromActiveBucket) {
      this.createdFromActiveBucket = createdFromActiveBucket;
      return this;
    }

    public Builder setBaseViewId(UUID baseViewId) {
      this.baseViewId = baseViewId;
      return this;
    }

    public Builder setSourceForkId(UUID sourceForkId) {
      this.sourceForkId = sourceForkId;
      return this;
    }

    public Builder setLineageDepth(int lineageDepth) {
      this.lineageDepth = lineageDepth;
      return this;
    }

    public BucketForkInfo build() {
      Objects.requireNonNull(forkId, "forkId == null");
      Objects.requireNonNull(sourceVolumeName, "sourceVolumeName == null");
      Objects.requireNonNull(sourceBucketName, "sourceBucketName == null");
      Objects.requireNonNull(targetVolumeName, "targetVolumeName == null");
      Objects.requireNonNull(targetBucketName, "targetBucketName == null");
      Objects.requireNonNull(baseSnapshotId, "baseSnapshotId == null");
      Objects.requireNonNull(baseSnapshotName, "baseSnapshotName == null");
      Objects.requireNonNull(status, "status == null");
      return new BucketForkInfo(this);
    }
  }
}
