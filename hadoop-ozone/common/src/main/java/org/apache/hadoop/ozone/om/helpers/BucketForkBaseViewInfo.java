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

import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketForkBaseViewStatusProto;

/**
 * Metadata for a retained bucket fork base view.
 */
public final class BucketForkBaseViewInfo
    implements CopyObject<BucketForkBaseViewInfo> {
  private static final Codec<BucketForkBaseViewInfo> CODEC =
      new DelegatedCodec<>(
          Proto2Codec.get(OzoneManagerProtocolProtos
              .BucketForkBaseViewInfo.getDefaultInstance()),
          BucketForkBaseViewInfo::getFromProtobuf,
          BucketForkBaseViewInfo::getProtobuf,
          BucketForkBaseViewInfo.class);

  private final UUID baseViewId;
  private final String sourceVolumeName;
  private final String sourceBucketName;
  private final long sourceBucketObjectId;
  private final UUID snapshotId;
  private final String snapshotName;
  private final UUID parentBaseViewId;
  private final UUID sourceForkId;
  private final long creationTime;
  private final long updateId;
  private final BucketForkBaseViewStatus status;
  private final long referenceCount;

  private BucketForkBaseViewInfo(Builder builder) {
    this.baseViewId = builder.baseViewId;
    this.sourceVolumeName = builder.sourceVolumeName;
    this.sourceBucketName = builder.sourceBucketName;
    this.sourceBucketObjectId = builder.sourceBucketObjectId;
    this.snapshotId = builder.snapshotId;
    this.snapshotName = builder.snapshotName;
    this.parentBaseViewId = builder.parentBaseViewId;
    this.sourceForkId = builder.sourceForkId;
    this.creationTime = builder.creationTime;
    this.updateId = builder.updateId;
    this.status = builder.status;
    this.referenceCount = builder.referenceCount;
  }

  public static Codec<BucketForkBaseViewInfo> getCodec() {
    return CODEC;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setBaseViewId(baseViewId)
        .setSourceVolumeName(sourceVolumeName)
        .setSourceBucketName(sourceBucketName)
        .setSourceBucketObjectId(sourceBucketObjectId)
        .setSnapshotId(snapshotId)
        .setSnapshotName(snapshotName)
        .setParentBaseViewId(parentBaseViewId)
        .setSourceForkId(sourceForkId)
        .setCreationTime(creationTime)
        .setUpdateId(updateId)
        .setStatus(status)
        .setReferenceCount(referenceCount);
  }

  public UUID getBaseViewId() {
    return baseViewId;
  }

  public String getSourceVolumeName() {
    return sourceVolumeName;
  }

  public String getSourceBucketName() {
    return sourceBucketName;
  }

  public long getSourceBucketObjectId() {
    return sourceBucketObjectId;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public UUID getParentBaseViewId() {
    return parentBaseViewId;
  }

  public UUID getSourceForkId() {
    return sourceForkId;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getUpdateId() {
    return updateId;
  }

  public BucketForkBaseViewStatus getStatus() {
    return status;
  }

  public long getReferenceCount() {
    return referenceCount;
  }

  public boolean isActive() {
    return status == BucketForkBaseViewStatus.ACTIVE;
  }

  public String getTableKey() {
    return baseViewId.toString();
  }

  public static String getTableKey(UUID baseViewId) {
    return baseViewId.toString();
  }

  public OzoneManagerProtocolProtos.BucketForkBaseViewInfo getProtobuf() {
    OzoneManagerProtocolProtos.BucketForkBaseViewInfo.Builder builder =
        OzoneManagerProtocolProtos.BucketForkBaseViewInfo.newBuilder()
            .setBaseViewID(toProtobuf(baseViewId))
            .setSourceVolumeName(sourceVolumeName)
            .setSourceBucketName(sourceBucketName)
            .setSourceBucketObjectID(sourceBucketObjectId)
            .setSnapshotID(toProtobuf(snapshotId))
            .setSnapshotName(snapshotName)
            .setCreationTime(creationTime)
            .setUpdateID(updateId)
            .setStatus(status.toProto())
            .setReferenceCount(referenceCount);
    if (parentBaseViewId != null) {
      builder.setParentBaseViewID(toProtobuf(parentBaseViewId));
    }
    if (sourceForkId != null) {
      builder.setSourceForkID(toProtobuf(sourceForkId));
    }
    return builder.build();
  }

  public static Builder builderFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkBaseViewInfo proto) {
    Builder builder = newBuilder()
        .setBaseViewId(fromProtobuf(proto.getBaseViewID()))
        .setSourceVolumeName(proto.getSourceVolumeName())
        .setSourceBucketName(proto.getSourceBucketName())
        .setSourceBucketObjectId(proto.getSourceBucketObjectID())
        .setSnapshotId(fromProtobuf(proto.getSnapshotID()))
        .setSnapshotName(proto.getSnapshotName())
        .setCreationTime(proto.getCreationTime())
        .setUpdateId(proto.getUpdateID())
        .setStatus(BucketForkBaseViewStatus.valueOf(proto.getStatus()))
        .setReferenceCount(proto.getReferenceCount());
    if (proto.hasParentBaseViewID()) {
      builder.setParentBaseViewId(fromProtobuf(proto.getParentBaseViewID()));
    }
    if (proto.hasSourceForkID()) {
      builder.setSourceForkId(fromProtobuf(proto.getSourceForkID()));
    }
    return builder;
  }

  public static BucketForkBaseViewInfo getFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkBaseViewInfo proto) {
    return builderFromProtobuf(proto).build();
  }

  @Override
  public BucketForkBaseViewInfo copyObject() {
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
    BucketForkBaseViewInfo that = (BucketForkBaseViewInfo) o;
    return sourceBucketObjectId == that.sourceBucketObjectId
        && creationTime == that.creationTime
        && updateId == that.updateId
        && referenceCount == that.referenceCount
        && Objects.equals(baseViewId, that.baseViewId)
        && Objects.equals(sourceVolumeName, that.sourceVolumeName)
        && Objects.equals(sourceBucketName, that.sourceBucketName)
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(snapshotName, that.snapshotName)
        && Objects.equals(parentBaseViewId, that.parentBaseViewId)
        && Objects.equals(sourceForkId, that.sourceForkId)
        && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseViewId, sourceVolumeName, sourceBucketName,
        sourceBucketObjectId, snapshotId, snapshotName, parentBaseViewId,
        sourceForkId, creationTime, updateId, status, referenceCount);
  }

  @Override
  public String toString() {
    return "BucketForkBaseViewInfo{"
        + "baseViewId=" + baseViewId
        + ", sourceVolumeName='" + sourceVolumeName + '\''
        + ", sourceBucketName='" + sourceBucketName + '\''
        + ", snapshotId=" + snapshotId
        + ", snapshotName='" + snapshotName + '\''
        + ", parentBaseViewId=" + parentBaseViewId
        + ", sourceForkId=" + sourceForkId
        + ", status=" + status
        + '}';
  }

  /**
   * Bucket fork base view lifecycle status.
   */
  public enum BucketForkBaseViewStatus {
    ACTIVE,
    DELETED;

    public static BucketForkBaseViewStatus valueOf(
        BucketForkBaseViewStatusProto proto) {
      switch (proto) {
      case BUCKET_FORK_BASE_VIEW_ACTIVE:
        return ACTIVE;
      case BUCKET_FORK_BASE_VIEW_DELETED:
        return DELETED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid bucket fork base view status, found status="
                + proto);
      }
    }

    public BucketForkBaseViewStatusProto toProto() {
      switch (this) {
      case ACTIVE:
        return BucketForkBaseViewStatusProto.BUCKET_FORK_BASE_VIEW_ACTIVE;
      case DELETED:
        return BucketForkBaseViewStatusProto.BUCKET_FORK_BASE_VIEW_DELETED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid bucket fork base view status, found status="
                + this);
      }
    }
  }

  /**
   * Builder for BucketForkBaseViewInfo.
   */
  public static final class Builder {
    private UUID baseViewId;
    private String sourceVolumeName;
    private String sourceBucketName;
    private long sourceBucketObjectId;
    private UUID snapshotId;
    private String snapshotName;
    private UUID parentBaseViewId;
    private UUID sourceForkId;
    private long creationTime;
    private long updateId;
    private BucketForkBaseViewStatus status = BucketForkBaseViewStatus.ACTIVE;
    private long referenceCount;

    private Builder() {
    }

    public Builder setBaseViewId(UUID baseViewId) {
      this.baseViewId = baseViewId;
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

    public Builder setSourceBucketObjectId(long sourceBucketObjectId) {
      this.sourceBucketObjectId = sourceBucketObjectId;
      return this;
    }

    public Builder setSnapshotId(UUID snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public Builder setSnapshotName(String snapshotName) {
      this.snapshotName = snapshotName;
      return this;
    }

    public Builder setParentBaseViewId(UUID parentBaseViewId) {
      this.parentBaseViewId = parentBaseViewId;
      return this;
    }

    public Builder setSourceForkId(UUID sourceForkId) {
      this.sourceForkId = sourceForkId;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setUpdateId(long updateId) {
      this.updateId = updateId;
      return this;
    }

    public Builder setStatus(BucketForkBaseViewStatus status) {
      this.status = status;
      return this;
    }

    public Builder setReferenceCount(long referenceCount) {
      this.referenceCount = referenceCount;
      return this;
    }

    public BucketForkBaseViewInfo build() {
      Objects.requireNonNull(baseViewId, "baseViewId == null");
      Objects.requireNonNull(sourceVolumeName, "sourceVolumeName == null");
      Objects.requireNonNull(sourceBucketName, "sourceBucketName == null");
      Objects.requireNonNull(snapshotId, "snapshotId == null");
      Objects.requireNonNull(snapshotName, "snapshotName == null");
      Objects.requireNonNull(status, "status == null");
      return new BucketForkBaseViewInfo(this);
    }
  }
}
