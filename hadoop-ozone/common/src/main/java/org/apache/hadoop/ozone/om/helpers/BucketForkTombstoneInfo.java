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

import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketForkTombstoneTypeProto;

/**
 * Metadata for a fork-local tombstone that hides a base snapshot entry.
 */
public final class BucketForkTombstoneInfo
    implements CopyObject<BucketForkTombstoneInfo> {
  private static final Codec<BucketForkTombstoneInfo> CODEC =
      new DelegatedCodec<>(
          Proto2Codec.get(OzoneManagerProtocolProtos
              .BucketForkTombstoneInfo.getDefaultInstance()),
          BucketForkTombstoneInfo::getFromProtobuf,
          BucketForkTombstoneInfo::getProtobuf,
          BucketForkTombstoneInfo.class);

  private final UUID forkId;
  private final String targetVolumeName;
  private final String targetBucketName;
  private final UUID baseSnapshotId;
  private final String logicalPath;
  private final long objectId;
  private final long creationTime;
  private final long updateId;
  private final BucketForkTombstoneType type;

  private BucketForkTombstoneInfo(Builder builder) {
    this.forkId = builder.forkId;
    this.targetVolumeName = builder.targetVolumeName;
    this.targetBucketName = builder.targetBucketName;
    this.baseSnapshotId = builder.baseSnapshotId;
    this.logicalPath = builder.logicalPath;
    this.objectId = builder.objectId;
    this.creationTime = builder.creationTime;
    this.updateId = builder.updateId;
    this.type = builder.type;
  }

  public static Codec<BucketForkTombstoneInfo> getCodec() {
    return CODEC;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setForkId(forkId)
        .setTargetVolumeName(targetVolumeName)
        .setTargetBucketName(targetBucketName)
        .setBaseSnapshotId(baseSnapshotId)
        .setLogicalPath(logicalPath)
        .setObjectId(objectId)
        .setCreationTime(creationTime)
        .setUpdateId(updateId)
        .setType(type);
  }

  public UUID getForkId() {
    return forkId;
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

  public String getLogicalPath() {
    return logicalPath;
  }

  public long getObjectId() {
    return objectId;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getUpdateId() {
    return updateId;
  }

  public BucketForkTombstoneType getType() {
    return type;
  }

  public String getTableKey() {
    return getTableKey(targetVolumeName, targetBucketName, logicalPath);
  }

  public static String getTableKey(String volumeName, String bucketName,
      String logicalPath) {
    String normalizedPath = logicalPath;
    while (normalizedPath.startsWith(OM_KEY_PREFIX)) {
      normalizedPath = normalizedPath.substring(OM_KEY_PREFIX.length());
    }
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName
        + OM_KEY_PREFIX + normalizedPath;
  }

  public OzoneManagerProtocolProtos.BucketForkTombstoneInfo getProtobuf() {
    return OzoneManagerProtocolProtos.BucketForkTombstoneInfo.newBuilder()
        .setForkID(toProtobuf(forkId))
        .setTargetVolumeName(targetVolumeName)
        .setTargetBucketName(targetBucketName)
        .setBaseSnapshotID(toProtobuf(baseSnapshotId))
        .setLogicalPath(logicalPath)
        .setObjectID(objectId)
        .setCreationTime(creationTime)
        .setUpdateID(updateId)
        .setType(type.toProto())
        .build();
  }

  public static Builder builderFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkTombstoneInfo proto) {
    return newBuilder()
        .setForkId(fromProtobuf(proto.getForkID()))
        .setTargetVolumeName(proto.getTargetVolumeName())
        .setTargetBucketName(proto.getTargetBucketName())
        .setBaseSnapshotId(fromProtobuf(proto.getBaseSnapshotID()))
        .setLogicalPath(proto.getLogicalPath())
        .setObjectId(proto.getObjectID())
        .setCreationTime(proto.getCreationTime())
        .setUpdateId(proto.getUpdateID())
        .setType(BucketForkTombstoneType.valueOf(proto.getType()));
  }

  public static BucketForkTombstoneInfo getFromProtobuf(
      OzoneManagerProtocolProtos.BucketForkTombstoneInfo proto) {
    return builderFromProtobuf(proto).build();
  }

  @Override
  public BucketForkTombstoneInfo copyObject() {
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
    BucketForkTombstoneInfo that = (BucketForkTombstoneInfo) o;
    return objectId == that.objectId
        && creationTime == that.creationTime
        && updateId == that.updateId
        && Objects.equals(forkId, that.forkId)
        && Objects.equals(targetVolumeName, that.targetVolumeName)
        && Objects.equals(targetBucketName, that.targetBucketName)
        && Objects.equals(baseSnapshotId, that.baseSnapshotId)
        && Objects.equals(logicalPath, that.logicalPath)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(forkId, targetVolumeName, targetBucketName,
        baseSnapshotId, logicalPath, objectId, creationTime, updateId, type);
  }

  @Override
  public String toString() {
    return "BucketForkTombstoneInfo{"
        + "forkId=" + forkId
        + ", targetVolumeName='" + targetVolumeName + '\''
        + ", targetBucketName='" + targetBucketName + '\''
        + ", baseSnapshotId=" + baseSnapshotId
        + ", logicalPath='" + logicalPath + '\''
        + ", objectId=" + objectId
        + ", creationTime=" + creationTime
        + ", updateId=" + updateId
        + ", type=" + type
        + '}';
  }

  /**
   * Type of fork-local tombstone.
   */
  public enum BucketForkTombstoneType {
    KEY,
    DIRECTORY,
    PREFIX;

    public static BucketForkTombstoneType valueOf(
        BucketForkTombstoneTypeProto proto) {
      switch (proto) {
      case DIRECTORY:
        return DIRECTORY;
      case PREFIX:
        return PREFIX;
      case KEY:
      default:
        return KEY;
      }
    }

    public BucketForkTombstoneTypeProto toProto() {
      switch (this) {
      case DIRECTORY:
        return BucketForkTombstoneTypeProto.DIRECTORY;
      case PREFIX:
        return BucketForkTombstoneTypeProto.PREFIX;
      case KEY:
      default:
        return BucketForkTombstoneTypeProto.KEY;
      }
    }
  }

  /**
   * Builder for BucketForkTombstoneInfo.
   */
  public static final class Builder {
    private UUID forkId;
    private String targetVolumeName = "";
    private String targetBucketName = "";
    private UUID baseSnapshotId;
    private String logicalPath = "";
    private long objectId;
    private long creationTime;
    private long updateId;
    private BucketForkTombstoneType type = BucketForkTombstoneType.KEY;

    private Builder() {
    }

    public Builder setForkId(UUID forkId) {
      this.forkId = forkId;
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

    public Builder setLogicalPath(String logicalPath) {
      this.logicalPath = logicalPath;
      return this;
    }

    public Builder setObjectId(long objectId) {
      this.objectId = objectId;
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

    public Builder setType(BucketForkTombstoneType type) {
      this.type = type;
      return this;
    }

    public BucketForkTombstoneInfo build() {
      Objects.requireNonNull(forkId, "forkId");
      Objects.requireNonNull(baseSnapshotId, "baseSnapshotId");
      Objects.requireNonNull(type, "type");
      return new BucketForkTombstoneInfo(this);
    }
  }
}
