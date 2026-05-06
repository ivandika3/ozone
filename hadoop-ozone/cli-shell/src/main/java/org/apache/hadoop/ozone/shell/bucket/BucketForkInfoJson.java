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

package org.apache.hadoop.ozone.shell.bucket;

import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;

/**
 * JSON-safe shell view for {@link BucketForkInfo}.
 */
final class BucketForkInfoJson {
  private final BucketForkInfo info;

  private BucketForkInfoJson(BucketForkInfo info) {
    this.info = info;
  }

  static BucketForkInfoJson of(BucketForkInfo info) {
    return new BucketForkInfoJson(info);
  }

  public UUID getForkId() {
    return info.getForkId();
  }

  public String getSourceVolumeName() {
    return info.getSourceVolumeName();
  }

  public String getSourceBucketName() {
    return info.getSourceBucketName();
  }

  public String getTargetVolumeName() {
    return info.getTargetVolumeName();
  }

  public String getTargetBucketName() {
    return info.getTargetBucketName();
  }

  public UUID getBaseSnapshotId() {
    return info.getBaseSnapshotId();
  }

  public String getBaseSnapshotName() {
    return info.getBaseSnapshotName();
  }

  public long getSourceBucketObjectId() {
    return info.getSourceBucketObjectId();
  }

  public long getTargetBucketObjectId() {
    return info.getTargetBucketObjectId();
  }

  public long getCreationTime() {
    return info.getCreationTime();
  }

  public long getDeletionTime() {
    return info.getDeletionTime();
  }

  public BucketForkInfo.BucketForkStatus getStatus() {
    return info.getStatus();
  }

  public long getQuotaBaselineBytes() {
    return info.getQuotaBaselineBytes();
  }

  public long getQuotaBaselineNamespace() {
    return info.getQuotaBaselineNamespace();
  }

  public boolean isCreatedFromActiveBucket() {
    return info.isCreatedFromActiveBucket();
  }
}
