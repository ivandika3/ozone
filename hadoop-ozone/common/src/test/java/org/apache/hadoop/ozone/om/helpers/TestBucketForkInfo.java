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

import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.UUID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests BucketForkInfo metadata structure.
 */
public class TestBucketForkInfo {
  private static final UUID FORK_ID = UUID.randomUUID();
  private static final UUID BASE_SNAPSHOT_ID = UUID.randomUUID();
  private static final String SOURCE_VOLUME = "source-vol";
  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String TARGET_VOLUME = "target-vol";
  private static final String TARGET_BUCKET = "target-bucket";
  private static final String BASE_SNAPSHOT_NAME = "base-snapshot";
  private static final long SOURCE_BUCKET_OBJECT_ID = 100L;
  private static final long TARGET_BUCKET_OBJECT_ID = 200L;
  private static final long CREATION_TIME = Time.now();

  private BucketForkInfo createBucketForkInfo() {
    return BucketForkInfo.newBuilder()
        .setForkId(FORK_ID)
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotId(BASE_SNAPSHOT_ID)
        .setBaseSnapshotName(BASE_SNAPSHOT_NAME)
        .setSourceBucketObjectId(SOURCE_BUCKET_OBJECT_ID)
        .setTargetBucketObjectId(TARGET_BUCKET_OBJECT_ID)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(-1L)
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .setQuotaBaselineBytes(1234L)
        .setQuotaBaselineNamespace(12L)
        .setCreatedFromActiveBucket(true)
        .build();
  }

  private OzoneManagerProtocolProtos.BucketForkInfo createBucketForkInfoProto() {
    return OzoneManagerProtocolProtos.BucketForkInfo.newBuilder()
        .setForkID(toProtobuf(FORK_ID))
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotID(toProtobuf(BASE_SNAPSHOT_ID))
        .setBaseSnapshotName(BASE_SNAPSHOT_NAME)
        .setSourceBucketObjectID(SOURCE_BUCKET_OBJECT_ID)
        .setTargetBucketObjectID(TARGET_BUCKET_OBJECT_ID)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(-1L)
        .setStatus(OzoneManagerProtocolProtos.BucketForkStatusProto.BUCKET_FORK_ACTIVE)
        .setQuotaBaselineBytes(1234L)
        .setQuotaBaselineNamespace(12L)
        .setCreatedFromActiveBucket(true)
        .build();
  }

  @Test
  public void testBucketForkInfoToProto() {
    assertEquals(createBucketForkInfoProto(), createBucketForkInfo().getProtobuf());
  }

  @Test
  public void testBucketForkInfoFromProto() {
    assertEquals(createBucketForkInfo(),
        BucketForkInfo.getFromProtobuf(createBucketForkInfoProto()));
  }

  @Test
  public void testDeletedForkIsNotActive() {
    BucketForkInfo deleted = createBucketForkInfo().toBuilder()
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_DELETED)
        .setDeletionTime(CREATION_TIME + 1)
        .build();

    assertFalse(deleted.isActive());
  }
}
