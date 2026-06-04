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
 * Tests BucketForkBaseViewInfo metadata structure.
 */
public class TestBucketForkBaseViewInfo {
  private static final UUID BASE_VIEW_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID PARENT_BASE_VIEW_ID = UUID.randomUUID();
  private static final UUID SOURCE_FORK_ID = UUID.randomUUID();
  private static final String SOURCE_VOLUME = "source-vol";
  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String SNAPSHOT_NAME = "base-snapshot";
  private static final long SOURCE_BUCKET_OBJECT_ID = 100L;
  private static final long CREATION_TIME = Time.now();
  private static final long UPDATE_ID = 200L;

  private BucketForkBaseViewInfo createBaseViewInfo() {
    return BucketForkBaseViewInfo.newBuilder()
        .setBaseViewId(BASE_VIEW_ID)
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setSourceBucketObjectId(SOURCE_BUCKET_OBJECT_ID)
        .setSnapshotId(SNAPSHOT_ID)
        .setSnapshotName(SNAPSHOT_NAME)
        .setParentBaseViewId(PARENT_BASE_VIEW_ID)
        .setSourceForkId(SOURCE_FORK_ID)
        .setCreationTime(CREATION_TIME)
        .setUpdateId(UPDATE_ID)
        .setStatus(BucketForkBaseViewInfo.BucketForkBaseViewStatus.ACTIVE)
        .setReferenceCount(3L)
        .build();
  }

  private OzoneManagerProtocolProtos.BucketForkBaseViewInfo
      createBaseViewInfoProto() {
    return OzoneManagerProtocolProtos.BucketForkBaseViewInfo.newBuilder()
        .setBaseViewID(toProtobuf(BASE_VIEW_ID))
        .setSourceVolumeName(SOURCE_VOLUME)
        .setSourceBucketName(SOURCE_BUCKET)
        .setSourceBucketObjectID(SOURCE_BUCKET_OBJECT_ID)
        .setSnapshotID(toProtobuf(SNAPSHOT_ID))
        .setSnapshotName(SNAPSHOT_NAME)
        .setParentBaseViewID(toProtobuf(PARENT_BASE_VIEW_ID))
        .setSourceForkID(toProtobuf(SOURCE_FORK_ID))
        .setCreationTime(CREATION_TIME)
        .setUpdateID(UPDATE_ID)
        .setStatus(OzoneManagerProtocolProtos
            .BucketForkBaseViewStatusProto.BUCKET_FORK_BASE_VIEW_ACTIVE)
        .setReferenceCount(3L)
        .build();
  }

  @Test
  public void testBaseViewInfoToProto() {
    assertEquals(createBaseViewInfoProto(), createBaseViewInfo().getProtobuf());
  }

  @Test
  public void testBaseViewInfoFromProto() {
    assertEquals(createBaseViewInfo(),
        BucketForkBaseViewInfo.getFromProtobuf(createBaseViewInfoProto()));
  }

  @Test
  public void testDeletedBaseViewIsNotActive() {
    BucketForkBaseViewInfo deleted = createBaseViewInfo().toBuilder()
        .setStatus(BucketForkBaseViewInfo.BucketForkBaseViewStatus.DELETED)
        .build();

    assertFalse(deleted.isActive());
  }
}
