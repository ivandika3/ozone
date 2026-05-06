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

import java.util.UUID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests BucketForkTombstoneInfo metadata structure.
 */
public class TestBucketForkTombstoneInfo {
  private static final UUID FORK_ID = UUID.randomUUID();
  private static final UUID BASE_SNAPSHOT_ID = UUID.randomUUID();
  private static final String TARGET_VOLUME = "vol";
  private static final String TARGET_BUCKET = "fork";
  private static final String LOGICAL_PATH = "dir/key";
  private static final long OBJECT_ID = 123L;
  private static final long CREATION_TIME = 456L;
  private static final long UPDATE_ID = 789L;

  @Test
  public void testTombstoneInfoRoundTrip() {
    BucketForkTombstoneInfo tombstone = createTombstoneInfo();

    assertEquals(createTombstoneInfoProto(), tombstone.getProtobuf());
    assertEquals(tombstone,
        BucketForkTombstoneInfo.getFromProtobuf(tombstone.getProtobuf()));
  }

  @Test
  public void testTableKeyIncludesForkBucketAndPath() {
    assertEquals("/vol/fork/dir/key",
        BucketForkTombstoneInfo.getTableKey(TARGET_VOLUME, TARGET_BUCKET,
            LOGICAL_PATH));
  }

  private static BucketForkTombstoneInfo createTombstoneInfo() {
    return BucketForkTombstoneInfo.newBuilder()
        .setForkId(FORK_ID)
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotId(BASE_SNAPSHOT_ID)
        .setLogicalPath(LOGICAL_PATH)
        .setObjectId(OBJECT_ID)
        .setCreationTime(CREATION_TIME)
        .setUpdateId(UPDATE_ID)
        .setType(BucketForkTombstoneInfo.BucketForkTombstoneType.KEY)
        .build();
  }

  private static OzoneManagerProtocolProtos.BucketForkTombstoneInfo
      createTombstoneInfoProto() {
    return OzoneManagerProtocolProtos.BucketForkTombstoneInfo.newBuilder()
        .setForkID(toProtobuf(FORK_ID))
        .setTargetVolumeName(TARGET_VOLUME)
        .setTargetBucketName(TARGET_BUCKET)
        .setBaseSnapshotID(toProtobuf(BASE_SNAPSHOT_ID))
        .setLogicalPath(LOGICAL_PATH)
        .setObjectID(OBJECT_ID)
        .setCreationTime(CREATION_TIME)
        .setUpdateID(UPDATE_ID)
        .setType(OzoneManagerProtocolProtos.BucketForkTombstoneTypeProto.KEY)
        .build();
  }
}
