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

package org.apache.hadoop.ozone.client;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests ObjectStore bucket fork client APIs.
 */
public class TestObjectStoreBucketFork {

  @Test
  public void testCreateBucketForkDelegatesToProxy() throws IOException {
    ClientProtocol proxy = Mockito.mock(ClientProtocol.class);
    ObjectStore store = new ObjectStore(new OzoneConfiguration(), proxy);
    BucketForkInfo forkInfo = createForkInfo();
    when(proxy.createBucketFork("source-vol", "source-bucket",
        "target-vol", "fork", "snap")).thenReturn(forkInfo);

    assertSame(forkInfo, store.createBucketFork("source-vol",
        "source-bucket", "target-vol", "fork", "snap"));
  }

  @Test
  public void testDeleteBucketForkDelegatesToProxy() throws IOException {
    ClientProtocol proxy = Mockito.mock(ClientProtocol.class);
    ObjectStore store = new ObjectStore(new OzoneConfiguration(), proxy);

    store.deleteBucketFork("vol", "fork");

    verify(proxy).deleteBucketFork("vol", "fork");
  }

  private static BucketForkInfo createForkInfo() {
    return BucketForkInfo.newBuilder()
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName("source-vol")
        .setSourceBucketName("source-bucket")
        .setTargetVolumeName("target-vol")
        .setTargetBucketName("fork")
        .setBaseSnapshotId(UUID.randomUUID())
        .setBaseSnapshotName("snap")
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .build();
  }
}
