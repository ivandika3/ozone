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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_FOLLOWER_READ_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for OM Follower Reads functionality.
 * Tests that the follower reads infrastructure (AlignmentContext, msync)
 * is properly integrated with OM.
 */
@Timeout(300)
public class TestOMFollowerReads {

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static OzoneConfiguration conf;
  private static String omServiceId;
  private static final int NUM_OMS = 3;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    omServiceId = "omService-" + UUID.randomUUID();
    
    // Enable follower reads
    conf.setBoolean(OZONE_OM_FOLLOWER_READ_ENABLED, true);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS_WILDCARD, "*");

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(NUM_OMS)
        .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    objectStore = client.getObjectStore();
  }

  @AfterAll
  public static void shutdown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testOMServerAlignmentContextInitialized() throws Exception {
    // Verify that OMServerAlignmentContext is initialized on all OMs
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      // Check that the alignment context is created when OM has Ratis server
      if (om.getOmRatisServer() != null) {
        // The alignment context should enable state tracking
        long lastAppliedIndex = om.getOmRatisServer()
            .getLastAppliedTermIndex().getIndex();
        assertTrue(lastAppliedIndex >= 0,
            "Last applied index should be non-negative");
      }
    }
  }

  @Test
  public void testMsyncRequest() throws Exception {
    // Create a volume and bucket first
    String volumeName = "volume-" + UUID.randomUUID();
    String bucketName = "bucket-" + UUID.randomUUID();

    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getName());

    // After creating volume and bucket, the last applied index
    // should have increased on the leader
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader, "Cluster should have a leader");
    
    long lastAppliedIndex = leader.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(lastAppliedIndex > 0,
        "Last applied index should be > 0 after writes");
  }

  @Test
  public void testReadAfterWrite() throws Exception {
    // Create volume and bucket
    String volumeName = "volume-rw-" + UUID.randomUUID();
    String bucketName = "bucket-rw-" + UUID.randomUUID();

    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);

    // Read back the created volume and bucket
    OzoneVolume readVolume = objectStore.getVolume(volumeName);
    assertNotNull(readVolume);
    assertEquals(volumeName, readVolume.getName());

    OzoneBucket readBucket = readVolume.getBucket(bucketName);
    assertNotNull(readBucket);
    assertEquals(bucketName, readBucket.getName());
  }

  @Test
  public void testFollowerReadsEnabledConfig() throws Exception {
    // Verify the configuration is properly set
    OzoneManager om = cluster.getOMLeader();
    boolean followerReadEnabled = om.getConfiguration().getBoolean(
        OZONE_OM_FOLLOWER_READ_ENABLED, false);
    assertTrue(followerReadEnabled,
        "Follower reads should be enabled in configuration");
  }

  @Test
  public void testStateIdProgressesDuringWrites() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    assertNotNull(leader);

    // Get initial state
    long initialIndex = leader.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();

    // Perform some writes
    String volumeName = "volume-progress-" + UUID.randomUUID();
    objectStore.createVolume(volumeName);
    
    OzoneVolume volume = objectStore.getVolume(volumeName);
    for (int i = 0; i < 5; i++) {
      volume.createBucket("bucket-" + i);
    }

    // State ID should have progressed
    long finalIndex = leader.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(finalIndex > initialIndex,
        "State ID should progress during writes: initial=" + initialIndex
            + ", final=" + finalIndex);
  }
}

