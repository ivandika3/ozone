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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMFollowerReadProxyProvider - proxy provider that routes
 * reads to followers and writes to leader.
 */
public class TestOMFollowerReadProxyProvider {

  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String NODE_ID_BASE_STR = "omNode-";
  private static final String DUMMY_NODE_ADDR = "0.0.0.0:8080";

  private OMFollowerReadProxyProvider<OzoneManagerProtocolPB> provider;
  private OMClientAlignmentContext alignmentContext;
  private int numNodes = 3;
  private OzoneConfiguration config;

  @BeforeEach
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    StringJoiner allNodeIds = new StringJoiner(",");
    for (int i = 1; i <= numNodes; i++) {
      String nodeId = NODE_ID_BASE_STR + i;
      config.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId), DUMMY_NODE_ADDR);
      allNodeIds.add(nodeId);
    }
    config.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
    
    alignmentContext = new OMClientAlignmentContext();
    provider = new OMFollowerReadProxyProvider<>(
        config,
        UserGroupInformation.getCurrentUser(),
        OM_SERVICE_ID,
        OzoneManagerProtocolPB.class,
        alignmentContext);
  }

  @Test
  public void testInitialization() {
    assertNotNull(provider);
    assertEquals(numNodes, provider.getOmNodeIDList().size());
    assertNotNull(provider.getAlignmentContext());
    assertEquals(alignmentContext, provider.getAlignmentContext());
  }

  @Test
  public void testFollowerNodeIds() {
    // Initially all nodes are potential followers
    List<String> followerIds = provider.getFollowerNodeIds();
    assertEquals(numNodes, followerIds.size());
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "1"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "2"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testUpdateLeader() {
    // Update leader to node 1
    provider.updateLeader(NODE_ID_BASE_STR + "1");
    
    assertEquals(NODE_ID_BASE_STR + "1", provider.getLeaderNodeId());
    List<String> followerIds = provider.getFollowerNodeIds();
    assertEquals(numNodes - 1, followerIds.size());
    assertFalse(followerIds.contains(NODE_ID_BASE_STR + "1"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "2"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testUpdateLeaderChangesLeader() {
    // Set initial leader
    provider.updateLeader(NODE_ID_BASE_STR + "1");
    assertEquals(NODE_ID_BASE_STR + "1", provider.getLeaderNodeId());

    // Change leader to node 2
    provider.updateLeader(NODE_ID_BASE_STR + "2");
    assertEquals(NODE_ID_BASE_STR + "2", provider.getLeaderNodeId());

    // Old leader should be back as follower
    List<String> followerIds = provider.getFollowerNodeIds();
    assertEquals(numNodes - 1, followerIds.size());
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "1"));
    assertFalse(followerIds.contains(NODE_ID_BASE_STR + "2"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testMarkFollowerUnavailable() {
    // Mark node 2 as unavailable
    provider.markFollowerUnavailable(NODE_ID_BASE_STR + "2");

    List<String> followerIds = provider.getFollowerNodeIds();
    assertEquals(numNodes - 1, followerIds.size());
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "1"));
    assertFalse(followerIds.contains(NODE_ID_BASE_STR + "2"));
    assertTrue(followerIds.contains(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testMarkFollowerAvailable() {
    // First mark as unavailable
    provider.markFollowerUnavailable(NODE_ID_BASE_STR + "2");
    assertFalse(provider.getFollowerNodeIds().contains(NODE_ID_BASE_STR + "2"));

    // Then mark as available again
    provider.markFollowerAvailable(NODE_ID_BASE_STR + "2");
    assertTrue(provider.getFollowerNodeIds().contains(NODE_ID_BASE_STR + "2"));
  }

  @Test
  public void testMarkLeaderAsFollowerDoesNothing() {
    // Set node 1 as leader
    provider.updateLeader(NODE_ID_BASE_STR + "1");

    // Try to mark leader as available follower - should do nothing
    provider.markFollowerAvailable(NODE_ID_BASE_STR + "1");
    assertFalse(provider.getFollowerNodeIds().contains(NODE_ID_BASE_STR + "1"));
  }
}

