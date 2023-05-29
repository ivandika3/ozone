/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniMultiOMOzoneHACluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;

/**
 * This class tests MiniMultiOMOzoneHACluster.
 */
public class TestMiniMultiOMOzoneHACluster {

  // Multiple Ozone clusters with the same SCM instance
  private static MiniMultiOMOzoneHACluster cluster;
  private static OzoneConfiguration conf;
  private String clusterId;
  private List<String> omServiceIds;

  private String scmId;
  private String scmServiceId;
  private int numOfOmsPerCluster = 3;
  private int numOfOmClusters = 2;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMiniMultiOMOzoneHACluster.class);


  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    scmServiceId = "scm-service-test1";
    omServiceIds = Arrays.asList(
        "omServiceId1",
        "omServiceId2"
    );
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    cluster = new MiniMultiOMOzoneHACluster.Builder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceIds(omServiceIds)
        .setSCMServiceId(scmServiceId)
        .setNumOfOMClusters(numOfOmClusters)
        .setNumOfOMPerCluster(numOfOmsPerCluster)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetOMLeaders() throws ExecutionException,
      InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(
        numOfOmClusters);
    List<Future> futureList = new ArrayList<>(numOfOmClusters);
    for (int i = 0; i < numOfOmClusters; i++) {
      int index = i;
      Future future = executorService.submit(() -> {
        try {
          testGetOMLeader(index);
          return true;
        } catch (Exception e) {
          LOG.error("Exception thrown when waiting for" +
              " OM leader for cluster index {}.", index, e);
          return false;
        }
      });
      futureList.add(future);
    }

    for (Future future: futureList) {
      future.get();
    }

  }

  public void testGetOMLeader(int index) throws InterruptedException,
      TimeoutException {
    AtomicReference<OzoneManager> ozoneManager = new AtomicReference<>();
    // Wait for OM leader election to finish
    GenericTestUtils.waitFor(() -> {
      OzoneManager om = cluster.getOMLeader(index);
      ozoneManager.set(om);
      return om != null;
    }, 100, 120000);

    Assert.assertNotNull("Timed out waiting OM leader election to finish: "
        + "no leader or more than one leader.", ozoneManager);
    Assert.assertTrue("Should have gotten the leader!",
        ozoneManager.get().isLeaderReady());
  }

}
