/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.MultiOMMiniOzoneHACluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.junit.Assert.fail;

/**
 * Base class for Multiple Ozone Manager HA pointing to One SCM HA tests.
 */
@Timeout(300)
public abstract class TestMultiOzoneManagerHA {

  private static MultiOMMiniOzoneHACluster cluster = null;
  private static MultiOMMiniOzoneHACluster.Builder clusterBuilder = null;
  private static List<ObjectStore> objectStores;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static List<String> omServiceIds;
  private static String scmServiceId;
  private static int numOfOmClusters = 2;
  private static int numOfOMsPerCluster = 3;
  private static final int LOG_PURGE_GAP = 50;
  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final Duration RETRY_CACHE_DURATION = Duration.ofSeconds(30);
  private static List<OzoneClient> clients;


  public MultiOMMiniOzoneHACluster getCluster() {
    return cluster;
  }

  public List<ObjectStore> getObjectStores() {
    return objectStores;
  }

  public ObjectStore getObjectStore(int index) {
    return objectStores.get(index);
  }

  public static OzoneClient getClient(int index) {
    return clients.get(index);
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  public MultiOMMiniOzoneHACluster.Builder getClusterBuilder() {
    return clusterBuilder;
  }

  public List<String> getOmServiceIds() {
    return omServiceIds;
  }

  public String getOmServiceId(int index) {
    return omServiceIds.get(index);
  }

  public static int getLogPurgeGap() {
    return LOG_PURGE_GAP;
  }

  public static long getSnapshotThreshold() {
    return SNAPSHOT_THRESHOLD;
  }

  public static int getNumOfOmClusters() {
    return numOfOmClusters;
  }

  public static int getNumOfOMsPerCluster() {
    return numOfOMsPerCluster;
  }

  public static int getOzoneClientFailoverMaxAttempts() {
    return OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS;
  }

  public static Duration getRetryCacheDuration() {
    return RETRY_CACHE_DURATION;
  }

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceIds = Arrays.asList(
        "omServiceId1",
        "omServiceId2"
    );
    scmServiceId = "sc-service-test1";
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    /* Reduce IPC retry interval to speed up unit test. */
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    // Some subclasses check RocksDB directly as part of their tests. These
    // depend on OBS layout.
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE);

    OzoneManagerRatisServerConfig omHAConfig =
        conf.getObject(OzoneManagerRatisServerConfig.class);

    omHAConfig.setRetryCacheTimeout(RETRY_CACHE_DURATION);

    conf.setFromObject(omHAConfig);

    /**
     * config for key deleting service.
     */
    conf.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "10s");
    conf.set(OZONE_KEY_DELETING_LIMIT_PER_TASK, "2");

    clusterBuilder = new MultiOMMiniOzoneHACluster.Builder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceIds(omServiceIds)
        .setSCMServiceId(scmServiceId)
        .setNumOfOMHAServices(numOfOmClusters)
        .setNumOfOmsPerHAService(numOfOMsPerCluster);

    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();

    objectStores = new ArrayList<>();
    clients = new ArrayList<>();
    for (String omServiceId: omServiceIds) {
      OzoneClient client = OzoneClientFactory.getRpcClient(omServiceId, conf);
      ObjectStore objectStore = client.getObjectStore();
      clients.add(client);
      objectStores.add(objectStore);
    }
  }


  /**
   * Reset cluster between tests.
   */
  @AfterEach
  public void resetCluster()
      throws IOException {
    for (int i = 0; i < numOfOmClusters; i++) {
      if (cluster != null) {
        cluster.restartOzoneManager(i);
      }
    }
  }

  /**
   * Shutdown MiniDFSCluster after all tests of a class have run.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create a key in the bucket.
   *
   * @return the key name.
   */
  public static String createKey(OzoneBucket ozoneBucket) throws IOException {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    String data = "data" + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
        data.length(), ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();
    return keyName;
  }

  protected OzoneBucket setupBucket(int index) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    ObjectStore objectStore = getObjectStore(index);

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
    Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
    Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

    String bucketName = UUID.randomUUID().toString();
    retVolumeinfo.createBucket(bucketName);

    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
    Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

    return ozoneBucket;
  }

  /**
   * Stop the current leader OM.
   *
   * @throws Exception
   */
  protected void stopLeaderOM(int index) {
    //Stop the leader OM.
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil.getFailoverProxyProvider(
            (RpcClient) getObjectStore(index).getClientProxy());

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    cluster.stopOzoneManager(index, leaderOMNodeId);
  }

  /**
   * Create a volume and test its attribute.
   */
  protected void createVolumeTest(int index, boolean checkSuccess)
      throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      getObjectStore(index).createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = getObjectStore(index).getVolume(volumeName);

      if (checkSuccess) {
        Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
        Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
        Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));
      } else {
        // Verify that the request failed
        fail("There is no quorum. Request should have failed");
      }
    } catch (ConnectException | RemoteException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains(
              "OMNotLeaderException", e);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * This method createFile and verifies the file is successfully created or
   * not.
   *
   * @param ozoneBucket
   * @param keyName
   * @param data
   * @param recursive
   * @param overwrite
   * @throws Exception
   */
  protected void testCreateFile(OzoneBucket ozoneBucket, String keyName,
                                String data, boolean recursive,
                                boolean overwrite)
      throws Exception {

    OzoneOutputStream ozoneOutputStream = ozoneBucket.createFile(keyName,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        overwrite, recursive);

    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();

    OzoneKeyDetails ozoneKeyDetails = ozoneBucket.getKey(keyName);

    Assert.assertEquals(keyName, ozoneKeyDetails.getName());
    Assert.assertEquals(ozoneBucket.getName(), ozoneKeyDetails.getBucketName());
    Assert.assertEquals(ozoneBucket.getVolumeName(),
        ozoneKeyDetails.getVolumeName());
    Assert.assertEquals(data.length(), ozoneKeyDetails.getDataSize());

    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

    byte[] fileContent = new byte[data.getBytes(UTF_8).length];
    ozoneInputStream.read(fileContent);
    Assert.assertEquals(data, new String(fileContent, UTF_8));
  }

  protected void createKeyTest(int omClusterIndex, boolean checkSuccess)
      throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      ObjectStore objectStore = getObjectStore(omClusterIndex);
      objectStore.createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

      Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
      Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
      Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();
      retVolumeinfo.createBucket(bucketName);

      OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

      Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
      Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

      String value = "random data";
      OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
          value.length(), ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE, new HashMap<>());
      ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
      ozoneOutputStream.close();

      OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      ozoneInputStream.read(fileContent);
      Assert.assertEquals(value, new String(fileContent, UTF_8));

    } catch (ConnectException | RemoteException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains(
              "OMNotLeaderException", e);
        }
      } else {
        throw e;
      }
    }
  }



}
