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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_FORK_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for bucket forks through the public Ozone client API.
 */
public class TestOzoneBucketFork {
  private static final ReplicationConfig RATIS_ONE =
      ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
          ReplicationFactor.ONE);
  private static MiniOzoneCluster cluster;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_BUCKET_FORK_ENABLED, true);
    conf.setBoolean(OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.OBJECT_STORE.name());
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testNamedSnapshotForkMutatesIndependently() throws Exception {
    ObjectStore store = client.getObjectStore();
    OzoneVolume volume = createVolume(store);
    OzoneBucket source = createBucket(volume, "source");
    String snapshotName = "snap";
    String forkBucketName = uniqueName("fork");

    writeKey(source, "base-key", "base");
    store.createSnapshot(volume.getName(), source.getName(), snapshotName);

    BucketForkInfo forkInfo = store.createBucketFork(volume.getName(),
        source.getName(), volume.getName(), forkBucketName, snapshotName);
    OzoneBucket fork = volume.getBucket(forkBucketName);

    writeKey(source, "base-key", "source-new");
    writeKey(source, "source-only", "source-only");
    writeKey(fork, "base-key", "fork-new");
    writeKey(fork, "fork-only", "fork-only");

    assertEquals("source-new", TestDataUtil.getKey(source, "base-key"));
    assertEquals("source-only", TestDataUtil.getKey(source, "source-only"));
    assertEquals("fork-new", TestDataUtil.getKey(fork, "base-key"));
    assertEquals("fork-only", TestDataUtil.getKey(fork, "fork-only"));
    assertThrows(IOException.class,
        () -> TestDataUtil.getKey(fork, "source-only"));

    Set<String> forkKeys = listKeyNames(fork);
    assertTrue(forkKeys.contains("base-key"));
    assertTrue(forkKeys.contains("fork-only"));
    assertFalse(forkKeys.contains("source-only"));
    assertEquals(forkInfo.getBaseSnapshotId(),
        store.getBucketForkInfo(volume.getName(), forkBucketName)
            .getBaseSnapshotId());
  }

  @Test
  public void testMultipleForksShareNamedSnapshot() throws Exception {
    ObjectStore store = client.getObjectStore();
    OzoneVolume volume = createVolume(store);
    OzoneBucket source = createBucket(volume, "source");
    String snapshotName = "snap";
    String forkA = uniqueName("fork-a");
    String forkB = uniqueName("fork-b");

    writeKey(source, "base-key", "base");
    store.createSnapshot(volume.getName(), source.getName(), snapshotName);

    BucketForkInfo first = store.createBucketFork(volume.getName(),
        source.getName(), volume.getName(), forkA, snapshotName);
    BucketForkInfo second = store.createBucketFork(volume.getName(),
        source.getName(), volume.getName(), forkB, snapshotName);

    assertEquals(first.getBaseSnapshotId(), second.getBaseSnapshotId());
    Set<String> forks = listForkBuckets(store, volume.getName());
    assertTrue(forks.contains(forkA));
    assertTrue(forks.contains(forkB));
  }

  @Test
  public void testForkMetadataSurvivesRestartAndDeletionReleasesSnapshot()
      throws Exception {
    ObjectStore store = client.getObjectStore();
    OzoneVolume volume = createVolume(store);
    OzoneBucket source = createBucket(volume, "source");
    String snapshotName = "snap";
    String forkBucketName = uniqueName("fork");

    writeKey(source, "base-key", "base");
    store.createSnapshot(volume.getName(), source.getName(), snapshotName);
    BucketForkInfo created = store.createBucketFork(volume.getName(),
        source.getName(), volume.getName(), forkBucketName, snapshotName);

    cluster.restartOzoneManager();
    ObjectStore restartedStore = client.getObjectStore();

    BucketForkInfo reloaded = restartedStore.getBucketForkInfo(volume.getName(),
        forkBucketName);
    assertEquals(created.getBaseSnapshotId(), reloaded.getBaseSnapshotId());
    OMException exception = assertThrows(OMException.class,
        () -> restartedStore.deleteSnapshot(volume.getName(), source.getName(),
            snapshotName));
    assertEquals(CONTAINS_SNAPSHOT, exception.getResult());

    restartedStore.deleteBucketFork(volume.getName(), forkBucketName);
    restartedStore.deleteSnapshot(volume.getName(), source.getName(),
        snapshotName);
  }

  @Test
  public void testActiveSourceForkCreatesHiddenInternalSnapshot()
      throws Exception {
    ObjectStore store = client.getObjectStore();
    OzoneVolume volume = createVolume(store);
    OzoneBucket source = createBucket(volume, "source");
    String forkBucketName = uniqueName("fork");

    writeKey(source, "base-key", "base");
    BucketForkInfo forkInfo = store.createBucketFork(volume.getName(),
        source.getName(), volume.getName(), forkBucketName, null);

    assertTrue(forkInfo.isCreatedFromActiveBucket());
    assertTrue(BucketForkInfo.isInternalBaseSnapshotName(
        forkInfo.getBaseSnapshotName()));
    assertEquals("base",
        TestDataUtil.getKey(volume.getBucket(forkBucketName), "base-key"));
    assertFalse(listSnapshots(store, volume.getName(), source.getName())
        .contains(forkInfo.getBaseSnapshotName()));
  }

  private static OzoneVolume createVolume(ObjectStore store)
      throws IOException {
    String volumeName = uniqueName("vol");
    store.createVolume(volumeName);
    return store.getVolume(volumeName);
  }

  private static OzoneBucket createBucket(OzoneVolume volume, String prefix)
      throws IOException {
    String bucketName = uniqueName(prefix);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .build());
    return volume.getBucket(bucketName);
  }

  private static void writeKey(OzoneBucket bucket, String key, String value)
      throws IOException {
    TestDataUtil.createKey(bucket, key, RATIS_ONE,
        value.getBytes(StandardCharsets.UTF_8));
  }

  private static Set<String> listKeyNames(OzoneBucket bucket)
      throws IOException {
    Set<String> keys = new HashSet<>();
    Iterator<? extends OzoneKey> iterator = bucket.listKeys(null);
    while (iterator.hasNext()) {
      keys.add(iterator.next().getName());
    }
    return keys;
  }

  private static Set<String> listForkBuckets(ObjectStore store,
      String volumeName) throws IOException {
    Set<String> forks = new HashSet<>();
    Iterator<BucketForkInfo> iterator = store.listBucketForks(volumeName,
        "fork", null);
    while (iterator.hasNext()) {
      forks.add(iterator.next().getTargetBucketName());
    }
    return forks;
  }

  private static Set<String> listSnapshots(ObjectStore store,
      String volumeName, String bucketName) throws IOException {
    Set<String> snapshots = new HashSet<>();
    Iterator<OzoneSnapshot> iterator = store.listSnapshot(volumeName,
        bucketName, null, null);
    while (iterator.hasNext()) {
      snapshots.add(iterator.next().getName());
    }
    return snapshots;
  }

  private static String uniqueName(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }
}
