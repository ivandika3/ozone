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

package org.apache.hadoop.hdds.scm.container;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class TestAllocateContainerWithStorageTier {

  @TempDir
  private File testDir;

  private DBStore dbStore;
  private ContainerManager containerManager;

  @BeforeEach
  void setUp() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    PipelineManager pipelineManager = Mockito.spy(new MockPipelineManager(
        dbStore, scmhaManager, new MockNodeManager(true, 10)));
    Mockito.doReturn(true).when(pipelineManager)
        .hasEnoughSpace(Mockito.any(Pipeline.class), Mockito.anyLong());
    ContainerReplicaPendingOps pendingOps = Mockito.mock(
        ContainerReplicaPendingOps.class);
    containerManager = new ContainerManagerImpl(conf, scmhaManager,
        sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore), pendingOps);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  void storesRequestedStorageTierOnAllocatedContainer() throws Exception {
    ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        "admin", StorageTier.SSD);

    assertEquals(StorageTier.SSD, container.getStorageTier());
    assertEquals(StorageTier.SSD,
        containerManager.getContainer(container.containerID()).getStorageTier());
    assertEquals(StorageTier.SSD,
        ContainerInfo.fromProtobuf(container.getProtobuf()).getStorageTier());
  }

  @Test
  void defaultsMissingProtoStorageTierToDefaultTier() {
    ContainerInfoProto containerInfoProto = HddsProtos.ContainerInfoProto
        .newBuilder()
        .setContainerID(1L)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setUsedBytes(0L)
        .setNumberOfKeys(0L)
        .setOwner("admin")
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .build();

    assertEquals(StorageTier.getDefaultTier(),
        ContainerInfo.fromProtobuf(containerInfoProto).getStorageTier());
  }
}
