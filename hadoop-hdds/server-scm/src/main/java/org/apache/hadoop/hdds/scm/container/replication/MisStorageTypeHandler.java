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

package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repairs replicas whose persisted storage type does not match the hosting
 * volume storage type by creating a replacement replica on a correctly typed
 * target.
 */
public class MisStorageTypeHandler implements UnhealthyReplicationHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MisStorageTypeHandler.class);

  private final PlacementPolicy ratisPlacementPolicy;
  private final PlacementPolicy ecPlacementPolicy;
  private final ReplicationManager replicationManager;

  public MisStorageTypeHandler(PlacementPolicy ratisPlacementPolicy,
      PlacementPolicy ecPlacementPolicy,
      ReplicationManager replicationManager) {
    this.ratisPlacementPolicy = ratisPlacementPolicy;
    this.ecPlacementPolicy = ecPlacementPolicy;
    this.replicationManager = replicationManager;
  }

  @Override
  public int processAndSendCommands(Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps, ContainerHealthResult result,
      int remainingMaintenanceRedundancy) throws IOException {
    ContainerInfo container = result.getContainerInfo();
    if (!pendingOps.isEmpty()) {
      return 0;
    }

    StorageType expectedStorageType;
    try {
      expectedStorageType = StorageTierUtil.getStorageTypeForUniformStorageTier(
          container.getStorageTier(), container.getReplicationConfig());
    } catch (Exception e) {
      LOG.debug("Skipping container {} for misplaced storage type handling",
          container, e);
      return 0;
    }

    ContainerReplica replicaToMove = null;
    for (ContainerReplica replica : replicas) {
      if (replica.getStorageType() == null
          || replica.getVolumeStorageType() == null
          || replica.getStorageType() != expectedStorageType) {
        return 0;
      }
      if (replica.getStorageType() != replica.getVolumeStorageType()) {
        replicaToMove = replica;
      }
    }
    if (replicaToMove == null) {
      return 0;
    }

    ContainerReplicaCount replicaCount = createReplicaCount(
        container, replicas, pendingOps, remainingMaintenanceRedundancy);
    if (!replicaCount.isSufficientlyReplicated()
        || replicaCount.isOverReplicated()) {
      return 0;
    }

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(
            container, new ArrayList<>(replicas), Collections.emptySet(),
            Collections.emptyList(), replicationManager);
    List<DatanodeDetails> targets = ReplicationManagerUtil.getTargetDatanodes(
        getPlacementPolicy(container), 1, excludedAndUsedNodes.getUsedNodes(),
        excludedAndUsedNodes.getExcludedNodes(), container.getUsedBytes(),
        container, expectedStorageType);
    if (targets.isEmpty()) {
      return 0;
    }

    return sendReplicationCommand(container, replicaToMove, targets.get(0),
        expectedStorageType);
  }

  private int sendReplicationCommand(ContainerInfo container,
      ContainerReplica replicaToMove, DatanodeDetails target,
      StorageType expectedStorageType)
      throws CommandTargetOverloadedException, NotLeaderException {
    DatanodeDetails source = replicaToMove.getDatanodeDetails();
    if (replicationManager.getConfig().isPush()) {
      replicationManager.sendThrottledReplicationCommand(container,
          Collections.singletonList(source), target,
          replicaToMove.getReplicaIndex(), expectedStorageType);
    } else {
      ReplicateContainerCommand command = ReplicateContainerCommand.fromSources(
          container.getContainerID(), Collections.singletonList(source),
          expectedStorageType);
      command.setReplicaIndex(replicaToMove.getReplicaIndex());
      replicationManager.sendDatanodeCommand(command, container, target);
    }
    return 1;
  }

  private PlacementPolicy getPlacementPolicy(ContainerInfo container) {
    return container.getReplicationType() == HddsProtos.ReplicationType.EC
        ? ecPlacementPolicy : ratisPlacementPolicy;
  }

  private ContainerReplicaCount createReplicaCount(ContainerInfo container,
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      int remainingMaintenanceRedundancy) {
    if (container.getReplicationType() == HddsProtos.ReplicationType.EC) {
      return new ECContainerReplicaCount(container, replicas, pendingOps,
          remainingMaintenanceRedundancy);
    }
    return new RatisContainerReplicaCount(container, replicas, pendingOps,
        remainingMaintenanceRedundancy, true);
  }
}
