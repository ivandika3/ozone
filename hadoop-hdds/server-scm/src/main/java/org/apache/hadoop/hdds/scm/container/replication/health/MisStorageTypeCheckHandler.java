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

package org.apache.hadoop.hdds.scm.container.replication.health;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ECContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;

/**
 * Detects closed containers whose replicas are stored on the wrong volume
 * storage type.
 */
public class MisStorageTypeCheckHandler extends AbstractCheck {

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (!hasMisStorageTypeReplica(request)) {
      return false;
    }

    request.getReport().incrementAndSample(ContainerHealthState.MIS_STORAGE_TYPE,
        request.getContainerInfo());
    if (!request.isReadOnly()) {
      request.getReplicationQueue().enqueue(
          new ContainerHealthResult.MisStorageTypeHealthResult(
              request.getContainerInfo()));
    }
    return true;
  }

  private boolean hasMisStorageTypeReplica(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    if (container.getState() != HddsProtos.LifeCycleState.CLOSED) {
      return false;
    }
    if (container.getStorageTier() == null
        || !container.getStorageTier().isUniformStorageType()) {
      return false;
    }
    if (!request.getPendingOps().isEmpty()) {
      return false;
    }
    if (container.getReplicationType() != HddsProtos.ReplicationType.RATIS
        && container.getReplicationType() != HddsProtos.ReplicationType.EC) {
      return false;
    }

    StorageType expectedStorageType;
    try {
      expectedStorageType = StorageTierUtil.getStorageTypeForUniformStorageTier(
          container.getStorageTier(), container.getReplicationConfig());
    } catch (Exception e) {
      return false;
    }

    ContainerReplicaCount replicaCount = createReplicaCount(container,
        request.getContainerReplicas(), request.getPendingOps(),
        request.getMaintenanceRedundancy());
    if (!replicaCount.isSufficientlyReplicated()
        || replicaCount.isOverReplicated()) {
      return false;
    }

    boolean foundMismatch = false;
    for (ContainerReplica replica : request.getContainerReplicas()) {
      if (replica.getStorageType() == null
          || replica.getVolumeStorageType() == null
          || replica.getStorageType() != expectedStorageType) {
        return false;
      }
      if (replica.getStorageType() != replica.getVolumeStorageType()) {
        foundMismatch = true;
      }
    }
    return foundMismatch;
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
