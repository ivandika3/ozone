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

package org.apache.hadoop.ozone.protocol.commands;

import java.util.Objects;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SetContainerStorageTypeCommandProto;

/**
 * Asks datanode to set the storage type for a specific container replica.
 */
public class SetContainerStorageTypeCommand
    extends SCMCommand<SetContainerStorageTypeCommandProto> {

  private final long containerID;
  private final int replicaIndex;
  private final StorageType storageType;

  public SetContainerStorageTypeCommand(long containerID, int replicaIndex,
      StorageType storageType) {
    super();
    this.containerID = containerID;
    this.replicaIndex = replicaIndex;
    this.storageType = storageType;
  }

  public SetContainerStorageTypeCommand(long containerID, int replicaIndex,
      StorageType storageType, long cmdId) {
    super(cmdId);
    this.containerID = containerID;
    this.replicaIndex = replicaIndex;
    this.storageType = storageType;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.setContainerStorageTypeCommand;
  }

  @Override
  public SetContainerStorageTypeCommandProto getProto() {
    return SetContainerStorageTypeCommandProto.newBuilder()
        .setContainerID(containerID)
        .setReplicaIndex(replicaIndex)
        .setStorageType(StorageTypeUtils.getStorageTypeProto(storageType))
        .setCmdId(getId())
        .build();
  }

  public static SetContainerStorageTypeCommand getFromProtobuf(
      SetContainerStorageTypeCommandProto proto) {
    Objects.requireNonNull(proto);
    return new SetContainerStorageTypeCommand(
        proto.getContainerID(),
        proto.getReplicaIndex(),
        StorageTypeUtils.getFromProtobuf(proto.getStorageType()),
        proto.getCmdId());
  }

  public long getContainerID() {
    return containerID;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @Override
  public String toString() {
    return getType()
        + ": containerID: " + containerID
        + ", replicaIndex: " + replicaIndex
        + ", storageType: " + storageType
        + ", commandId: " + getId();
  }
}
