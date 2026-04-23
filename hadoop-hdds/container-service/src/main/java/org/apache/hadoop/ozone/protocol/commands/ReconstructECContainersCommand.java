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

import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

/**
 * SCM command to request reconstruction of EC containers.
 */
public class ReconstructECContainersCommand
    extends SCMCommand<ReconstructECContainersCommandProto> {
  private final long containerID;
  private final List<DatanodeDetailsAndReplicaIndex> sources;
  private final List<ECReconstructionTarget> targetDatanodes;
  private final ByteString missingContainerIndexes;
  private final ECReplicationConfig ecReplicationConfig;

  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetailsAndReplicaIndex> sources,
      List<ECReconstructionTarget> targetDatanodes,
      ByteString missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig) {
    this(containerID, sources, targetDatanodes, missingContainerIndexes,
        ecReplicationConfig, HddsIdFactory.getLongId());
  }

  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetailsAndReplicaIndex> sourceDatanodes,
      List<ECReconstructionTarget> targetDatanodes,
      ByteString missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig, long id) {
    super(id);
    this.containerID = containerID;
    this.sources = sourceDatanodes;
    this.targetDatanodes = targetDatanodes;
    this.missingContainerIndexes = missingContainerIndexes;
    this.ecReplicationConfig = ecReplicationConfig;
    if (targetDatanodes.size() != missingContainerIndexes.size()) {
      throw new IllegalArgumentException("Number of target datanodes and " +
          "container indexes should be same");
    }
  }

  @Override
  public Type getType() {
    return Type.reconstructECContainersCommand;
  }

  @Override
  public ReconstructECContainersCommandProto getProto() {
    Builder builder =
        ReconstructECContainersCommandProto.newBuilder().setCmdId(getId())
            .setContainerID(containerID);
    for (DatanodeDetailsAndReplicaIndex dd : sources) {
      builder.addSources(dd.toProto());
    }
    for (ECReconstructionTarget dd : targetDatanodes) {
      builder.addTargets(dd.getDatanodeDetails().getProtoBufMessage());
      builder.addReconstructionTargets(dd.toProto());
    }
    builder.setMissingContainerIndexes(missingContainerIndexes);
    builder.setEcReplicationConfig(ecReplicationConfig.toProto());
    return builder.build();
  }

  public static ReconstructECContainersCommand getFromProtobuf(
      ReconstructECContainersCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    List<DatanodeDetailsAndReplicaIndex> srcDatanodeDetails =
        protoMessage.getSourcesList().stream()
            .map(a -> DatanodeDetailsAndReplicaIndex.fromProto(a))
            .collect(Collectors.toList());
    List<ECReconstructionTarget> targetDatanodeDetails;
    if (protoMessage.getReconstructionTargetsCount() > 0) {
      targetDatanodeDetails = protoMessage.getReconstructionTargetsList()
          .stream()
          .map(ECReconstructionTarget::fromProto)
          .collect(Collectors.toList());
    } else {
      targetDatanodeDetails = protoMessage.getTargetsList().stream()
          .map(DatanodeDetails::getFromProtoBuf)
          .map(dn -> new ECReconstructionTarget(dn, null))
          .collect(Collectors.toList());
    }

    return new ReconstructECContainersCommand(protoMessage.getContainerID(),
        srcDatanodeDetails, targetDatanodeDetails,
        protoMessage.getMissingContainerIndexes(),
        new ECReplicationConfig(protoMessage.getEcReplicationConfig()),
        protoMessage.getCmdId()
    );
  }

  public long getContainerID() {
    return containerID;
  }

  public List<DatanodeDetailsAndReplicaIndex> getSources() {
    return sources;
  }

  public List<ECReconstructionTarget> getTargetDatanodes() {
    return targetDatanodes;
  }

  public ByteString getMissingContainerIndexes() {
    return missingContainerIndexes;
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return ecReplicationConfig;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", containerID: ").append(containerID)
        .append(", replicationConfig: ").append(ecReplicationConfig)
        .append(", sources: [").append(getSources().stream()
            .map(a -> a.dnDetails
                + " replicaIndex: " + a.getReplicaIndex())
            .collect(Collectors.joining(", "))).append(']')
        .append(", targets: ").append(getTargetDatanodes())
        .append(", missingIndexes: ").append(
            Arrays.toString(missingContainerIndexes.toByteArray()));
    return sb.toString();
  }

  /**
   * To store the datanode details with replica index.
   */
  public static class DatanodeDetailsAndReplicaIndex {
    private DatanodeDetails dnDetails;
    private int replicaIndex;

    public DatanodeDetailsAndReplicaIndex(DatanodeDetails dnDetails,
        int replicaIndex) {
      this.dnDetails = dnDetails;
      this.replicaIndex = replicaIndex;
    }

    public DatanodeDetails getDnDetails() {
      return dnDetails;
    }

    public int getReplicaIndex() {
      return replicaIndex;
    }

    public StorageContainerDatanodeProtocolProtos
        .DatanodeDetailsAndReplicaIndexProto toProto() {
      StorageContainerDatanodeProtocolProtos.DatanodeDetailsAndReplicaIndexProto.Builder builder =
          StorageContainerDatanodeProtocolProtos
              .DatanodeDetailsAndReplicaIndexProto.newBuilder()
              .setDatanodeDetails(dnDetails.getProtoBufMessage())
              .setReplicaIndex(replicaIndex);
      return builder.build();
    }

    public static DatanodeDetailsAndReplicaIndex fromProto(
        StorageContainerDatanodeProtocolProtos
            .DatanodeDetailsAndReplicaIndexProto proto) {
      return new DatanodeDetailsAndReplicaIndex(
          DatanodeDetails.getFromProtoBuf(proto.getDatanodeDetails()),
          proto.getReplicaIndex()
      );
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DatanodeDetailsAndReplicaIndex that = (DatanodeDetailsAndReplicaIndex) o;
      return replicaIndex == that.replicaIndex &&
          Objects.equals(dnDetails, that.dnDetails);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dnDetails, replicaIndex);
    }

    @Override
    public String toString() {
      return "DatanodeDetailsAndReplicaIndex{" +
          "dnDetails=" + dnDetails +
          ", replicaIndex=" + replicaIndex +
          '}';
    }
  }

  /**
   * Represents a target node for EC reconstruction, including
   * its details and the associated storage type.
   */
  public static class ECReconstructionTarget {
    private final DatanodeDetails datanodeDetails;
    @Nullable private final StorageType storageType;

    /**
     * Constructs an ECReconstructionTarget object.
     *
     * @param datanodeDetails the details of the target datanode.
     * @param storageType     the storage type of the target datanode.
     */
    public ECReconstructionTarget(DatanodeDetails datanodeDetails,
        @Nullable StorageType storageType) {
      this.datanodeDetails = datanodeDetails;
      this.storageType = storageType;
    }

    /**
     * Gets the datanode details.
     *
     * @return the datanode details.
     */
    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    /**
     * Gets the storage type.
     *
     * @return the storage type.
     */
    @Nullable public StorageType getStorageType() {
      return storageType;
    }

    public StorageContainerDatanodeProtocolProtos.ECReconstructionTargetProto toProto() {
      StorageContainerDatanodeProtocolProtos.ECReconstructionTargetProto.Builder builder =
          StorageContainerDatanodeProtocolProtos.ECReconstructionTargetProto.newBuilder()
              .setDatanodeDetails(datanodeDetails.getProtoBufMessage());
      if (storageType != null) {
        builder.setStorageType(StorageTypeUtils.getStorageTypeProto(storageType));
      }
      return builder.build();
    }

    public static ECReconstructionTarget fromProto(
        StorageContainerDatanodeProtocolProtos.ECReconstructionTargetProto proto) {
      DatanodeDetails datanodeDetails = DatanodeDetails.getFromProtoBuf(proto.getDatanodeDetails());
      StorageType storageType = proto.hasStorageType() ?
          StorageTypeUtils.getFromProtobuf(proto.getStorageType()) : null;
      return new ECReconstructionTarget(datanodeDetails, storageType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ECReconstructionTarget that = (ECReconstructionTarget) o;
      return Objects.equals(datanodeDetails, that.datanodeDetails) &&
          storageType == that.storageType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(datanodeDetails, storageType);
    }

    @Override
    public String toString() {
      return "ECReconstructionTarget{" +
          "datanodeDetails=" + datanodeDetails +
          ", storageType=" + storageType +
          '}';
    }
  }
}
