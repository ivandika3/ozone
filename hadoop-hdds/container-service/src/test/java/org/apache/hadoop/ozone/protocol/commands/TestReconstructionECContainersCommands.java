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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.ECReconstructionTarget;
import org.junit.jupiter.api.Test;

/**
 * Test ECReconstructionContainersCommand.
 */
public class TestReconstructionECContainersCommands {

  @Test
  public void testExceptionIfSourceAndMissingNotSameLength() {
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    final ByteString missingContainerIndexes = UnsafeByteOperations.unsafeWrap(new byte[]{1, 2});

    List<ECReconstructionTarget> targetDns = new ArrayList<>();
    targetDns.add(new ECReconstructionTarget(MockDatanodeDetails.randomDatanodeDetails(),
        StorageType.DEFAULT));

    assertThrows(IllegalArgumentException.class,
        () -> new ReconstructECContainersCommand(1L, Collections.emptyList(),
        targetDns, missingContainerIndexes, ecReplicationConfig));
  }

  @Test
  public void protobufConversion() {
    byte[] missingIndexes = {1, 2};
    final ByteString missingContainerIndexes = UnsafeByteOperations.unsafeWrap(missingIndexes);
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    final List<ECReconstructionTarget> dnDetails = getDNDetails(5);

    List<DatanodeDetailsAndReplicaIndex>
        sources = dnDetails.stream().map(a -> new DatanodeDetailsAndReplicaIndex(
            a.getDatanodeDetails(), dnDetails.indexOf(a)))
        .collect(Collectors.toList());
    List<ECReconstructionTarget> targets = getDNDetails(2);
    ReconstructECContainersCommand reconstructECContainersCommand =
        new ReconstructECContainersCommand(1L, sources, targets,
            missingContainerIndexes, ecReplicationConfig);

    assertThat(reconstructECContainersCommand.toString())
        .contains("missingIndexes: " + Arrays.toString(missingIndexes));

    StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto
        proto = reconstructECContainersCommand.getProto();

    List<DatanodeDetailsAndReplicaIndex>
        srcDnsFromProto = proto.getSourcesList().stream().map(
        DatanodeDetailsAndReplicaIndex::fromProto).collect(Collectors.toList());
    List<ECReconstructionTarget> targetDnsFromProto = proto.getReconstructionTargetsList().stream()
        .map(ECReconstructionTarget::fromProto)
        .collect(Collectors.toList());
    assertEquals(1L, proto.getContainerID());
    assertEquals(sources, srcDnsFromProto);
    assertEquals(targets, targetDnsFromProto);
    assertEquals(missingContainerIndexes, proto.getMissingContainerIndexes());
    assertEquals(ecReplicationConfig,
        new ECReplicationConfig(proto.getEcReplicationConfig()));

    ReconstructECContainersCommand fromProtobuf =
        ReconstructECContainersCommand.getFromProtobuf(proto);

    assertEquals(reconstructECContainersCommand.getContainerID(),
        fromProtobuf.getContainerID());
    assertEquals(reconstructECContainersCommand.getSources(),
        fromProtobuf.getSources());
    assertEquals(reconstructECContainersCommand.getTargetDatanodes(),
        fromProtobuf.getTargetDatanodes());
    assertEquals(reconstructECContainersCommand.getMissingContainerIndexes(),
        fromProtobuf.getMissingContainerIndexes());
    assertEquals(
        reconstructECContainersCommand.getEcReplicationConfig(),
        fromProtobuf.getEcReplicationConfig());
  }

  private List<ECReconstructionTarget> getDNDetails(int numDns) {
    List<ECReconstructionTarget> dns = new ArrayList<>();
    for (int i = 0; i < numDns; i++) {
      dns.add(new ECReconstructionTarget(MockDatanodeDetails.randomDatanodeDetails(),
          StorageType.DEFAULT));
    }
    return dns;
  }

}
