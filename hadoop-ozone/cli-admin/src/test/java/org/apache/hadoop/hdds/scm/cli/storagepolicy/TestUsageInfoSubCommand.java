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

package org.apache.hadoop.hdds.scm.cli.storagepolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeStorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for StoragePolicy usage info command.
 */
public class TestUsageInfoSubCommand {

  private UsageInfoSubCommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new UsageInfoSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
  }

  @Test
  public void testSummaryOutput() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.listStorageTypeUsageInfo(any()))
        .thenReturn(getUsageInfo());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertThat(output).contains("Cluster StorageType Usage Summary");
    assertThat(output).contains("DISK Datanode Count");
    assertThat(output).contains("SSD Datanode Count");
  }

  @Test
  public void testDatanodeOutput() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.listStorageTypeUsageInfo(any()))
        .thenReturn(getUsageInfo());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--with-datanode");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertThat(output).contains("Datanode StorageType Usage List");
    assertThat(output).contains("StorageType DISK");
    assertThat(output).contains("StorageType SSD");
    assertThat(output).contains("host");
  }

  private List<DatanodeStorageTypeUsageInfoProto> getUsageInfo() {
    return Collections.singletonList(
        DatanodeStorageTypeUsageInfoProto.newBuilder()
            .setDatanodeDetails(createDatanodeDetails().getProtoBufMessage())
            .addStorageTypeUsageInfo(createUsageInfo(StorageType.DISK, 100, 10, 90))
            .addStorageTypeUsageInfo(createUsageInfo(StorageType.SSD, 200, 20, 180))
            .build());
  }

  private StorageTypeUsageInfoProto createUsageInfo(StorageType storageType,
      long capacity, long used, long remaining) {
    return StorageTypeUsageInfoProto.newBuilder()
        .setStorageType(StorageTypeUtils.getStorageTypeProto(storageType))
        .setCapacity(capacity)
        .setUsed(used)
        .setRemaining(remaining)
        .setCommitted(0)
        .setFreeSpaceToSpare(0)
        .build();
  }

  private DatanodeDetails createDatanodeDetails() {
    return DatanodeDetails.getFromProtoBuf(HddsProtos.DatanodeDetailsProto
        .newBuilder()
        .setUuid("00000000-0000-0000-0000-000000000001")
        .setHostName("host1")
        .setIpAddress("127.0.0.1")
        .setNetworkLocation("/default")
        .build());
  }
}
