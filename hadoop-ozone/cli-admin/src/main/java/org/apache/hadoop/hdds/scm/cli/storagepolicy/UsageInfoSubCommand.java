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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeStorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListStorageTypeUsageInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;

/**
 * Command to list usage information grouped by storage type.
 */
@CommandLine.Command(
    name = "usageinfo",
    description = "List usage information in the storage policy dimension",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UsageInfoSubCommand extends ScmSubcommand {

  @CommandLine.Option(
      names = {"-wd", "--with-datanode"},
      description = "Print detailed datanode storage type usage information",
      defaultValue = "false")
  private boolean printDatanodeInfo;

  @CommandLine.Option(names = {"-os", "--operational-state"},
      description = "Show datanodes in a specific operational state, or ALL",
      defaultValue = "IN_SERVICE")
  private String nodeOperationalStateStr;

  @CommandLine.Option(names = {"-ns", "--node-state"},
      description = "Show datanodes in a specific node state, or ALL",
      defaultValue = "HEALTHY")
  private String nodeStateStr;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ListStorageTypeUsageInfoRequestProto.Builder requestBuilder =
        ListStorageTypeUsageInfoRequestProto.newBuilder();
    if (!"ALL".equalsIgnoreCase(nodeOperationalStateStr)) {
      requestBuilder.setOpState(HddsProtos.NodeOperationalState.valueOf(
          nodeOperationalStateStr.toUpperCase()));
    }
    if (!"ALL".equalsIgnoreCase(nodeStateStr)) {
      requestBuilder.setState(HddsProtos.NodeState.valueOf(
          nodeStateStr.toUpperCase()));
    }
    List<DatanodeStorageTypeUsageInfoProto> usageInfos =
        scmClient.listStorageTypeUsageInfo(requestBuilder.build());
    printStorageTypeSummaryInfo(usageInfos);
    if (printDatanodeInfo) {
      printStorageTypeDatanodeInfo(usageInfos);
    }
  }

  private void printStorageTypeSummaryInfo(
      List<DatanodeStorageTypeUsageInfoProto> usageInfos) {
    Map<StorageType, UsageTotals> totalsByType =
        new EnumMap<>(StorageType.class);
    System.out.println("Cluster StorageType Usage Summary");
    for (DatanodeStorageTypeUsageInfoProto datanodeUsageInfo : usageInfos) {
      for (StorageTypeUsageInfoProto usageInfo :
          datanodeUsageInfo.getStorageTypeUsageInfoList()) {
        StorageType storageType =
            StorageTypeUtils.getFromProtobuf(usageInfo.getStorageType());
        totalsByType.computeIfAbsent(storageType, ignored -> new UsageTotals())
            .add(usageInfo);
      }
    }
    for (Map.Entry<StorageType, UsageTotals> entry : totalsByType.entrySet()) {
      printUsage(entry.getKey().name() + " ", entry.getValue(), false);
    }
  }

  private void printStorageTypeDatanodeInfo(
      List<DatanodeStorageTypeUsageInfoProto> usageInfos) {
    Map<StorageType, List<DatanodeStorageTypeUsageInfoProto>> byType =
        new EnumMap<>(StorageType.class);
    for (DatanodeStorageTypeUsageInfoProto usageInfo : usageInfos) {
      for (StorageTypeUsageInfoProto storageTypeUsage :
          usageInfo.getStorageTypeUsageInfoList()) {
        StorageType storageType =
            StorageTypeUtils.getFromProtobuf(storageTypeUsage.getStorageType());
        byType.computeIfAbsent(storageType, ignored -> new ArrayList<>())
            .add(DatanodeStorageTypeUsageInfoProto.newBuilder()
                .setDatanodeDetails(usageInfo.getDatanodeDetails())
                .addStorageTypeUsageInfo(storageTypeUsage)
                .build());
      }
    }

    System.out.println();
    System.out.println("Datanode StorageType Usage List");
    for (Map.Entry<StorageType, List<DatanodeStorageTypeUsageInfoProto>> entry :
        byType.entrySet()) {
      System.out.printf("%nStorageType %s:%n%n", entry.getKey());
      for (DatanodeStorageTypeUsageInfoProto usageInfo : entry.getValue()) {
        StorageTypeUsageInfoProto stats = usageInfo.getStorageTypeUsageInfo(0);
        System.out.printf("  %-23s: %s (%s, %s, %s)%n", "Datanode",
            usageInfo.getDatanodeDetails().getUuid(),
            usageInfo.getDatanodeDetails().getHostName(),
            usageInfo.getDatanodeDetails().getIpAddress(),
            usageInfo.getDatanodeDetails().getNetworkLocation());
        printUsage(entry.getKey().name() + " ",
            new UsageTotals(stats), true);
        System.out.println();
      }
    }
  }

  private void printUsage(String prefix, UsageTotals usage, boolean skipCount) {
    if (!skipCount) {
      System.out.printf("  %-23s: %s%n", prefix + "Datanode Count",
          usage.datanodeCount);
    }
    System.out.printf("  %-23s: %s (%s)%n", prefix + "Capacity",
        usage.capacity + " B", StringUtils.byteDesc(usage.capacity));
    System.out.printf("  %-23s: %s (%s)%n", prefix + "Ozone Used",
        usage.used + " B", StringUtils.byteDesc(usage.used));
    System.out.printf("  %-23s: %s (%s)%n", prefix + "Remaining",
        usage.remaining + " B", StringUtils.byteDesc(usage.remaining));
    System.out.printf("  %-23s: %s (%s)%n", prefix + "Committed",
        usage.committed + " B", StringUtils.byteDesc(usage.committed));
    System.out.printf("  %-23s: %s (%s)%n", prefix + "FreeSpaceToSpare",
        usage.freeSpaceToSpare + " B",
        StringUtils.byteDesc(usage.freeSpaceToSpare));
    if (!skipCount) {
      System.out.println();
    }
  }

  private static final class UsageTotals {
    private long capacity;
    private long used;
    private long remaining;
    private long committed;
    private long freeSpaceToSpare;
    private int datanodeCount;

    private UsageTotals() {
    }

    private UsageTotals(StorageTypeUsageInfoProto usageInfo) {
      add(usageInfo);
      datanodeCount = 0;
    }

    private void add(StorageTypeUsageInfoProto usageInfo) {
      capacity += usageInfo.getCapacity();
      used += usageInfo.getUsed();
      remaining += usageInfo.getRemaining();
      committed += usageInfo.getCommitted();
      freeSpaceToSpare += usageInfo.getFreeSpaceToSpare();
      datanodeCount++;
    }
  }
}
