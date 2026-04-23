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

package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Set or unset the storage tier for one or more containers.
 */
@Command(
    name = "setstoragetier",
    aliases = {"set-storage-tier"},
    description = "Set container storage tier",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SetContainerStorageTierSubcommand extends ScmSubcommand {

  private static final String NULL_STORAGE_TIER = "null";

  @Mixin
  private ContainerIDParameters containerList;

  @Option(names = {"--storage-tier", "--storageTier", "-st"},
      required = true,
      description = "Storage tier to set (SSD, DISK, ARCHIVE, or null to unset)")
  private String storageTierStr;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<Long> containerIds = containerList.getValidatedIDs();
    StorageTier storageTier = parseStorageTier(storageTierStr);
    boolean unsetStorageTier = storageTier == null;

    scmClient.setContainerStorageTier(containerIds, storageTier,
        unsetStorageTier);
    if (unsetStorageTier) {
      System.out.printf("Successfully unset storage tier for containers: %s%n",
          containerIds);
    } else {
      System.out.printf("Successfully set storage tier to %s for containers: %s%n",
          storageTier.getTierName(), containerIds);
    }
  }

  private StorageTier parseStorageTier(String tierStr) {
    if (NULL_STORAGE_TIER.equalsIgnoreCase(tierStr.trim())) {
      return null;
    }
    try {
      StorageTier tier = StorageTier.valueOf(tierStr.trim().toUpperCase());
      if (tier == StorageTier.EMPTY) {
        throw new IllegalArgumentException("EMPTY storage tier is not allowed");
      }
      return tier;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid storage tier: " + tierStr
          + ". Allowed values are: SSD, DISK, ARCHIVE, or null.");
    }
  }
}
