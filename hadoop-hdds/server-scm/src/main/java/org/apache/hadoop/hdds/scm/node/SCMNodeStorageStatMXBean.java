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

package org.apache.hadoop.hdds.scm.node;

import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;

/**
 *
 * This is the JMX management interface for node manager information.
 */
@InterfaceAudience.Private
public interface SCMNodeStorageStatMXBean {
  /**
   * Get the capacity of the dataNode.
   * @param datanodeID Datanode Id
   * @return long
   */
  long getCapacity(UUID datanodeID);

  /**
   * Returns the remaining space of a Datanode.
   * @param datanodeId Datanode Id
   * @return long
   */
  long getRemainingSpace(UUID datanodeId);


  /**
   * Returns used space in bytes of a Datanode.
   * @return long
   */
  long getUsedSpace(UUID datanodeId);

  /**
   * Returns the total capacity of all dataNodes.
   * @return long
   */
  long getTotalCapacity();

  /**
   * Returns the total Used Space in all Datanodes.
   * @return long
   */
  long getTotalSpaceUsed();

  /**
   * Returns the total Remaining Space in all Datanodes.
   * @return long
   */
  long getTotalFreeSpace();

  /**
   * Returns the set of disks for a given Datanode.
   * @return set of storage volumes
   */
  Set<StorageLocationReport> getStorageVolumes(UUID datanodeId);
}
