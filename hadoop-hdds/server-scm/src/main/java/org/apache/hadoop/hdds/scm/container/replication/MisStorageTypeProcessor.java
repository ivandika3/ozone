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
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Processes containers queued for misplaced storage type remediation.
 */
public class MisStorageTypeProcessor extends
    UnhealthyReplicationProcessor<ContainerHealthResult.MisStorageTypeHealthResult> {

  MisStorageTypeProcessor(ReplicationManager replicationManager,
      Supplier<Duration> interval) {
    super(replicationManager, interval);
  }

  @Override
  protected ContainerHealthResult.MisStorageTypeHealthResult
      dequeueHealthResultFromQueue(ReplicationQueue queue) {
    return queue.dequeueMisStorageTypeContainer();
  }

  @Override
  protected void requeueHealthResult(ReplicationQueue queue,
      ContainerHealthResult.MisStorageTypeHealthResult healthResult) {
    queue.enqueue(healthResult);
  }

  @Override
  protected boolean inflightOperationLimitReached(ReplicationManager rm,
      long inflightLimit) {
    return rm.getInflightReplicationCount() >= inflightLimit;
  }

  @Override
  protected int sendDatanodeCommands(ReplicationManager rm,
      ContainerHealthResult.MisStorageTypeHealthResult healthResult)
      throws IOException {
    return rm.processMisStorageTypeContainer(healthResult);
  }
}
