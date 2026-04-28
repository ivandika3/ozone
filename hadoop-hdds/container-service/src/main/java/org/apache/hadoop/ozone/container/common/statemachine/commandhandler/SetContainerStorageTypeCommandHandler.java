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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetContainerStorageTypeCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set-container-storage-type commands from SCM.
 */
public class SetContainerStorageTypeCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SetContainerStorageTypeCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicInteger timeoutCount = new AtomicInteger(0);
  private final ExecutorService executor;
  private final AtomicLong totalTime = new AtomicLong(0);
  private final int maxQueueSize;
  private final Clock clock;

  public SetContainerStorageTypeCommandHandler(int threadPoolSize,
      int queueSize, String threadNamePrefix) {
    this(Clock.systemDefaultZone(),
        new ThreadPoolExecutor(threadPoolSize, threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new ThreadFactoryBuilder()
                .setNameFormat(
                    threadNamePrefix + "SetContainerStorageTypeThread-%d")
                .build()),
        queueSize);
  }

  @VisibleForTesting
  SetContainerStorageTypeCommandHandler(Clock clock,
      ExecutorService executor, int queueSize) {
    this.clock = clock;
    this.executor = executor;
    this.maxQueueSize = queueSize;
  }

  @Override
  public void handle(SCMCommand<?> command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    try {
      executor.execute(() -> handleInternal(command, ozoneContainer, context));
    } catch (RejectedExecutionException ex) {
      LOG.warn("SetContainerStorageType command ignored because command "
          + "queue reached max size {}.", maxQueueSize);
    }
  }

  private void handleInternal(SCMCommand<?> command,
      OzoneContainer ozoneContainer, StateContext context) {
    long startTime = Time.monotonicNow();
    invocationCount.incrementAndGet();
    try {
      if (command.hasExpired(clock.millis())) {
        LOG.info("Not processing SetContainerStorageType command because "
                + "current time {}ms is after deadline {}ms",
            clock.millis(), command.getDeadline());
        timeoutCount.incrementAndGet();
        return;
      }

      if (context != null && context.getTermOfLeaderSCM().isPresent()
          && command.getTerm() < context.getTermOfLeaderSCM().getAsLong()) {
        LOG.info("Ignoring SetContainerStorageType command because SCM "
                + "leader has newer term ({} < {})",
            command.getTerm(), context.getTermOfLeaderSCM().getAsLong());
        return;
      }

      SetContainerStorageTypeCommand setCommand =
          (SetContainerStorageTypeCommand) command;
      ContainerController controller = ozoneContainer.getController();
      controller.setContainerStorageType(setCommand.getContainerID(),
          setCommand.getReplicaIndex(), setCommand.getStorageType());
    } catch (Exception e) {
      LOG.error("Failed to set storage type for container.", e);
    } finally {
      totalTime.addAndGet(Time.monotonicNow() - startTime);
    }
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.setContainerStorageTypeCommand;
  }

  @Override
  public int getInvocationCount() {
    return invocationCount.get();
  }

  public int getTimeoutCount() {
    return timeoutCount.get();
  }

  @Override
  public long getAverageRunTime() {
    int invocations = invocationCount.get();
    return invocations == 0 ? 0 : totalTime.get() / invocations;
  }

  @Override
  public long getTotalRunTime() {
    return totalTime.get();
  }

  @Override
  public int getQueuedCount() {
    if (executor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor) executor).getQueue().size();
    }
    return 0;
  }

  @Override
  public void stop() {
    try {
      executor.shutdown();
      if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
