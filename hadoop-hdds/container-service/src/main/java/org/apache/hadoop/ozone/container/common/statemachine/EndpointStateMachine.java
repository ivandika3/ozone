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

package org.apache.hadoop.ozone.container.common.statemachine;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getLogWarnInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmHeartbeatInterval;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.time.ZonedDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import net.jcip.annotations.GuardedBy;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FullContainerReportLeaseProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the RPC connection and state for a single SCM or Recon endpoint.
 *
 * <p>Datanode submits version, registration and heartbeat tasks to a single-thread executor.
 * Each endpoint owns an executor so blocked RPCs to one SCM do not prevent communication with other endpoints.
 * Closing the endpoint shuts down its executor and RPC client; an in-flight task may still finish.
 *
 * <p>Locks:
 * <ul>
 * <li>{@link #lock}: serializes endpoint RPCs and protects the cached FCR lease. Hold it across lease checks and
 * updates, including check-and-consume sequences.</li>
 * <li>This object's monitor ({@code this}): protects {@link #currentTask} and {@link #closed} during task submission
 * and shutdown. Do not acquire {@link #lock} or perform RPCs while holding this monitor.</li>
 * </ul>
 *
 * <p>When both locks are needed, acquire {@link #lock} first. Separate locks allow submission checks and executor
 * shutdown to proceed while an RPC is blocked.
 *
 * <p>{@link #state} and {@link #version} are volatile for readers outside the endpoint lock. The missed-heartbeat count
 * is atomic for monitoring reads without the endpoint lock.
 */
public class EndpointStateMachine
    implements Closeable, EndpointStateMachineMBean {
  private static final String SCM_TYPE = "SCM";
  private static final String RECON_TYPE = "Recon";
  static final Logger
      LOG = LoggerFactory.getLogger(EndpointStateMachine.class);

  private final HostAndPort hostAndPort;
  private final StorageContainerDatanodeProtocolClientSideTranslatorPB endpoint;
  private final ConfigurationSource conf;
  private boolean isPassive;

  // RunningDatanodeState reads this without the endpoint lock and must see late SHUTDOWN transitions,
  // even after its wait for the endpoint task has timed out.
  private volatile EndPointStates state = EndPointStates.FIRST;
  private volatile VersionResponse version;
  private final AtomicLong missedCount;
  private ZonedDateTime lastSuccessfulHeartbeat;

  /**
   * Serializes endpoint RPCs and protects the cached FCR lease. If both this lock and this object's monitor are needed,
   * acquire this lock first; never acquire it while holding the monitor.
   */
  private final Lock lock;
  @GuardedBy("lock")
  private FullContainerReportLeaseProto fullContainerReportLease;

  private final ExecutorService executorService;
  @GuardedBy("this")
  private Future<EndPointStates> currentTask;
  @GuardedBy("this")
  private boolean closed;

  /**
   * Constructs the RPC endpoint.
   *
   * @param endpoint - RPC endpoint.
   */
  public EndpointStateMachine(HostAndPort hostAndPort,
      StorageContainerDatanodeProtocolClientSideTranslatorPB endpoint,
      ConfigurationSource conf, String threadNamePrefix) {
    this.endpoint = endpoint;
    this.missedCount = new AtomicLong(0);
    this.hostAndPort = hostAndPort;
    lock = new ReentrantLock();
    this.conf = conf;
    executorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix + "EndpointStateMachineTaskThread-" + hostAndPort + "-%d ")
            .build());
  }

  /**
   * Takes a lock on this endpoint so that other threads don't use this while we
   * are trying to communicate via this endpoint.
   */
  public void lock() {
    lock.lock();
  }

  /**
   * Unlocks this endpoint.
   */
  public void unlock() {
    lock.unlock();
  }

  /**
   * Returns the version that we read from the server if anyone asks .
   *
   * @return - Version Response.
   */
  public VersionResponse getVersion() {
    return version;
  }

  /**
   * Sets the Version response we received from the SCM.
   *
   * @param version VersionResponse
   */
  public void setVersion(VersionResponse version) {
    this.version = version;
  }

  /**
   * Checks whether this SCM endpoint advertises full container report (FCR) lease support.
   *
   * <p>Returns false before the version response is available and for passive endpoints such as Recon. This capability
   * check does not indicate whether the feature is enabled by upgrade finalization or whether a lease is available.
   *
   * @return true if this non-passive endpoint advertises FCR lease support
   */
  public boolean supportsFullContainerReportLease() {
    VersionResponse currentVersion = version;
    return !isPassive && currentVersion != null
        && currentVersion.getBooleanValue(OzoneConsts.SCM_FCR_LEASE_SUPPORTED);
  }

  /**
   * Checks whether a lease granted by this SCM is cached locally. Presence does not guarantee that the lease is still
   * valid; SCM validates its ID, term and expiry when receiving the FCR.
   *
   * <p>Callers must hold the endpoint lock acquired by {@link #lock()}.
   *
   * @return true if an FCR lease is cached for this endpoint
   */
  public boolean hasFullContainerReportLease() {
    return fullContainerReportLease != null;
  }

  /**
   * Stores an FCR lease granted by this SCM, replacing any cached lease. This method does not validate the grant.
   *
   * <p>Callers must hold the endpoint lock acquired by {@link #lock()}.
   *
   * @param lease the granted FCR lease, or null to clear the cached lease
   */
  public void setFullContainerReportLease(FullContainerReportLeaseProto lease) {
    fullContainerReportLease = lease;
  }

  /**
   * Returns and clears the cached FCR lease so it can be attached to one report. Consuming the local copy does not
   * release the outstanding lease on SCM.
   *
   * <p>Callers must hold the endpoint lock acquired by {@link #lock()}.
   *
   * @return the cached FCR lease, or null if none is cached
   */
  public FullContainerReportLeaseProto takeFullContainerReportLease() {
    FullContainerReportLeaseProto lease = fullContainerReportLease;
    fullContainerReportLease = null;
    return lease;
  }

  /**
   * Clears the cached FCR lease, for example before re-registration. This does not notify SCM or release its lease.
   *
   * <p>Callers must hold the endpoint lock acquired by {@link #lock()}.
   */
  public void clearFullContainerReportLease() {
    fullContainerReportLease = null;
  }

  /**
   * Clears the cached FCR lease if its term is lower than the supplied term. Equal or older terms leave it unchanged;
   * this check does not validate lease expiry.
   *
   * <p>Callers must hold the endpoint lock acquired by {@link #lock()}.
   *
   * @param term the leader term reported by this endpoint
   */
  public void clearFullContainerReportLeaseIfStale(long term) {
    if (hasFullContainerReportLease() && term > fullContainerReportLease.getTerm()) {
      clearFullContainerReportLease();
    }
  }

  /**
   * Returns the current {@link EndPointStates} this endpoint is in.
   *
   * @return - getState.
   */
  @Override
  public EndPointStates getState() {
    return state;
  }

  @Override
  public int getVersionNumber() {
    if (version != null) {
      return version.getProtobufMessage().getSoftwareVersion();
    } else {
      return -1;
    }
  }

  /**
   * Sets the endpoint state.
   *
   * @param epState - endpoint state.
   */
  public EndPointStates setState(EndPointStates epState) {
    this.state = epState;
    return this.state;
  }

  /**
   * Submit an endpoint task.
   *
   * <p>The task factory is invoked and its result submitted while holding this object's monitor, after checking that
   * the previous task has completed. The factory must not acquire the endpoint lock or perform RPCs. The submitted
   * task runs asynchronously without holding this monitor and may acquire the endpoint lock and perform RPCs.
   * <p>Only one endpoint task can be run or queued at one point of time. If this method is called
   *    when there is a running or queued task or when this state machine has been shutdown, it gets ignored.
   *
   * @param taskFactory Task factory.
   * @return null if there is a running or pending task, return the newly submitted task otherwise.
   */
  public synchronized Future<EndPointStates> submitTask(Supplier<Callable<EndPointStates>> taskFactory) {
    if (closed || executorService.isShutdown() || currentTask != null && !currentTask.isDone()) {
      return null;
    }
    Callable<EndPointStates> task = taskFactory.get();
    if (task == null) {
      return CompletableFuture.completedFuture(state);
    }
    currentTask = executorService.submit(task);
    return currentTask;
  }

  /**
   * Closes the connection.
   */
  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      executorService.shutdown();
    }
    if (endpoint != null) {
      endpoint.close();
    }
  }

  /**
   * Increments the count of consecutive communication failures. Atomic updates allow monitoring reads without requiring
   * the endpoint lock.
   */
  public void incMissed() {
    this.missedCount.incrementAndGet();
  }

  /**
   * Returns the value of the missed count.
   *
   * @return int
   */
  @Override
  public long getMissedCount() {
    return this.missedCount.get();
  }

  @Override
  public String getAddressString() {
    return hostAndPort.getAddress().toString();
  }

  public void zeroMissedCount() {
    this.missedCount.set(0);
  }

  /**
   * Returns the InetAddress of the endpoint.
   *
   * @return - EndPoint.
   */
  public HostAndPort getAddress() {
    return hostAndPort;
  }

  /**
   * Returns real RPC endpoint.
   *
   * @return rpc client.
   */
  public StorageContainerDatanodeProtocolClientSideTranslatorPB
      getEndpoint() {
    return endpoint;
  }

  /**
   * Returns the string that represents this endpoint.
   *
   * @return - String
   */
  @Override
  public String toString() {
    return hostAndPort.toString();
  }

  /**
   * Logs exception if needed.
   *  @param ex         - Exception
   */
  public void logIfNeeded(Exception ex) {

    double missCounter =
        this.getMissedCount() % getLogWarnInterval(conf);
    String serverName = "SCM";

    if (isPassive) {
      // Recon connection failures can be logged 10 times lower than regular SCM.
      missCounter = this.getMissedCount() % (10L * getLogWarnInterval(conf));
      serverName = "Recon";
    }

    if (missCounter == 0) {
      long missedDurationSeconds = TimeUnit.MILLISECONDS.toSeconds(
              this.getMissedCount() * getScmHeartbeatInterval(this.conf)
      );
      LOG.warn("Unable to communicate to {} server at {} past {} seconds.",
              serverName, hostAndPort, missedDurationSeconds, ex);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Incrementing the Missed count.", ex);
    }
    this.incMissed();
  }

  /**
   * Returns true if the endpoint is not an SCM. A passive endpoint can be a
   * Server that only reads information from Datanode, like Recon.
   * @return true if the endpoint is a passive endpoint.
   */
  public boolean isPassive() {
    return isPassive;
  }

  public void setPassive(boolean passive) {
    isPassive = passive;
  }

  /**
   * States that an endpoint can be in.
   * <p>
   * This is a sorted list of states that endpoint will traverse.
   * <p>
   * {@link #getNextState()} will move from {@link #FIRST} to {@link #LAST}.
   */
  public enum EndPointStates {
    GETVERSION,
    REGISTER,
    HEARTBEAT,
    SHUTDOWN;

    private static final EndPointStates FIRST = values()[0];
    private static final EndPointStates LAST = values()[values().length - 1];

    /**
     * Returns the next logical state that endpoint should move to.
     * The next state is computed by adding 1 to the current state.
     *
     * @return next state.
     */
    public EndPointStates getNextState() {
      final int n = this.ordinal();
      return n >= LAST.ordinal() ? LAST : values()[n + 1];
    }
  }

  @Override
  public long getLastSuccessfulHeartbeat() {
    return lastSuccessfulHeartbeat == null ?
        0 :
        lastSuccessfulHeartbeat.toEpochSecond();
  }

  public void setLastSuccessfulHeartbeat(
      ZonedDateTime lastSuccessfulHeartbeat) {
    this.lastSuccessfulHeartbeat = lastSuccessfulHeartbeat;
  }

  @Override
  public String getType() {
    if (endpoint.getUnderlyingProxyObject() instanceof ReconDatanodeProtocolPB) {
      return RECON_TYPE;
    }
    return SCM_TYPE;
  }
}
