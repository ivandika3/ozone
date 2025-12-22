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

package org.apache.hadoop.ozone.om.ha;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc_.AlignmentContext;
import org.apache.hadoop.ipc_.Client.ConnectionId;
import org.apache.hadoop.ipc_.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ipc_.RpcInvocationHandler;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MsyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_FOLLOWER_PROBE_RETRY_PERIOD_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_FOLLOWER_PROBE_RETRY_PERIOD_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * that supports reading from follower OM(s) (i.e. non-leader OMs which also includes
 * OM listener).
 *
 * This constructs a wrapper proxy that sends the request to follower
 * OMs(s), if follower read is enabled. In case there are multiple
 * follower OMs, it will try them one by one in case the RPC failed. It
 * will fail back to the leader OM after it has exhausted all the
 * follower OMs.
 *
 * Read and write requests will still be sent to leader OM if reading from
 * follower is turned off.
 */
public class HadoopRpcOMFollowerReadFailoverProxyProvider<T> extends FailoverProxyProvider<T> {
  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(HadoopRpcOMFollowerReadFailoverProxyProvider.class);

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final ConfigurationSource conf;
  private final Class<T> protocolClass;
  
  private final String clientID;

  /** Client-side context for syncing with the OM server side. */
  private final AlignmentContext alignmentContext;

  /** The inner proxy provider used for leader/follower failover. */
  private final HadoopRpcOMFailoverProxyProvider<T> failoverProxy;

  private Map<String, ProxyInfo<T>> omProxies;
  private List<String> omNodeIDList;
  private Map<String, InetSocketAddress> omNodeAddressMap;
  private Map<String, OMProxyInfo> omProxyInfos;

  private String currentProxyOMNodeId;
  private int currentProxyIndex;
  private String nextProxyOMNodeId;
  private int nextProxyIndex;

  /** The policy used to determine if an exception is fatal or retriable. */
  private final RetryPolicy followerRetryPolicy;
  /** The combined proxy which redirects to other proxies as necessary. */
  private final ProxyInfo<T> combinedProxy;

  /**
   * Whether reading from follower is enabled. If this is false, all read
   * requests will still go to active OM.
   */
  private boolean followerReadEnabled;

  /**
   * This adjusts how frequently this proxy provider should auto-msync to the
   * Leader OM, automatically performing an msync() call to the active
   * to fetch the current transaction ID before submitting read requests to
   * follower nodes. See HDFS-14211 for more description of this feature.
   * If this is below 0, never auto-msync. If this is 0, perform an msync on
   * every read operation. If this is above 0, perform an msync after this many
   * ms have elapsed since the last msync.
   */
  private final long autoMsyncPeriodMs;

  /**
   * The time, in millisecond epoch, that the last msync operation was
   * performed. This includes any implicit msync (any operation which is
   * serviced by the OM Leader).
   */
  private volatile long lastMsyncTimeMs = -1;

  /**
   * A client using an ObserverReadProxyProvider should first sync with the
   * OM Leader on startup. This ensures that the client reads data which
   * is consistent with the state of the world as of the time of its
   * instantiation. This variable will be true after this initial sync has
   * been performed.
   */
  private volatile boolean msynced = false;

  /**
   * The index into the omProxyInfos map currently being used. Should only
   * be accessed in synchronized methods.
   */
  private int currentIndex = -1;

  /**
   * The proxy being used currently. Should only be accessed in synchronized
   * methods.
   */
  private OMProxyInfo currentProxy;

  /** The last proxy that has been used. Only used for testing. */
  private volatile ProxyInfo<T> lastProxy = null;

  /**
   * In case there is no Observer node, for every read call, client will try
   * to loop through all Standby nodes and fail eventually. Since there is no
   * guarantee on when Observer node will be enabled. This can be very
   * inefficient.
   * The following value specify the period on how often to retry all Standby.
   */
  private long followerProbeRetryPeriodMs;

  /**
   * Timeout in ms when we try to get the HA state of a OM.
   */
  private long omHAStateProbeTimeoutMs;

  /**
   * The previous time where zero follower were found. If there was follower,
   * or it is initialization, this is set to 0.
   */
  private long lastFollowerProbeTime;

  /**
   * Threadpool to send the getHAServiceState requests.
   */
  private final BlockingThreadPoolExecutorService omProbingThreadPool;

  private boolean performFailoverDone;

  private final UserGroupInformation ugi;


  public HadoopRpcOMFollowerReadFailoverProxyProvider(
      ConfigurationSource configuration, UserGroupInformation ugi, String omServiceId, Class<T> protocol)
      throws IOException {
    this(configuration, ugi, omServiceId, protocol,
        new HadoopRpcOMFailoverProxyProvider<>(configuration, ugi, omServiceId, protocol));
  }

  @SuppressWarnings("unchecked")
  public HadoopRpcOMFollowerReadFailoverProxyProvider(
      ConfigurationSource configuration, UserGroupInformation ugi, String omServiceId, Class<T> protocol,
      HadoopRpcOMFailoverProxyProvider<T> failoverProxy) throws IOException {
    this.conf = configuration;
    this.protocolClass = protocol;
    this.performFailoverDone = true;
    this.ugi = ugi;
    this.failoverProxy = failoverProxy;
    this.alignmentContext = new ClientAlignmentContext();
    this.lastFollowerProbeTime = 0;

    // Don't bother configuring the number of retries and such on the retry
    // policy since it is mainly only used for determining whether or not an
    // exception is retriable or fatal
    followerRetryPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, 1);

    loadOMClientConfigs(conf, omServiceId);
    Preconditions.checkNotNull(omProxies);
    Preconditions.checkNotNull(omNodeIDList);
    Preconditions.checkNotNull(omNodeAddressMap);

    nextProxyIndex = 0;
    nextProxyOMNodeId = omNodeIDList.get(nextProxyIndex);
    currentProxyIndex = 0;
    currentProxyOMNodeId = nextProxyOMNodeId;

    // Create a wrapped proxy containing all the proxies. Since this combined
    // proxy is just redirecting to other proxies, all invocations can share it.
    StringBuilder combinedInfo = new StringBuilder("[");
    for (int i = 0; i < omProxies.size(); i++) {
      if (i > 0) {
        combinedInfo.append(",");
      }
      combinedInfo.append(omProxies.get(i).proxyInfo);
    }
    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
        FollowerReadInvocationHandler.class.getClassLoader(),
        new Class<?>[] {protocol}, new FollowerReadInvocationHandler());
    combinedProxy = new ProxyInfo<>(wrappedProxy, combinedInfo.toString());

    autoMsyncPeriodMs = configuration.getTimeDuration(
        // The host of the URI is the omservice ID
        OZONE_CLIENT + "." + uri.getHost(),
        AUTO_MSYNC_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);
    followerProbeRetryPeriodMs = conf.getTimeDuration(
        OZONE_CLIENT_FAILOVER_FOLLOWER_PROBE_RETRY_PERIOD_KEY,
        OZONE_CLIENT_FAILOVER_FOLLOWER_PROBE_RETRY_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);
    omHAStateProbeTimeoutMs = conf.getTimeDuration(OM_HA_STATE_PROBE_TIMEOUT,
        OM_HA_STATE_PROBE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);



    if (wrappedProxy instanceof OzoneManagerProtocolPB) {
      this.followerReadEnabled = true;
    } else {
      LOG.info("Disabling follower reads for {} because the requested proxy "
          + "class does not implement {}", omServiceId, OzoneManagerProtocolPB.class.getName());
      this.followerReadEnabled = false;
    }

    /*
     * At most 4 threads will be running and each thread will die after 10
     * seconds of no use. Up to 132 tasks (4 active + 128 waiting) can be
     * submitted simultaneously.
     */
    omProbingThreadPool =
        BlockingThreadPoolExecutorService.newInstance(4, 128, 10L, TimeUnit.SECONDS,
            "om-ha-state-probing");
  }

  private void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
      throws IOException {
    Map<String, ProxyInfo<T>> omProxies = new HashMap<>();
    this.omProxyInfos = new HashMap<>();
    List<String> omNodeIDList = new ArrayList<>();
    Map<String, InetSocketAddress> omNodeAddressMap = new HashMap<>();

    Collection<String> omNodeIds = OmUtils.getActiveNonListenerOMNodeIds(config,
        omSvcId);

    for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omSvcId, nodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
      if (rpcAddrStr == null) {
        continue;
      }

      OMProxyInfo omProxyInfo = new OMProxyInfo(omSvcId, nodeId,
          rpcAddrStr);

      if (omProxyInfo.getAddress() != null) {
        // For a non-HA OM setup, nodeId might be null. If so, we assign it
        // the default value
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        // ProxyInfo will be set during first time call to server.
        omProxies.put(nodeId, null);
        omProxyInfos.put(nodeId, omProxyInfo);
        omNodeIDList.add(nodeId);
        omNodeAddressMap.put(nodeId, omProxyInfo.getAddress());
      } else {
        LOG.error("Failed to create OM proxy for {} at address {}",
            nodeId, rpcAddrStr);
      }
    }

    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
    setOmProxies(omProxies);
    setOmNodeIDList(omNodeIDList);
    setOmNodeAddressMap(omNodeAddressMap);
  }


  public AlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return combinedProxy;
  }

  @Override
  public void performFailover(T currentProxy) {
    failoverProxy.performFailover(currentProxy);
  }

  /**
   * Check if request args is read-only.
   *
   * @return whether the 'args' is a read-only operation.
   */
  private static boolean isRead(Object[] args) {
    // TODO: Verify whether checks in ProtobufRpcEngine.Invoker#invoker have already been done
    final Message theRequest = (Message) args[1];
    if ((theRequest instanceof OMRequest)) {
      OMRequest omRequest = (OMRequest) theRequest;
      return OmUtils.isReadOnly(omRequest);
    }
    return false;

  }

  @VisibleForTesting
  void setFollowerReadEnabled(boolean flag) {
    this.followerReadEnabled = flag;
  }

  @VisibleForTesting
  ProxyInfo<T> getLastProxy() {
    return lastProxy;
  }

  /**
   * Return the currently used proxy. If there is none, first calls
   * {@link #changeProxy(OMProxyInfo)} to initialize one.
   */
  private OMProxyInfo getCurrentProxy() {
    return changeProxy(null);
  }

  /**
   * Move to the next proxy in the proxy list. If the OMProxyInfo supplied by
   * the caller does not match the current proxy, the call is ignored; this is
   * to handle concurrent calls (to avoid changing the proxy multiple times).
   * The service state of the newly selected proxy will be updated before
   * returning.
   *
   * @param initial The expected current proxy
   * @return The new proxy that should be used.
   */
  private synchronized OMProxyInfo changeProxy(OMProxyInfo initial) {
    if (currentProxy != initial) {
      // Must have been a concurrent modification; ignore the move request
      return currentProxy;
    }
    currentIndex = (currentIndex + 1) % omProxyInfos.size();
    currentProxy = createProxyIfNeeded(omProxies.get(currentIndex));
    currentProxy.setCachedState(getHAServiceStateWithTimeout(currentProxy));
    LOG.debug("Changed current proxy from {} to {}",
        initial == null ? "none" : initial.proxyInfo,
        currentProxy.proxyInfo);
    return currentProxy;
  }

  /**
   * Execute getHAServiceState() call with a timeout, to avoid a long wait when
   * an NN becomes irresponsive to rpc requests
   * (when a thread/heap dump is being taken, e.g.).
   *
   * For each getHAServiceState() call, a task is created and submitted to a
   * threadpool for execution. We will wait for a response up to
   * namenodeHAStateProbeTimeoutSec and cancel these requests if they time out.
   *
   * The implementation is split into two functions so that we can unit test
   * the second function.
   */
  HAServiceState getHAServiceStateWithTimeout(final OMProxyInfo proxyInfo) {
    Callable<HAServiceState> getHAServiceStateTask = () -> getHAServiceState(proxyInfo);

    try {
      Future<HAServiceState> task =
          omProbingThreadPool.submit(getHAServiceStateTask);
      return getHAServiceStateWithTimeout(proxyInfo, task);
    } catch (RejectedExecutionException e) {
      LOG.warn("Run out of threads to submit the request to query HA state. "
          + "Ok to return null and we will fallback to use active NN to serve "
          + "this request.");
      return null;
    }
  }

  HAServiceState getHAServiceStateWithTimeout(final OMProxyInfo proxyInfo,
      Future<HAServiceState> task) {
    HAServiceState state = null;
    try {
      if (omHAStateProbeTimeoutMs > 0) {
        state = task.get(omHAStateProbeTimeoutMs, TimeUnit.MILLISECONDS);
      } else {
        // Disable timeout by waiting indefinitely when omHAStateProbeTimeoutSec is set to 0
        // or a negative value.
        state = task.get();
      }
      LOG.debug("HA State for {} is {}", proxyInfo.proxyInfo, state);
    } catch (TimeoutException e) {
      // Cancel the task on timeout
      String msg = String.format("Cancel OM probe task due to timeout for %s", proxyInfo.proxyInfo);
      LOG.warn(msg, e);
      if (task != null) {
        task.cancel(true);
      }
    } catch (InterruptedException | ExecutionException e) {
      String msg = String.format("Exception in OM probe task for %s", proxyInfo.proxyInfo);
      LOG.warn(msg, e);
    }

    return state;
  }

  /**
   * Fetch the service state from a proxy. If it is unable to be fetched,
   * assume it is in standby state, but log the exception.
   */
  private HAServiceState getHAServiceState(OMProxyInfo<T> proxyInfo) {
    IOException ioe;
    try {
      return getProxyAsClientProtocol(proxyInfo.proxy).getHAServiceState();
    } catch (RemoteException re) {
      // Though a Standby will allow a getHAServiceState call, it won't allow
      // delegation token lookup, so if DT is used it throws StandbyException
      if (re.unwrapRemoteException() instanceof NotLeaderException) {
        LOG.debug("OM {} threw NotLeaderException when fetching HAState",
            proxyInfo.getAddress());
        return HAServiceState.STANDBY;
      }
      ioe = re;
    } catch (IOException e) {
      ioe = e;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Failed to connect to {} while fetching HAServiceState",
          proxyInfo.getAddress(), ioe);
    }
    return null;
  }

  /**
   * Return the input proxy, cast as a {@link OzoneManagerProtocolPB}. This catches any
   * {@link ClassCastException} and wraps it in a more helpful message. This
   * should ONLY be called if the caller is certain that the proxy is, in fact,
   * a {@link OzoneManagerProtocolPB}.
   */
  private OzoneManagerProtocolPB getProxyAsClientProtocol(T proxy) {
    assert proxy instanceof OzoneManagerProtocolPB : "BUG: Attempted to use proxy "
        + "of class " + proxy.getClass() + " as if it was a OzoneManagerProtocolPB.";
    return (OzoneManagerProtocolPB) proxy;
  }

  /**
   * This will call {@link #msync()} on the OM leader
   * (via the {@link #failoverProxy}) to initialize the state of this client.
   * Calling it multiple times is a no-op; only the first will perform an
   * msync.
   *
   * @see #msynced
   */
  private synchronized void initializeMsync() throws IOException {
    if (msynced) {
      return; // No need for an msync
    }
    msync();
    msynced = true;
    lastMsyncTimeMs = Time.monotonicNow();
  }

  /**
   * Call msync to the current OM.
   * @throws IOException
   */
  private void msync() throws IOException {
    try {
      OzoneManagerProtocolPB clientProtocol = getProxyAsClientProtocol(failoverProxy.getProxy().proxy);

      MsyncRequest req = MsyncRequest.newBuilder().build();

      OMRequest omRequest = createOMRequest(Type.Msync)
          .setMsyncRequest(req)
          .build();

      clientProtocol.submitRequest(NULL_RPC_CONTROLLER, omRequest);
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          getNotLeaderException(e);
      if (notLeaderException == null) {
        throw ProtobufHelper.getRemoteException(e);
      }
      throw new IOException("Could not determine or connect to OM leader during msync.");
    }
  }

  /**
   * Returns a OMRequest builder with specified type.
   * @param cmdType type of the request
   */
  private OMRequest.Builder createOMRequest(Type cmdType) {
    return OMRequest.newBuilder()
        .setCmdType(cmdType)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(clientID);
  }
  

  /**
   * Check if client need to find an Observer proxy.
   * If current proxy is Active then we should stick to it and postpone probing
   * for Observers for a period of time. When this time expires the client will
   * try to find an Observer again.
   * *
   * @return true if we did not reach the threshold
   * to start looking for Observer, or false otherwise.
   */
  private boolean shouldFindFollower() {
    // lastFollowerProbeTime > 0 means we tried, but did not find any
    // Observers yet
    // If lastFollowerProbeTime <= 0, previous check found follower, so
    // we should not skip follower read.
    if (lastFollowerProbeTime > 0) {
      return Time.monotonicNow() - lastFollowerProbeTime
          >= followerProbeRetryPeriodMs;
    }
    return true;
  }

  /**
   * This will call {@link #msync()} on the OM leader
   * (via the {@link #failoverProxy}) to update the state of this client, only
   * if at least {@link #autoMsyncPeriodMs} ms has elapsed since the last time
   * an msync was performed.
   *
   * @see #autoMsyncPeriodMs
   */
  private void autoMsyncIfNecessary() throws IOException {
    if (autoMsyncPeriodMs == 0) {
      // Always msync
      msync();
    } else if (autoMsyncPeriodMs > 0) {
      if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
        synchronized (this) {
          // Use a synchronized block so that only one thread will msync
          // if many operations are submitted around the same time.
          // Re-check the entry criterion since the status may have changed
          // while waiting for the lock.
          if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
            msync();
            lastMsyncTimeMs = Time.monotonicNow();
          }
        }
      }
    }
  }

  /**
   * An InvocationHandler to handle incoming requests. This class's invoke
   * method contains the primary logic for redirecting to followers.
   *
   * If follower reads are enabled, attempt to send read operations to the
   * current proxy. If it is not an follower, or the follower fails, adjust
   * the current proxy and retry on the next one. If all proxies are tried
   * without success, the request is forwarded to the leader.
   *
   * Write requests are always forwarded to the leader.
   */
  private class FollowerReadInvocationHandler implements RpcInvocationHandler {

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      Object retVal;

      if (followerReadEnabled && shouldFindFollower() && isRead(args)) {
        if (!msynced) {
          // An msync() must first be performed to ensure that this client is
          // up-to-date with the active's state. This will only be done once.
          initializeMsync();
        } else {
          autoMsyncIfNecessary();
        }

        int failedObserverCount = 0;
        int activeCount = 0;
        int standbyCount = 0;
        int unreachableCount = 0;
        for (int i = 0; i < nameNodeProxies.size(); i++) {
          OMProxyInfo current = getCurrentProxy();
          OMRoleInfo currState = current.getCachedState();
          if (currState != HAServiceState.OBSERVER) {
            if (currState == HAServiceState.ACTIVE) {
              activeCount++;
            } else if (currState == HAServiceState.STANDBY) {
              standbyCount++;
            } else if (currState == null) {
              unreachableCount++;
            }
            LOG.debug("Skipping proxy {} for {} because it is in state {}",
                current.proxyInfo, method.getName(),
                currState == null ? "unreachable" : currState);
            changeProxy(current);
            continue;
          }
          LOG.debug("Attempting to service {} using proxy {}",
              method.getName(), current.proxyInfo);
          try {
            retVal = method.invoke(current.proxy, args);
            lastProxy = current;
            LOG.debug("Invocation of {} using {} was successful",
                method.getName(), current.proxyInfo);
            return retVal;
          } catch (InvocationTargetException ite) {
            if (!(ite.getCause() instanceof Exception)) {
              throw ite.getCause();
            }
            Exception e = (Exception) ite.getCause();
            if (e instanceof InterruptedIOException ||
                e instanceof InterruptedException) {
              // If interrupted, do not retry.
              LOG.warn("Invocation returned interrupted exception on [{}];",
                  current.proxyInfo, e);
              throw e;
            }
            if (e instanceof RemoteException) {
              RemoteException re = (RemoteException) e;
              Exception unwrapped = re.unwrapRemoteException(
                  ObserverRetryOnActiveException.class);
              if (unwrapped instanceof ObserverRetryOnActiveException) {
                LOG.debug("Encountered ObserverRetryOnActiveException from {}." +
                    " Retry active namenode directly.", current.proxyInfo);
                break;
              }
            }
            RetryAction retryInfo = followerRetryPolicy.shouldRetry(e, 0, 0,
                method.isAnnotationPresent(Idempotent.class)
                    || method.isAnnotationPresent(AtMostOnce.class));
            if (retryInfo.action == RetryAction.RetryDecision.FAIL) {
              throw e;
            } else {
              failedObserverCount++;
              LOG.warn(
                  "Invocation returned exception on [{}]; {} failure(s) so far",
                  current.proxyInfo, failedObserverCount, e);
              changeProxy(current);
            }
          }
        }

        // Only log message if there are actual follower failures.
        // Getting here with failedObserverCount = 0 could
        // be that there is simply no Observer node running at all.
        if (failedObserverCount > 0) {
          // If we get here, it means all followers have failed.
          LOG.warn("{} followers have failed for read request {}; "
                  + "also found {} standby, {} active, and {} unreachable. "
                  + "Falling back to active.", failedObserverCount,
              method.getName(), standbyCount, activeCount, unreachableCount);
          lastFollowerProbeTime = 0;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Read falling back to active without follower read "
                + "fail, is there no follower node running?");
          }
          lastFollowerProbeTime = Time.monotonicNow();
        }
      }

      // Either all followers have failed, follower reads are disabled,
      // or this is a write request. In any case, forward the request to
      // the active NameNode.
      LOG.debug("Using failoverProxy to service {}", method.getName());
      ProxyInfo<T> activeProxy = failoverProxy.getProxy();
      try {
        retVal = method.invoke(activeProxy.proxy, args);
      } catch (InvocationTargetException e) {
        // This exception will be handled by higher layers
        throw e.getCause();
      }
      // If this was reached, the request reached the active, so the
      // state is up-to-date with active and no further msync is needed.
      msynced = true;
      lastMsyncTimeMs = Time.monotonicNow();
      lastProxy = activeProxy;
      return retVal;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(followerReadEnabled
          ? getCurrentProxy().proxy : failoverProxy.getProxy().proxy);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> pi : omProxies.values()) {
      if (pi.proxy != null) {
        if (pi.proxy instanceof Closeable) {
          ((Closeable)pi.proxy).close();
        } else {
          RPC.stopProxy(pi.proxy);
        }
        // Set to null to avoid the failoverProxy having to re-do the close
        // if it is sharing a proxy instance
        pi.proxy = null;
      }
    }
    failoverProxy.close();
    omProbingThreadPool.shutdown();
  }

  protected synchronized void setOmProxies(Map<String,
      ProxyInfo<T>> omProxies) {
    this.omProxies = omProxies;
  }

  protected synchronized void setOmNodeIDList(List<String> omNodeIDList) {
    Collections.shuffle(omNodeIDList);
    this.omNodeIDList = Collections.unmodifiableList(omNodeIDList);
  }

  protected synchronized List<String> getOmNodeIDList() {
    return omNodeIDList;
  }

  protected synchronized void setOmNodeAddressMap(
      Map<String, InetSocketAddress> map) {
    this.omNodeAddressMap = map;
  }
}
