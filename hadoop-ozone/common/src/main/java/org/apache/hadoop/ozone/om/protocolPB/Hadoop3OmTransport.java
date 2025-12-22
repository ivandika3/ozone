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

package org.apache.hadoop.ozone.om.protocolPB;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMClientAlignmentContext;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.om.ha.OMFollowerReadProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full-featured Hadoop RPC implementation with failover support.
 * Supports optional follower reads when configured.
 */
public class Hadoop3OmTransport implements OmTransport {

  private static final Logger LOG =
      LoggerFactory.getLogger(Hadoop3OmTransport.class);

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OMFailoverProxyProviderBase<OzoneManagerProtocolPB>
      omFailoverProxyProvider;

  private final OzoneManagerProtocolPB rpcProxy;

  /** Whether follower reads are enabled. */
  private final boolean followerReadEnabled;

  /** AlignmentContext for follower reads (null if not enabled). */
  private final OMClientAlignmentContext alignmentContext;

  public Hadoop3OmTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.followerReadEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_FOLLOWER_READ_ENABLED,
        OMConfigKeys.OZONE_OM_FOLLOWER_READ_ENABLED_DEFAULT);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    if (followerReadEnabled) {
      LOG.info("Follower reads enabled, using OMFollowerReadProxyProvider");
      this.alignmentContext = new OMClientAlignmentContext();
      this.omFailoverProxyProvider = new OMFollowerReadProxyProvider<>(
          conf, ugi, omServiceId, OzoneManagerProtocolPB.class,
          alignmentContext);
    } else {
      this.alignmentContext = null;
      this.omFailoverProxyProvider =
          new HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB>(
              conf, ugi, omServiceId, OzoneManagerProtocolPB.class);
    }

    this.rpcProxy = createRetryProxy(omFailoverProxyProvider, maxFailovers);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      OMResponse omResponse =
          rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

      if (omResponse.hasLeaderOMNodeId() && omFailoverProxyProvider != null) {
        String leaderOmId = omResponse.getLeaderOMNodeId();

        // Failover to the OM node returned by OMResponse leaderOMNodeId if
        // current proxy is not pointing to that node.
        omFailoverProxyProvider.setNextOmProxy(leaderOmId);
        omFailoverProxyProvider.performFailover(null);

        // Update leader in follower read proxy provider if applicable
        if (followerReadEnabled && 
            omFailoverProxyProvider instanceof OMFollowerReadProxyProvider) {
          ((OMFollowerReadProxyProvider<?>) omFailoverProxyProvider)
              .updateLeader(leaderOmId);
        }
      }

      // Update alignment context with response state if follower reads enabled
      if (followerReadEnabled && omResponse.hasMsyncResponse()) {
        long lastAppliedIndex = omResponse.getMsyncResponse()
            .getLastAppliedIndex();
        alignmentContext.advanceStateId(lastAppliedIndex);
      }

      return omResponse;
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          HadoopRpcOMFailoverProxyProvider.getNotLeaderException(e);
      if (notLeaderException == null) {
        throw ProtobufHelper.getRemoteException(e);
      }
      throw new IOException("Could not determine or connect to OM Leader.");
    }
  }

  /**
   * Perform msync to synchronize client state with OM leader.
   * This is used for follower reads to establish a consistent read baseline.
   *
   * @return the last applied index from the leader
   * @throws IOException if msync fails
   */
  public long msync() throws IOException {
    if (!followerReadEnabled) {
      LOG.debug("msync called but follower reads not enabled");
      return 0;
    }

    OMRequest msyncRequest = OMRequest.newBuilder()
        .setCmdType(Type.Msync)
        .setClientId(java.util.UUID.randomUUID().toString())
        .build();

    OMResponse response = submitRequest(msyncRequest);
    
    if (response.hasMsyncResponse()) {
      long lastAppliedIndex = response.getMsyncResponse().getLastAppliedIndex();
      alignmentContext.advanceStateId(lastAppliedIndex);
      return lastAppliedIndex;
    }
    return 0;
  }

  @Override
  public Text getDelegationTokenService() {
    if (omFailoverProxyProvider instanceof HadoopRpcOMFailoverProxyProvider) {
      return ((HadoopRpcOMFailoverProxyProvider<?>) omFailoverProxyProvider)
          .getCurrentProxyDelegationToken();
    } else if (omFailoverProxyProvider instanceof OMFollowerReadProxyProvider) {
      return ((OMFollowerReadProxyProvider<?>) omFailoverProxyProvider)
          .getCurrentProxyDelegationToken();
    }
    return null;
  }

  /**
   * Creates a {@link RetryProxy} encapsulating the
   * proxy provider. The retry proxy
   * fails over on network exception or if the current proxy
   * is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      OMFailoverProxyProviderBase<OzoneManagerProtocolPB> proxyProvider,
      int maxFailovers) {

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, proxyProvider,
        proxyProvider.getRetryPolicy(maxFailovers));
    return proxy;
  }

  @VisibleForTesting
  public OMFailoverProxyProviderBase<OzoneManagerProtocolPB>
      getOmFailoverProxyProvider() {
    return omFailoverProxyProvider;
  }

  /**
   * Check if follower reads are enabled.
   *
   * @return true if follower reads are enabled
   */
  public boolean isFollowerReadEnabled() {
    return followerReadEnabled;
  }

  /**
   * Get the alignment context for follower reads.
   *
   * @return the alignment context, or null if follower reads not enabled
   */
  @VisibleForTesting
  public OMClientAlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
  }
}
