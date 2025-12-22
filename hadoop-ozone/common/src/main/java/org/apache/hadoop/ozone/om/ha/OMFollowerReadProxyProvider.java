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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * A failover proxy provider that routes read requests to OM followers
 * and write requests to the OM leader.
 * 
 * This provider implements HDFS-style follower reads for Ozone Manager,
 * allowing read scalability by distributing read load across followers.
 * 
 * Key behaviors:
 * - Read requests are routed to followers in round-robin fashion
 * - Write requests are always routed to the leader
 * - Msync requests are routed to the leader to get current state
 * - On follower failure, falls back to other followers or leader
 * - Uses AlignmentContext to ensure read-after-write consistency
 */
public class OMFollowerReadProxyProvider<T> extends OMFailoverProxyProviderBase<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFollowerReadProxyProvider.class);

  private final Text delegationTokenService;
  private Map<String, OMProxyInfo> omProxyInfos;
  
  // Current known leader node ID
  private volatile String leaderNodeId;
  
  // List of follower node IDs for round-robin selection
  private final List<String> followerNodeIds = new ArrayList<>();
  
  // Index for round-robin follower selection
  private final AtomicInteger followerIndex = new AtomicInteger(0);
  
  // AlignmentContext for tracking client state
  private final OMClientAlignmentContext alignmentContext;

  /**
   * Creates an OMFollowerReadProxyProvider.
   * 
   * @param configuration the configuration
   * @param ugi the user group information
   * @param omServiceId the OM service ID
   * @param protocol the protocol class
   * @param alignmentContext the client alignment context for state tracking
   * @throws IOException if initialization fails
   */
  public OMFollowerReadProxyProvider(ConfigurationSource configuration,
                                     UserGroupInformation ugi,
                                     String omServiceId,
                                     Class<T> protocol,
                                     OMClientAlignmentContext alignmentContext)
      throws IOException {
    super(configuration, ugi, omServiceId, protocol);
    this.alignmentContext = alignmentContext;
    this.delegationTokenService = computeDelegationTokenService();
    
    // Initially, all nodes are potential followers until we learn the leader
    followerNodeIds.addAll(getOmNodeIDList());
  }

  @Override
  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
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

      OMProxyInfo omProxyInfo = new OMProxyInfo(omSvcId, nodeId, rpcAddrStr);

      if (omProxyInfo.getAddress() != null) {
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
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

  @Override
  protected T createOMProxy(InetSocketAddress omAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(getConf());

    RPC.setProtocolEngine(hadoopConf, getInterface(), ProtobufRpcEngine.class);

    RetryPolicy connectionRetryPolicy = RetryPolicies.failoverOnNetworkException(0);

    // Create proxy with AlignmentContext for state tracking
    return (T) RPC.getProtocolProxy(
        getInterface(),
        RPC.getProtocolVersion(getInterface()),
        omAddress,
        UserGroupInformation.getCurrentUser(),
        hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(getConf()),
        connectionRetryPolicy,
        null,  // fallbackToSimpleAuth
        alignmentContext
    ).getProxy();
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    // Default behavior returns the current proxy (leader or first available)
    String nodeId = getCurrentProxyOMNodeId();
    ProxyInfo<T> proxyInfo = getOMProxyMap().get(nodeId);
    if (proxyInfo == null) {
      proxyInfo = createOMProxy(nodeId);
    }
    return proxyInfo;
  }

  /**
   * Get a proxy for the current leader node.
   * Used for write requests and msync.
   * 
   * @return proxy info for the leader
   */
  public synchronized ProxyInfo<T> getLeaderProxy() {
    String nodeId = leaderNodeId != null ? leaderNodeId : getCurrentProxyOMNodeId();
    ProxyInfo<T> proxyInfo = getOMProxyMap().get(nodeId);
    if (proxyInfo == null) {
      proxyInfo = createOMProxy(nodeId);
    }
    return proxyInfo;
  }

  /**
   * Get a proxy for a follower node.
   * Uses round-robin selection among available followers.
   * 
   * @return proxy info for a follower, or leader if no followers available
   */
  public synchronized ProxyInfo<T> getFollowerProxy() {
    if (followerNodeIds.isEmpty()) {
      LOG.debug("No followers available, using leader proxy");
      return getLeaderProxy();
    }

    // Round-robin selection among followers
    int index = followerIndex.getAndIncrement() % followerNodeIds.size();
    String nodeId = followerNodeIds.get(index);
    
    // Skip if this is the known leader
    if (nodeId.equals(leaderNodeId) && followerNodeIds.size() > 1) {
      index = followerIndex.getAndIncrement() % followerNodeIds.size();
      nodeId = followerNodeIds.get(index);
    }

    ProxyInfo<T> proxyInfo = getOMProxyMap().get(nodeId);
    if (proxyInfo == null) {
      proxyInfo = createOMProxy(nodeId);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Selected follower proxy: {}", nodeId);
    }
    return proxyInfo;
  }

  /**
   * Creates proxy object for the given node.
   */
  protected ProxyInfo<T> createOMProxy(String nodeId) {
    OMProxyInfo omProxyInfo = omProxyInfos.get(nodeId);
    InetSocketAddress address = omProxyInfo.getAddress();
    ProxyInfo<T> proxyInfo;
    try {
      T proxy = createOMProxy(address);
      proxyInfo = new ProxyInfo<>(proxy, omProxyInfo.toString());
      getOMProxyMap().put(nodeId, proxyInfo);
    } catch (IOException ioe) {
      LOG.error("{} Failed to create RPC proxy to OM at {}",
          this.getClass().getSimpleName(), address, ioe);
      throw new RuntimeException(ioe);
    }
    return proxyInfo;
  }

  /**
   * Update the known leader node ID.
   * Called when we receive leader information from OM response.
   * 
   * @param newLeaderId the new leader node ID
   */
  public synchronized void updateLeader(String newLeaderId) {
    if (newLeaderId != null && !newLeaderId.equals(leaderNodeId)) {
      LOG.info("Updating leader from {} to {}", leaderNodeId, newLeaderId);
      
      // Add old leader back to followers if it was removed
      if (leaderNodeId != null && !followerNodeIds.contains(leaderNodeId)) {
        followerNodeIds.add(leaderNodeId);
      }
      
      // Update leader and remove from followers
      leaderNodeId = newLeaderId;
      followerNodeIds.remove(newLeaderId);
      
      // Update the base class's current proxy to point to leader
      setNextOmProxy(newLeaderId);
    }
  }

  /**
   * Mark a follower as unavailable (e.g., after connection failure).
   * 
   * @param nodeId the node ID to mark as unavailable
   */
  public synchronized void markFollowerUnavailable(String nodeId) {
    if (followerNodeIds.remove(nodeId)) {
      LOG.warn("Marked follower {} as unavailable", nodeId);
    }
  }

  /**
   * Re-enable a follower that was previously marked unavailable.
   * 
   * @param nodeId the node ID to re-enable
   */
  public synchronized void markFollowerAvailable(String nodeId) {
    if (!nodeId.equals(leaderNodeId) && !followerNodeIds.contains(nodeId)
        && omProxyInfos.containsKey(nodeId)) {
      followerNodeIds.add(nodeId);
      LOG.info("Marked follower {} as available", nodeId);
    }
  }

  public Text getCurrentProxyDelegationToken() {
    return delegationTokenService;
  }

  private Text computeDelegationTokenService() {
    List<String> addresses = new ArrayList<>();

    for (Map.Entry<String, OMProxyInfo> omProxyInfoSet :
        omProxyInfos.entrySet()) {
      Text dtService = omProxyInfoSet.getValue().getDelegationTokenService();
      if (dtService != null) {
        addresses.add(dtService.toString());
      }
    }

    if (!addresses.isEmpty()) {
      addresses.sort(String::compareTo);
      return new Text(String.join(",", addresses));
    } else {
      return null;
    }
  }

  /**
   * Get the alignment context used by this proxy provider.
   * 
   * @return the client alignment context
   */
  public OMClientAlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  /**
   * Get the current known leader node ID.
   * 
   * @return leader node ID, or null if unknown
   */
  public String getLeaderNodeId() {
    return leaderNodeId;
  }

  /**
   * Get the list of available follower node IDs.
   * 
   * @return list of follower node IDs
   */
  public List<String> getFollowerNodeIds() {
    return new ArrayList<>(followerNodeIds);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxyInfo : getOMProxies()) {
      if (proxyInfo != null) {
        RPC.stopProxy(proxyInfo.proxy);
      }
    }
  }

  /**
   * Get the OM proxy info map.
   * 
   * @return map of node ID to OMProxyInfo
   */
  public Map<String, OMProxyInfo> getOMProxyInfoMap() {
    return omProxyInfos;
  }
}

