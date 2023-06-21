/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectMetrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.recon.ConfigurationProvider;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.logging.log4j.util.Strings;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.function.CheckedConsumer;
import org.hadoop.ozone.recon.codegen.ReconSqlDbConfig;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION;
import static org.apache.hadoop.ozone.MiniOzoneCluster.PortAllocator.anyHostWithFreePort;
import static org.apache.hadoop.ozone.MiniOzoneCluster.PortAllocator.getFreePort;
import static org.apache.hadoop.ozone.MiniOzoneCluster.PortAllocator.localhostWithFreePort;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ADMIN_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_SERVER_PORT;
import static org.apache.hadoop.ozone.om.OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;

/**
 * MultiOMMiniOzoneHACluster creates a complete in-process Ozone cluster
 * with Multiple OM HAs and SCM HA suitable for running tests.
 * The cluster consists of a set of
 * multiple OzoneManagers HAs pointing to
 * a single StorageContainerManagers HA that manages multiple DataNodes.
 */
public class MultiOMMiniOzoneHACluster {

  public static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneHAClusterImpl.class);

  private OzoneConfiguration conf;
  
  private SCMConfigurator scmConfigurator;
  private final List<HddsDatanodeService> hddsDatanodes;
  private final ReconServer reconServer;

  private final List<OMHAService> omhaServiceList;
  private final SCMHAService scmhaService;

  private final String clusterMetaPath;

  // Timeout for the cluster to be ready
  private int waitForClusterToBeReadyTimeout = 120000; // 2 min
  private CertificateClient caClient;
  // Timeout for transfer leadership to finish
  private int waitForTransferLeadershipTimeout = 30000; // 30 seconds
  private final Set<AutoCloseable> clients = ConcurrentHashMap.newKeySet();
  private SecretKeyClient secretKeyClient;


  private static final int RATIS_RPC_TIMEOUT = 1000; // 1 second
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  /**
   * Creates a new MiniOMMiniOzoneHACluster.
   *
   * @throws IOException if there is an I/O error
   */
  public MultiOMMiniOzoneHACluster(
      OzoneConfiguration conf,
      SCMConfigurator scmConfigurator,
      List<OMHAService> omhaServiceList,
      SCMHAService scmhaService,
      List<HddsDatanodeService> hddsDatanodes,
      String clusterPath,
      ReconServer reconServer) {
    this.scmConfigurator = scmConfigurator;
    this.conf = conf;
    this.hddsDatanodes = hddsDatanodes;
    this.reconServer = reconServer;

    this.omhaServiceList = omhaServiceList;
    this.scmhaService = scmhaService;
    this.clusterMetaPath = clusterPath;
  }

  public SCMConfigurator getSCMConfigurator() {
    return scmConfigurator;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  public void setConf(OzoneConfiguration newConf) {
    this.conf = newConf;
  }
  
  public String getOMServiceId(int haServiceIndex) {
    return omhaServiceList.get(haServiceIndex).getServiceId();
  }

  public String getSCMServiceId() {
    return scmhaService.getServiceId();
  }

  /**
   * Returns the first OzoneManager from an OM HA Service.
   * @return
   */
  public OzoneManager getOzoneManager(int haServiceIndex) {
    return this.omhaServiceList.get(haServiceIndex).getServices().get(0);
  }

  private OzoneClient createClient(int haServiceIndex) throws IOException {
    String omServiceId = omhaServiceList.get(haServiceIndex).getServiceId();
    if (omServiceId == null) {
      // Non-HA cluster.
      return OzoneClientFactory.getRpcClient(getConf());
    } else {
      // HA cluster
      return OzoneClientFactory.getRpcClient(omServiceId, getConf());
    }
  }

  public boolean isOMActive(int haServiceIndex, String omNodeId) {
    return omhaServiceList.get(haServiceIndex).isServiceActive(omNodeId);
  }

  public Iterator<StorageContainerManager> getInactiveSCM(int haServiceIndex) {
    return scmhaService.inactiveServices();
  }

  public StorageContainerManager getSCM(String scmNodeId) {
    return this.scmhaService.getServiceById(scmNodeId);
  }

  public OzoneManager getOzoneManager(int haServiceIndex, int index) {
    return this.omhaServiceList.get(haServiceIndex).getServiceByIndex(index);
  }

  public OzoneManager getOzoneManager(int haServiceindex, String omNodeId) {
    return this.omhaServiceList.get(haServiceindex).getServiceById(omNodeId);
  }

  public List<OzoneManager> getOzoneManagersList(int haServiceIndex) {
    return omhaServiceList.get(haServiceIndex).getServices();
  }

  public List<StorageContainerManager> getStorageContainerManagersList() {
    return scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager(int index) {
    return this.scmhaService.getServiceByIndex(index);
  }

  public StorageContainerManager getScmLeader() {
    return getStorageContainerManagers().stream()
        .filter(StorageContainerManager::checkLeader)
        .findFirst().orElse(null);
  }

  private OzoneManager getOMLeader(int haServiceIndex,
                                   boolean waitForLeaderElection)
      throws TimeoutException, InterruptedException {
    if (waitForLeaderElection) {
      final OzoneManager[] om = new OzoneManager[1];
      GenericTestUtils.waitFor(() -> {
        om[0] = getOMLeader(haServiceIndex);
        return om[0] != null;
      }, 200, waitForClusterToBeReadyTimeout);
      return om[0];
    } else {
      return getOMLeader(haServiceIndex);
    }
  }

  /**
   * Get OzoneManager leader object.
   * @return OzoneManager object, null if there isn't one or more than one
   */
  public OzoneManager getOMLeader(int haServiceIndex) {
    OzoneManager res = null;
    for (OzoneManager ozoneManager : this.omhaServiceList.get(haServiceIndex)
        .getActiveServices()) {
      if (ozoneManager.isLeaderReady()) {
        if (res != null) {
          // Found more than one leader
          // Return null, expect the caller to retry in a while
          return null;
        }
        // Found a leader
        res = ozoneManager;
      }
    }
    return res;
  }

  /**
   * Start a previously inactive OM.
   */
  public void startInactiveOM(int haServiceIndex, String omNodeID)
      throws IOException {
    omhaServiceList.get(haServiceIndex)
        .startInactiveService(omNodeID, OzoneManager::start);
  }

  /**
   * Start a previously inactive SCM.
   */
  public void startInactiveSCM(String scmNodeId) throws IOException {
    scmhaService
        .startInactiveService(scmNodeId, StorageContainerManager::start);
  }

  public void restartOzoneManager(int haServiceIndex) throws IOException {
    for (OzoneManager ozoneManager : this.omhaServiceList.get(haServiceIndex)
        .getServices()) {
      ozoneManager.stop();
      ozoneManager.restart();
    }
  }

  public void shutdownOzoneManager(OzoneManager ozoneManager) {
    LOG.info("Shutting down OzoneManager " + ozoneManager.getOMNodeId());

    ozoneManager.stop();
  }

  public void restartOzoneManager(OzoneManager ozoneManager, boolean waitForOM)
      throws IOException, TimeoutException, InterruptedException {
    LOG.info("Restarting OzoneManager " + ozoneManager.getOMNodeId());
    ozoneManager.restart();

    if (waitForOM) {
      GenericTestUtils.waitFor(ozoneManager::isRunning,
          1000, waitForClusterToBeReadyTimeout);
    }
  }

  public void shutdownStorageContainerManager(StorageContainerManager scm) {
    LOG.info("Shutting down StorageContainerManager " + scm.getScmId());

    scm.stop();
    scmhaService.removeInstance(scm);
  }

  public StorageContainerManager restartStorageContainerManager(
      StorageContainerManager scm, boolean waitForSCM)
      throws IOException, TimeoutException,
      InterruptedException, AuthenticationException {
    LOG.info("Restarting SCM in cluster " + this.getClass());
    scmhaService.removeInstance(scm);
    OzoneConfiguration scmConf = scm.getConfiguration();
    shutdownStorageContainerManager(scm);
    scm.join();
    scm = HddsTestUtils.getScmSimple(scmConf, getSCMConfigurator());
    scmhaService.addInstance(scm, true);
    scm.start();
    if (waitForSCM) {
      waitForClusterToBeReady();
    }
    return scm;
  }

  /**
   * Waits for the Ozone cluster to be ready for processing requests.
   */
  public void waitForClusterToBeReady()
      throws TimeoutException, InterruptedException {
    waitForSCMToBeReady();
    GenericTestUtils.waitFor(() -> {
      StorageContainerManager activeScm = getActiveSCM();
      final int healthy = activeScm.getNodeCount(HEALTHY);
      final boolean isNodeReady = healthy == hddsDatanodes.size();
      final boolean exitSafeMode = !activeScm.isInSafeMode();
      final boolean checkScmLeader = activeScm.checkLeader();

      LOG.info("{}. Got {} of {} DN Heartbeats.",
          isNodeReady ? "Nodes are ready" : "Waiting for nodes to be ready",
          healthy, hddsDatanodes.size());
      LOG.info(exitSafeMode ? "Cluster exits safe mode" :
          "Waiting for cluster to exit safe mode");
      LOG.info(checkScmLeader ? "SCM became leader" :
          "SCM has not become leader");

      return isNodeReady && exitSafeMode && checkScmLeader;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  public String getClusterId() {
    return scmhaService.getServices().get(0)
        .getClientProtocolServer().getScmInfo().getClusterId();
  }

  public StorageContainerManager getActiveSCM() {
    for (StorageContainerManager scm : scmhaService.getServices()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }

  public void waitForSCMToBeReady()
      throws TimeoutException, InterruptedException  {
    GenericTestUtils.waitFor(() -> {
      for (StorageContainerManager scm : scmhaService.getServices()) {
        if (scm.checkLeader()) {
          return true;
        }
      }
      return false;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  private String getName() {
    return getClass().getSimpleName() + "-" + getClusterId();
  }

  private String getBaseDir() {
    return GenericTestUtils.getTempPath(getName());
  }

  public void shutdown() {
    try {
      LOG.info("Shutting down the Multi OM Ozone Cluster");
      IOUtils.closeQuietly(clients);
      final File baseDir = new File(getBaseDir());
      stop();
      FileUtils.deleteDirectory(baseDir);
      ContainerCache.getInstance(conf).shutdownCache();
      DefaultMetricsSystem.shutdown();

      ManagedRocksObjectMetrics.INSTANCE.assertNoLeaks();
      CodecBuffer.assertNoLeaks();
    } catch (IOException e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }

  public void stop() {
    for (OMHAService omhaService: this.omhaServiceList) {
      for (OzoneManager ozoneManager : omhaService.getServices()) {
        if (ozoneManager != null) {
          LOG.info("Stopping the OzoneManager {}", ozoneManager.getOMNodeId());
          ozoneManager.stop();
          ozoneManager.join();
        }
      }
    }


    for (StorageContainerManager scm : this.scmhaService.getServices()) {
      if (scm != null) {
        LOG.info("Stopping the StorageContainerManager {}", scm.getScmId());
        scm.stop();
        scm.join();
      }
    }

    stopDatanodes(hddsDatanodes);
    stopRecon(reconServer);
  }

  private CertificateClient getCAClient() {
    return this.caClient;
  }

  private void setCAClient(CertificateClient client) {
    this.caClient = client;
  }

  private void setSecretKeyClient(SecretKeyClient client) {
    this.secretKeyClient = client;
  }

  /**
   * Start DataNodes.
   */
  public void startHddsDatanodes() {
    hddsDatanodes.forEach((datanode) -> {
      try {
        datanode.setCertificateClient(getCAClient());
      } catch (IOException e) {
        LOG.error("Exception while setting certificate client to DataNode.", e);
      }
      datanode.setSecretKeyClient(secretKeyClient);
      datanode.start();
    });
  }

  private static void stopDatanodes(
      Collection<HddsDatanodeService> hddsDatanodes) {
    if (!hddsDatanodes.isEmpty()) {
      LOG.info("Stopping the HddsDatanodes");
      hddsDatanodes.parallelStream()
          .forEach(MultiOMMiniOzoneHACluster::stopDatanode);
    }
  }

  private static void stopDatanode(HddsDatanodeService dn) {
    if (dn != null) {
      dn.stop();
      dn.join();
    }
  }

  private static void stopRecon(ReconServer reconServer) {
    try {
      if (reconServer != null) {
        LOG.info("Stopping Recon");
        reconServer.stop();
        reconServer.join();
      }
    } catch (Exception e) {
      LOG.error("Exception while shutting down Recon.", e);
    }
  }

  public void stopOzoneManager(int haServiceIndex, int index) {
    OzoneManager om = omhaServiceList.get(haServiceIndex)
        .getServices().get(index);
    om.stop();
    om.join();
    omhaServiceList.get(haServiceIndex).deactivate(om);
  }

  public void stopOzoneManager(int haServiceIndex, String omNodeId) {
    OzoneManager om = omhaServiceList.get(haServiceIndex)
        .getServiceById(omNodeId);
    om.stop();
    om.join();
    omhaServiceList.get(haServiceIndex).deactivate(om);
  }

  private static void configureOMPorts(ConfigurationTarget conf,
                                       String omServiceId, String omNodeId) {

    String omAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpsAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
    String omRatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

    conf.set(omAddrKey, localhostWithFreePort());
    conf.set(omHttpAddrKey, localhostWithFreePort());
    conf.set(omHttpsAddrKey, localhostWithFreePort());
    conf.setInt(omRatisPortKey, getFreePort());
  }

  /**
   * Builder for configuring the MultiOMMiniOzoneHACluster to run.
   */
  @SuppressWarnings("visibilitymodifier")
  public static class Builder {

    private static final int DEFAULT_HB_INTERVAL_MS = 1000;
    private static final int DEFAULT_HB_PROCESSOR_INTERVAL_MS = 100;
    private static final int ACTIVE_OMS_NOT_SET = -1;
    private static final int ACTIVE_SCMS_NOT_SET = -1;
    private static final int DEFAULT_PIPELINE_LIMIT = 3;
    private static final int DEFAULT_RATIS_RPC_TIMEOUT_SEC = 1;

    private OzoneConfiguration conf;
    private String path;

    private String clusterId;
    private List<String> omServiceIds;
    private int numOfOMHAServices;
    private int numOfOMsPerHAService;
    private int numOfActiveOMsPerHAService = ACTIVE_OMS_NOT_SET;

    private static final String OM_NODE_ID_PREFIX = "omNode-";
    private List<List<OzoneManager>> activeOMs = new ArrayList<>();
    private List<List<OzoneManager>> inactiveOMs = new ArrayList<>();

    private String scmServiceId;
    private int numOfSCMs;
    private int numOfActiveSCMs = ACTIVE_SCMS_NOT_SET;
    private SCMConfigurator scmConfigurator;

    private Optional<Boolean> enableTrace = Optional.of(false);
    private Optional<Integer> hbInterval = Optional.empty();
    private Optional<Integer> hbProcessorInterval = Optional.empty();
    private Optional<String> scmId = Optional.empty();

    private Boolean enableContainerDatastream = true;
    private Optional<String> datanodeReservedSpace = Optional.empty();
    private Optional<Integer> chunkSize = Optional.empty();
    private OptionalInt streamBufferSize = OptionalInt.empty();
    private Optional<Long> streamBufferFlushSize = Optional.empty();
    private Optional<Long> dataStreamBufferFlushSize = Optional.empty();
    private Optional<Long> datastreamWindowSize = Optional.empty();
    private Optional<Long> streamBufferMaxSize = Optional.empty();
    private OptionalInt dataStreamMinPacketSize = OptionalInt.empty();
    private Optional<Long> blockSize = Optional.empty();
    private Optional<StorageUnit> streamBufferSizeUnit = Optional.empty();
    private boolean includeRecon = false;


    private Optional<Integer> omLayoutVersion = Optional.empty();
    private Optional<Integer> scmLayoutVersion = Optional.empty();
    private Optional<Integer> dnLayoutVersion = Optional.empty();

    // Use relative smaller number of handlers for testing
    private int numOfOmHandlers = 20;
    private int numOfScmHandlers = 20;
    private int numOfDatanodes = 3;
    private int numDataVolumes = 1;
    private boolean  startDataNodes = true;
    private CertificateClient certClient;
    private int pipelineNumLimit = DEFAULT_PIPELINE_LIMIT;

    private static final String SCM_NODE_ID_PREFIX = "scmNode-";
    private List<StorageContainerManager> activeSCMs = new ArrayList<>();
    private List<StorageContainerManager> inactiveSCMs = new ArrayList<>();

    public Builder(OzoneConfiguration conf) {
      this.conf = conf;
      setClusterId(UUID.randomUUID().toString());
      // Use default SCM configurations i fno override is provided.(new SCm)
      setSCMConfigurator(new SCMConfigurator());
      ExitUtils.disableSystemExit();
    }
    
    public MultiOMMiniOzoneHACluster build() throws IOException {
      if (numOfActiveOMsPerHAService > numOfOMsPerHAService) {
        throw new IllegalArgumentException("Number of active OMs cannot be " +
            "more than the total number of OMs");
      }

      // If num of ActiveOMs is not set, set it to numOfOMs.
      if (numOfActiveOMsPerHAService == ACTIVE_OMS_NOT_SET) {
        numOfActiveOMsPerHAService = numOfOMsPerHAService;
      }

      // If num of SCMs it not set, set it to 1.
      if (numOfSCMs == 0) {
        numOfSCMs = 1;
      }

      // If num of ActiveSCMs is not set, set it to numOfSCMs.
      if (numOfActiveSCMs == ACTIVE_SCMS_NOT_SET) {
        numOfActiveSCMs = numOfSCMs;
      }

      DefaultMetricsSystem.setMiniClusterMode(true);
      DatanodeStoreCache.setMiniClusterMode();
      initializeConfiguration();
      initOMRatisConf();
      SCMHAService scmService;
      List<OMHAService> omServiceList;
      ReconServer reconServer = null;
      try {
        scmService = createSCMService();
        omServiceList = createOMServices();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build  ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          scmService.getActiveServices(), reconServer);

      MultiOMMiniOzoneHACluster cluster = new MultiOMMiniOzoneHACluster(conf,
          scmConfigurator, omServiceList, scmService, hddsDatanodes, path,
          reconServer);

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }

    private String getSCMAddresses(List<StorageContainerManager> scms) {
      StringBuilder stringBuilder = new StringBuilder();
      Iterator<StorageContainerManager> iter = scms.iterator();

      while (iter.hasNext()) {
        StorageContainerManager scm = iter.next();
        stringBuilder.append(scm.getDatanodeRpcAddress().getHostString() +
            ":" + scm.getDatanodeRpcAddress().getPort());
        if (iter.hasNext()) {
          stringBuilder.append(",");
        }

      }
      return stringBuilder.toString();
    }

    private void configureHddsDatanodes() {
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED,
          enableContainerDatastream);
    }

    /**
     * Creates HddsDatanodeService(s) instance.
     *
     * @return List of HddsDatanodeService
     * @throws IOException
     */
    private List<HddsDatanodeService> createHddsDatanodes(
        List<StorageContainerManager> scms, ReconServer reconServer)
        throws IOException {
      configureHddsDatanodes();
      String scmAddress = getSCMAddresses(scms);
      String[] args = new String[] {};
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, scmAddress);
      List<HddsDatanodeService> hddsDatanodes = new ArrayList<>();
      for (int i = 0; i < numOfDatanodes; i++) {
        OzoneConfiguration dnConf = new OzoneConfiguration(conf);
        configureDatanodePorts(dnConf);
        String datanodeBaseDir = path + "/datanode-" + Integer.toString(i);
        Path metaDir = Paths.get(datanodeBaseDir, "meta");
        List<String> dataDirs = new ArrayList<>();
        List<String> reservedSpaceList = new ArrayList<>();
        for (int j = 0; j < numDataVolumes; j++) {
          Path dir = Paths.get(datanodeBaseDir, "data-" + j, "containers");
          Files.createDirectories(dir);
          dataDirs.add(dir.toString());
          datanodeReservedSpace.ifPresent(
              s -> reservedSpaceList.add(dir + ":" + s));
        }
        String reservedSpaceString = String.join(",", reservedSpaceList);
        String listOfDirs = String.join(",", dataDirs);
        Path ratisDir = Paths.get(datanodeBaseDir, "data", "ratis");
        Path workDir = Paths.get(datanodeBaseDir, "data", "replication",
            "work");
        Files.createDirectories(metaDir);
        Files.createDirectories(ratisDir);
        Files.createDirectories(workDir);
        dnConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
        dnConf.set(DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY, listOfDirs);
        dnConf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, listOfDirs);
        dnConf.set(ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED,
            reservedSpaceString);
        dnConf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
            ratisDir.toString());
        if (reconServer != null) {
          OzoneStorageContainerManager reconScm =
              reconServer.getReconStorageContainerManager();
          dnConf.set(OZONE_RECON_ADDRESS_KEY,
              reconScm.getDatanodeRpcAddress().getHostString() + ":" +
                  reconScm.getDatanodeRpcAddress().getPort());
        }

        HddsDatanodeService datanode
            = HddsDatanodeService.createHddsDatanodeService(args);
        datanode.setConfiguration(dnConf);
        hddsDatanodes.add(datanode);
      }
      if (dnLayoutVersion.isPresent()) {
        configureLayoutVersionInDatanodes(hddsDatanodes, dnLayoutVersion.get());
      }
      return hddsDatanodes;
    }

    private void configureLayoutVersionInDatanodes(
        List<HddsDatanodeService> dns, int layoutVersion) throws IOException {
      for (HddsDatanodeService dn : dns) {
        DatanodeLayoutStorage layoutStorage;
        layoutStorage = new DatanodeLayoutStorage(dn.getConf(),
            UUID.randomUUID().toString(), layoutVersion);
        layoutStorage.initialize();
      }
    }

    private void configureSCM() {
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY,
          localhostWithFreePort());
      conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numOfScmHandlers);
      conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
          "3s");
      configureSCMheartbeat();
    }

    private void configureSCMheartbeat() {
      if (hbInterval.isPresent()) {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            hbInterval.get(), TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            DEFAULT_HB_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }

      if (hbProcessorInterval.isPresent()) {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            hbProcessorInterval.get(),
            TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            DEFAULT_HB_PROCESSOR_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }
    }

    private void configureDatanodePorts(ConfigurationTarget dnConf) {
      dnConf.set(ScmConfigKeys.HDDS_REST_HTTP_ADDRESS_KEY,
          anyHostWithFreePort());
      dnConf.set(HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY,
          anyHostWithFreePort());
      dnConf.set(HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY,
          anyHostWithFreePort());
      dnConf.setInt(DFS_CONTAINER_IPC_PORT, getFreePort());
      dnConf.setInt(DFS_CONTAINER_RATIS_IPC_PORT, getFreePort());
      dnConf.setInt(DFS_CONTAINER_RATIS_ADMIN_PORT, getFreePort());
      dnConf.setInt(DFS_CONTAINER_RATIS_SERVER_PORT, getFreePort());
      dnConf.setInt(DFS_CONTAINER_RATIS_DATASTREAM_PORT, getFreePort());
      dnConf.setFromObject(new ReplicationConfig().setPort(getFreePort()));
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
        GenericTestUtils.setRootLogLevel(Level.TRACE);
      }
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }


    private void configureRecon() throws IOException {
      ConfigurationProvider.resetConfiguration();

      TemporaryFolder tempFolder = new TemporaryFolder();
      tempFolder.create();
      File tempNewFolder = tempFolder.newFolder();
      conf.set(OZONE_RECON_DB_DIR,
          tempNewFolder.getAbsolutePath());
      conf.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, tempNewFolder
          .getAbsolutePath());
      conf.set(OZONE_RECON_SCM_DB_DIR,
          tempNewFolder.getAbsolutePath());

      ReconSqlDbConfig dbConfig = conf.getObject(ReconSqlDbConfig.class);
      dbConfig.setJdbcUrl("jdbc:derby:" + tempNewFolder.getAbsolutePath()
          + "/ozone_recon_derby.db");
      conf.setFromObject(dbConfig);

      conf.set(OZONE_RECON_HTTP_ADDRESS_KEY, anyHostWithFreePort());
      conf.set(OZONE_RECON_DATANODE_ADDRESS_KEY, anyHostWithFreePort());
      conf.set(OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD, "10s");

      ConfigurationProvider.setConfiguration(conf);
    }

    /**
     * Initializes the configuration required for starting 
     * MultiOMMiniOzoneHACluster.
     *
     * @throws IOException
     */
    private void initializeConfiguration() throws IOException {
      Path metaDir = Paths.get(path, "ozone-meta");
      Files.createDirectories(metaDir);
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
      // conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
      if (!chunkSize.isPresent()) {
        //set it to 1MB by default in tests
        chunkSize = Optional.of(1);
      }
      if (!streamBufferSize.isPresent()) {
        streamBufferSize = OptionalInt.of(chunkSize.get());
      }
      if (!streamBufferFlushSize.isPresent()) {
        streamBufferFlushSize = Optional.of((long) chunkSize.get());
      }
      if (!streamBufferMaxSize.isPresent()) {
        streamBufferMaxSize = Optional.of(2 * streamBufferFlushSize.get());
      }
      if (!dataStreamBufferFlushSize.isPresent()) {
        dataStreamBufferFlushSize = Optional.of((long) 4 * chunkSize.get());
      }
      if (!dataStreamMinPacketSize.isPresent()) {
        dataStreamMinPacketSize = OptionalInt.of(chunkSize.get() / 4);
      }
      if (!datastreamWindowSize.isPresent()) {
        datastreamWindowSize = Optional.of((long) 8 * chunkSize.get());
      }
      if (!blockSize.isPresent()) {
        blockSize = Optional.of(2 * streamBufferMaxSize.get());
      }

      if (!streamBufferSizeUnit.isPresent()) {
        streamBufferSizeUnit = Optional.of(StorageUnit.MB);
      }

      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamBufferSize(
          (int) Math.round(
              streamBufferSizeUnit.get().toBytes(streamBufferSize.getAsInt())));
      clientConfig.setStreamBufferMaxSize(Math.round(
          streamBufferSizeUnit.get().toBytes(streamBufferMaxSize.get())));
      clientConfig.setStreamBufferFlushSize(Math.round(
          streamBufferSizeUnit.get().toBytes(streamBufferFlushSize.get())));
      clientConfig.setDataStreamBufferFlushSize(Math.round(
          streamBufferSizeUnit.get().toBytes(dataStreamBufferFlushSize.get())));
      clientConfig.setDataStreamMinPacketSize((int) Math.round(
          streamBufferSizeUnit.get()
              .toBytes(dataStreamMinPacketSize.getAsInt())));
      clientConfig.setStreamWindowSize(Math.round(
          streamBufferSizeUnit.get().toBytes(datastreamWindowSize.get())));
      conf.setFromObject(clientConfig);

      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
          chunkSize.get(), streamBufferSizeUnit.get());

      conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, blockSize.get(),
          streamBufferSizeUnit.get());
      // MultiOMMiniOzoneHACluster should have global pipeline upper limit.
      conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
          pipelineNumLimit >= DEFAULT_PIPELINE_LIMIT ?
              pipelineNumLimit : DEFAULT_PIPELINE_LIMIT);
      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          DEFAULT_RATIS_RPC_TIMEOUT_SEC, TimeUnit.SECONDS);
      SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
      // default max retry timeout set to 30s
      scmClientConfig.setMaxRetryTimeout(30 * 1000);
      conf.setFromObject(scmClientConfig);
      // In this way safemode exit will happen only when atleast we have one
      // pipeline.
      conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
          numOfDatanodes >= 3 ? 3 : 1);
      configureTrace();
    }
    
    private void initOMRatisConf() {
      conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);

      // If test change the following config values we will respect,
      // otherwise we will set lower timeout values.
      long defaultDuration = OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
          .getDuration();
      long curRatisRpcTimeout = conf.getTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          defaultDuration, TimeUnit.MILLISECONDS);
      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          defaultDuration == curRatisRpcTimeout ?
              RATIS_RPC_TIMEOUT : curRatisRpcTimeout, TimeUnit.MILLISECONDS);

      long defaultNodeFailureTimeout =
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.
              getDuration();
      long curNodeFailureTimeout = conf.getTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
          defaultNodeFailureTimeout,
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.
              getUnit());
      conf.setTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
          curNodeFailureTimeout == defaultNodeFailureTimeout ?
              NODE_FAILURE_TIMEOUT : curNodeFailureTimeout,
          TimeUnit.MILLISECONDS);
    }

    private void initCommonOMHAConfig() {
      // Set configurations required for starting OM HA services, because that
      // is the serviceIDs being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY,
          String.join(",", omServiceIds));
    }

    /**
     * Start OM service with multiple OMs.
     */
    private List<OMHAService> createOMServices() throws IOException,
        AuthenticationException {
      if (omServiceIds == null || numOfOMHAServices != omServiceIds.size()) {
        throw new IllegalArgumentException("OM HA Service Ids should be " +
            "specified and consistent with the number of OM HA services");
      }

      List<OzoneManager> omList = Lists.newArrayList();
      int retryCount = 0;
      List<OMHAService> omServices = new ArrayList<>();

      while (true) {
        try {
          initCommonOMHAConfig();
          for (int i = 0; i < numOfOMHAServices; i++) {
            activeOMs.add(new ArrayList<>());
            inactiveOMs.add(new ArrayList<>());
            initOMHAConfig(i);
            for (int j = 1; j <= numOfOMsPerHAService; j++) {
              // Set nodeId
              String nodeId = OM_NODE_ID_PREFIX + j;
              OzoneConfiguration config = new OzoneConfiguration(conf);
              config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);
              // Set the OM http(s) address to null so that the cluster picks
              // up the address set with service ID and node ID in initHAConfig
              config.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "");
              config.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "");

              // Set metadata/DB dir base path
              String metaDirPath = path + "/" + i + "/" + nodeId;
              config.set(OZONE_METADATA_DIRS, metaDirPath);

              // Set non standard layout version if needed.
              omLayoutVersion.ifPresent(integer ->
                  config.set(OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION,
                      String.valueOf(integer)));

              OzoneManager.omInit(config);
              OzoneManager om = OzoneManager.createOm(config);
              if (certClient != null) {
                om.setCertClient(certClient);
              }
              omList.add(om);

              if (j <= numOfOMsPerHAService) {
                om.start();
                activeOMs.get(i).add(om);
                LOG.info("Started OzoneManager {} RPC server at {}", nodeId,
                    om.getOmRpcServerAddr());
              } else {
                inactiveOMs.get(i).add(om);
                LOG.info("Initialized OzoneManager at {}. This OM is currently "
                    + "inactive (not running).", om.getOmRpcServerAddr());
              }
            }
            omServices.add(new OMHAService(
                activeOMs.get(i), inactiveOMs.get(i), omServiceIds.get(i)));
          }
          break;
        } catch (BindException e) {
          for (OzoneManager om : omList) {
            om.stop();
            om.join();
            LOG.info("Stopping OzoneManager server at {}",
                om.getOmRpcServerAddr());
          }
          omList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        }
      }
      return omServices;
    }

    /**
     * Creates a new StorageContainerManager instance.
     *
     * @return {@link StorageContainerManager}
     * @throws IOException
     */
    protected StorageContainerManager createSCM()
        throws IOException, AuthenticationException {
      configureSCM();
      SCMStorageConfig scmStore;

      // Set non standard layout version if needed.
      scmLayoutVersion.ifPresent(integer ->
          conf.set(HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
              String.valueOf(integer)));

      scmStore = new SCMStorageConfig(conf);
      initializeScmStorage(scmStore);
      StorageContainerManager scm = HddsTestUtils.getScmSimple(conf,
          scmConfigurator);
      HealthyPipelineSafeModeRule rule =
          scm.getScmSafeModeManager().getHealthyPipelineSafeModeRule();
      if (rule != null) {
        // Set threshold to wait for safe mode exit - this is needed since a
        // pipeline is marked open only after leader election.
        rule.setHealthyPipelineThresholdCount(numOfDatanodes / 3);
      }
      return scm;
    }

    protected void initializeScmStorage(SCMStorageConfig scmStore)
        throws IOException {
      if (scmStore.getState() == StorageState.INITIALIZED) {
        return;
      }
      scmStore.setClusterId(clusterId);
      if (!scmId.isPresent()) {
        scmId = Optional.of(UUID.randomUUID().toString());
      }
      scmStore.setScmId(scmId.get());
      scmStore.initialize();
      //TODO: HDDS-6897
      //Disabling Ratis for only of MultiOMMiniOzoneHAClusterImpl.
      //MultiOMMiniOzoneHAClusterImpl doesn't work with Ratis enabled SCM
      if (Strings.isNotEmpty(conf.get(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY))
          && SCMHAUtils.isSCMHAEnabled(conf)) {
        scmStore.setSCMHAFlag(true);
        scmStore.persistCurrentState();
        SCMRatisServerImpl.initialize(clusterId, scmId.get(),
            SCMHANodeDetails.loadSCMHAConfig(conf, scmStore)
                .getLocalNodeDetails(), conf);
      }
    }

    /**
     * Start OM service with multiple OMs.
     */
    private SCMHAService createSCMService()
        throws IOException, AuthenticationException {
      if (scmServiceId == null) {
        StorageContainerManager scm = createSCM();
        scm.start();
        return new SCMHAService(singletonList(scm), null, null);
      }

      List<StorageContainerManager> scmList = Lists.newArrayList();

      int retryCount = 0;

      while (true) {
        try {
          initSCMHAConfig();

          for (int i = 1; i <= numOfSCMs; i++) {
            // Set nodeId
            String nodeId = SCM_NODE_ID_PREFIX + i;
            String metaDirPath = path + "/" + nodeId;
            OzoneConfiguration scmConfig = new OzoneConfiguration(conf);
            scmConfig.set(OZONE_METADATA_DIRS, metaDirPath);
            scmConfig.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, nodeId);
            scmConfig.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);

            scmLayoutVersion.ifPresent(integer ->
                scmConfig.set(HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
                    String.valueOf(integer)));

            configureSCM();
            if (i == 1) {
              StorageContainerManager.scmInit(scmConfig, clusterId);
            } else {
              StorageContainerManager.scmBootstrap(scmConfig);
            }
            StorageContainerManager scm =
                HddsTestUtils.getScmSimple(scmConfig, scmConfigurator);
            HealthyPipelineSafeModeRule rule =
                scm.getScmSafeModeManager().getHealthyPipelineSafeModeRule();
            if (rule != null) {
              // Set threshold to wait for safe mode exit -
              // this is needed since a pipeline is marked open only after
              // leader election.
              rule.setHealthyPipelineThresholdCount(numOfDatanodes / 3);
            }
            scmList.add(scm);

            if (i <= numOfActiveSCMs) {
              scm.start();
              activeSCMs.add(scm);
              LOG.info("Started SCM RPC server at {}",
                  scm.getClientRpcAddress());
            } else {
              inactiveSCMs.add(scm);
              LOG.info("Intialized SCM at {}. This SCM is currently "
                  + "inactive (not running).", scm.getClientRpcAddress());
            }
          }
          break;
        } catch (BindException e) {
          for (StorageContainerManager scm : scmList) {
            scm.stop();
            scm.join();
            LOG.info("Stopping StorageContainerManager server at {}",
                scm.getClientRpcAddress());
          }
          scmList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        }
      }

      return new SCMHAService(activeSCMs, inactiveSCMs, scmServiceId);
    }

    /**
     * Initialize HA related configurations.
     */
    private void initSCMHAConfig() {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
      conf.set(ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID, scmServiceId);
      String scmNodesKey = ConfUtils.addKeySuffixes(
          ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
      StringBuilder scmNodesKeyValue = new StringBuilder();
      StringBuilder scmNames = new StringBuilder();

      for (int i = 1; i <= numOfSCMs; i++) {
        String scmNodeId = SCM_NODE_ID_PREFIX + i;
        scmNodesKeyValue.append(",").append(scmNodeId);

        String scmAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpsAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmRatisPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, scmServiceId, scmNodeId);
        String dnPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String blockClientKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String ssClientKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String scmGrpcPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, scmServiceId, scmNodeId);
        String scmSecurityAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY, scmServiceId,
            scmNodeId);

        conf.set(scmAddrKey, "127.0.0.1");
        conf.set(scmHttpAddrKey, localhostWithFreePort());
        conf.set(scmHttpsAddrKey, localhostWithFreePort());
        conf.set(scmSecurityAddrKey, localhostWithFreePort());
        conf.set("ozone.scm.update.service.port", "0");

        int ratisPort = getFreePort();
        conf.setInt(scmRatisPortKey, ratisPort);
        //conf.setInt("ozone.scm.ha.ratis.bind.port", ratisPort);

        int dnPort = getFreePort();
        conf.set(dnPortKey, "127.0.0.1:" + dnPort);
        scmNames.append(",localhost:").append(dnPort);

        conf.set(ssClientKey, localhostWithFreePort());
        conf.setInt(scmGrpcPortKey, getFreePort());

        String blockAddress = localhostWithFreePort();
        conf.set(blockClientKey, blockAddress);
        conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            blockAddress);
      }

      conf.set(scmNodesKey, scmNodesKeyValue.substring(1));
      conf.set(ScmConfigKeys.OZONE_SCM_NAMES, scmNames.substring(1));
    }

    /**
     * Initialize HA related configurations.
     */
    private void initOMHAConfig(int haServiceIndex) {
      String omServiceId = omServiceIds.get(haServiceIndex);
      conf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, omServiceId);
      String omNodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      List<String> omNodeIds = new ArrayList<>();

      for (int i = 1; i <= numOfOMsPerHAService; i++) {
        String omNodeId = OM_NODE_ID_PREFIX + i;
        omNodeIds.add(omNodeId);

        configureOMPorts(conf, omServiceId, omNodeId);
      }

      conf.set(omNodesKey, String.join(",", omNodeIds));
    }

    public Builder setConf(OzoneConfiguration config) {
      this.conf = config;
      return this;
    }

    public Builder setSCMConfigurator(SCMConfigurator configurator) {
      this.scmConfigurator = configurator;
      return this;
    }

    /**
     * Sets the cluster Id.
     *
     * @param id cluster Id
     *
     * @return MultiOMMiniOzoneHACluster.Builder
     */
    public Builder setClusterId(String id) {
      clusterId = id;
      path = GenericTestUtils.getTempPath(
          MultiOMMiniOzoneHACluster.class.getSimpleName() + "-" + clusterId);
      return this;
    }

    /**
     * Sets the SCM id.
     *
     * @param id SCM Id
     *
     * @return MultiOMMiniOzoneHACluster.Builder
     */
    public Builder setScmId(String id) {
      scmId = Optional.of(id);
      return this;
    }

    /**
     * For tests that do not use any features of SCM, we can get by with
     * 0 datanodes.  Also need to skip safemode in this case.
     * This allows the cluster to come up much faster.
     */
    public Builder withoutDatanodes() {
      setNumDatanodes(0);
      conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, false);
      return this;
    }

    public Builder setStartDataNodes(boolean nodes) {
      this.startDataNodes = nodes;
      return this;
    }

    /**
     * Sets the certificate client.
     *
     * @param client
     *
     * @return Builder
     */
    public Builder setCertificateClient(CertificateClient client) {
      this.certClient = client;
      return this;
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MultiOMMiniOzoneHACluster.
     *
     * @param val number of datanodes
     *
     * @return Builder
     */
    public Builder setNumDatanodes(int val) {
      numOfDatanodes = val;
      return this;
    }

    /**
     * Sets the number of data volumes per datanode.
     *
     * @param val number of volumes per datanode.
     *
     * @return Builder
     */
    public Builder setNumDataVolumes(int val) {
      numDataVolumes = val;
      return this;
    }

    /**
     * Sets the total number of pipelines to create.
     * @param val number of pipelines
     * @return Builder
     */
    public Builder setTotalPipelineNumLimit(int val) {
      pipelineNumLimit = val;
      return this;
    }

    /**
     * Sets the number of HeartBeat Interval of Datanodes, the value should be
     * in MilliSeconds.
     *
     * @param val HeartBeat interval in milliseconds
     *
     * @return Builder
     */
    public Builder setHbInterval(int val) {
      hbInterval = Optional.of(val);
      return this;
    }

    /**
     * Sets the number of HeartBeat Processor Interval of Datanodes,
     * the value should be in MilliSeconds.
     *
     * @param val HeartBeat Processor interval in milliseconds
     *
     * @return Builder
     */
    public Builder setHbProcessorInterval(int val) {
      hbProcessorInterval = Optional.of(val);
      return this;
    }

    /**
     * When set to true, enables trace level logging.
     *
     * @param trace true or false
     *
     * @return Builder
     */
    public Builder setTrace(Boolean trace) {
      enableTrace = Optional.of(trace);
      return this;
    }

    /**
     * Sets the reserved space
     * {@link org.apache.hadoop.hdds.scm.ScmConfigKeys}
     * HDDS_DATANODE_DIR_DU_RESERVED
     * for each volume in each datanode.
     * @param reservedSpace String that contains the numeric size value and
     *                      ends with a
     *                      {@link org.apache.hadoop.hdds.conf.StorageUnit}
     *                      suffix. For example, "50GB".
     * @see org.apache.hadoop.ozone.container.common.volume.VolumeInfo
     *
     * @return {@link MultiOMMiniOzoneHACluster} Builder
     */
    public Builder setDatanodeReservedSpace(String reservedSpace) {
      datanodeReservedSpace = Optional.of(reservedSpace);
      return this;
    }

    /**
     * Sets the chunk size.
     *
     * @return Builder
     */
    public Builder setChunkSize(int size) {
      chunkSize = Optional.of(size);
      return this;
    }

    public Builder setStreamBufferSize(int size) {
      streamBufferSize = OptionalInt.of(size);
      return this;
    }

    /**
     * Sets the flush size for stream buffer.
     *
     * @return Builder
     */
    public Builder setStreamBufferFlushSize(long size) {
      streamBufferFlushSize = Optional.of(size);
      return this;
    }

    /**
     * Sets the max size for stream buffer.
     *
     * @return Builder
     */
    public Builder setStreamBufferMaxSize(long size) {
      streamBufferMaxSize = Optional.of(size);
      return this;
    }

    public Builder setDataStreamBufferFlushize(long size) {
      dataStreamBufferFlushSize = Optional.of(size);
      return this;
    }

    public Builder setDataStreamMinPacketSize(int size) {
      dataStreamMinPacketSize = OptionalInt.of(size);
      return this;
    }

    public Builder setDataStreamStreamWindowSize(long size) {
      datastreamWindowSize = Optional.of(size);
      return this;
    }

    /**
     * Sets the block size for stream buffer.
     *
     * @return Builder
     */
    public Builder setBlockSize(long size) {
      blockSize = Optional.of(size);
      return this;
    }

    public Builder setNumOfOMHAServices(int numOfOMHAServices) {
      this.numOfOMHAServices = numOfOMHAServices;
      return this;
    }

    public Builder setNumOfOmsPerHAService(int numOMs) {
      this.numOfOMsPerHAService = numOMs;
      return this;
    }

    public Builder setNumOfActiveOMsPerHAService(int numActiveOMs) {
      this.numOfActiveOMsPerHAService = numActiveOMs;
      return this;
    }

    public Builder setStreamBufferSizeUnit(StorageUnit unit) {
      this.streamBufferSizeUnit = Optional.of(unit);
      return this;
    }

    public Builder setOMServiceIds(List<String> serviceIds) {
      this.omServiceIds = serviceIds;
      return this;
    }

    public Builder includeRecon(boolean include) {
      this.includeRecon = include;
      return this;
    }

    public Builder setNumOfStorageContainerManagers(int numSCMs) {
      this.numOfSCMs = numSCMs;
      return this;
    }

    public Builder setNumOfActiveSCMs(int numActiveSCMs) {
      this.numOfActiveSCMs = numActiveSCMs;
      return this;
    }

    public Builder setSCMServiceId(String serviceId) {
      this.scmServiceId = serviceId;
      return this;
    }

    public Builder setScmLayoutVersion(int layoutVersion) {
      scmLayoutVersion = Optional.of(layoutVersion);
      return this;
    }

    public Builder setOmLayoutVersion(int layoutVersion) {
      omLayoutVersion = Optional.of(layoutVersion);
      return this;
    }

    public Builder setDnLayoutVersion(int layoutVersion) {
      dnLayoutVersion = Optional.of(layoutVersion);
      return this;
    }
  }

  /**
   * Bootstrap new OM by updating existing OM configs.
   */
  public void bootstrapOzoneManager(int haServiceIndex,
                                    String omNodeId) throws Exception {
    bootstrapOzoneManager(haServiceIndex, omNodeId, true, false);
  }

  /**
   * Bootstrap new OM and add to existing OM HA service ring.
   * @param omHAServiceIndex index of OM HA Service in the cluster
   * @param omNodeId nodeId of new OM
   * @param updateConfigs if true, update the old OM configs with new node
   *                      information
   * @param force if true, start new OM with FORCE_BOOTSTRAP option.
   *              Otherwise, start new OM with BOOTSTRAP option.
   */
  public void bootstrapOzoneManager(int omHAServiceIndex, String omNodeId,
        boolean updateConfigs, boolean force) throws Exception {

    // Set testReloadConfigFlag to true so that
    // OzoneManager#reloadConfiguration does not reload config as it will
    // return the default configurations.
    OzoneManager.setTestReloadConfigFlag(true);

    int retryCount = 0;
    OzoneManager om = null;

    OzoneManager omLeader = getOMLeader(omHAServiceIndex, true);
    long leaderSnapshotIndex = omLeader.getRatisSnapshotIndex();

    while (true) {
      try {
        OzoneConfiguration newConf = addNewOMToConfig(
            getOMServiceId(omHAServiceIndex), omNodeId);

        if (updateConfigs) {
          updateOMConfigs(omHAServiceIndex, newConf);
        }

        om = bootstrapNewOM(omHAServiceIndex, omNodeId, newConf, force);

        LOG.info("Bootstrapped OzoneManager {} RPC server at {}", omNodeId,
            om.getOmRpcServerAddr());

        // Add new OMs to cluster's in memory map and update existing OMs conf.
        setConf(newConf);

        break;
      } catch (IOException e) {
        // Existing OM config could have been updated with new conf. Reset it.
        for (OzoneManager existingOM : omhaServiceList.get(omHAServiceIndex)
            .getServices()) {
          existingOM.setConfiguration(getConf());
        }
        if (e instanceof BindException ||
            e.getCause() instanceof BindException) {
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        } else {
          throw e;
        }
      }
    }

    waitForBootstrappedNodeToBeReady(om, leaderSnapshotIndex);
    if (updateConfigs) {
      waitForConfigUpdateOnActiveOMs(omHAServiceIndex, omNodeId);
    }
  }

  /**
   * Set the configs for new OMs.
   */
  private OzoneConfiguration addNewOMToConfig(String omServiceId,
                                              String omNodeId) {
    OzoneConfiguration newConf = new OzoneConfiguration(getConf());
    configureOMPorts(newConf, omServiceId, omNodeId);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    newConf.set(omNodesKey, newConf.get(omNodesKey) + "," + omNodeId);

    return newConf;
  }

  /**
   * Update the configurations of the given list of OMs.
   */
  public void updateOMConfigs(int haServiceIndex, OzoneConfiguration newConf) {
    for (OzoneManager om : omhaServiceList.get(haServiceIndex)
        .getActiveServices()) {
      om.setConfiguration(newConf);
    }
  }

  /**
   * Start a new OM in Bootstrap mode. Configs (address and ports) for the new
   * OM must already be set in the newConf.
   */
  private OzoneManager bootstrapNewOM(int omHAServiceIndex,
                                      String nodeId, OzoneConfiguration newConf,
                                      boolean force)
      throws IOException, AuthenticationException {

    OzoneConfiguration config = new OzoneConfiguration(newConf);

    // For bootstrapping node, set the nodeId config also.
    config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);

    // Set metadata/DB dir base path
    String metaDirPath = clusterMetaPath + "/" + omHAServiceIndex +
        "/" + nodeId;
    config.set(OZONE_METADATA_DIRS, metaDirPath);

    OzoneManager.omInit(config);
    OzoneManager om;

    if (force) {
      om = OzoneManager.createOm(config,
          OzoneManager.StartupOption.FORCE_BOOTSTRAP);
    } else {
      om = OzoneManager.createOm(config, OzoneManager.StartupOption.BOOTSTRAP);
    }

    ExitManagerForOM exitManager = new ExitManagerForOM(this, nodeId);
    om.setExitManagerForTesting(exitManager);
    omhaServiceList.get(omHAServiceIndex).addInstance(om, false);
    startInactiveOM(omHAServiceIndex, nodeId);

    return om;
  }

  /**
   * Wait for AddOM command to execute on all OMs.
   */
  private void waitForBootstrappedNodeToBeReady(OzoneManager newOM,
        long leaderSnapshotIndex) throws Exception {
    // Wait for bootstrapped nodes to catch up with others
    GenericTestUtils.waitFor(() -> {
      try {
        if (newOM.getRatisSnapshotIndex() >= leaderSnapshotIndex) {
          return true;
        }
      } catch (IOException e) {
        return false;
      }
      return false;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  private void waitForConfigUpdateOnActiveOMs(int haServiceIndex,
      String newOMNodeId) throws Exception {
    OzoneManager newOMNode = omhaServiceList.get(haServiceIndex)
        .getServiceById(newOMNodeId);
    OzoneManagerRatisServer newOMRatisServer = newOMNode.getOmRatisServer();
    GenericTestUtils.waitFor(() -> {
      // Each existing active OM should contain the new OM in its peerList.
      // Also, the new OM should contain each existing active OM in it's OM
      // peer list and RatisServer peerList.
      for (OzoneManager om : omhaServiceList.get(haServiceIndex)
          .getActiveServices()) {
        if (!om.doesPeerExist(newOMNodeId)) {
          return false;
        }
        if (!newOMNode.doesPeerExist(om.getOMNodeId())) {
          return false;
        }
        if (!newOMRatisServer.doesPeerExist(om.getOMNodeId())) {
          return false;
        }
      }
      return true;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  public void setupExitManagerForTesting(int haServiceIndex) {
    for (OzoneManager om : omhaServiceList.get(haServiceIndex).getServices()) {
      om.setExitManagerForTesting(new ExitManagerForOM(this, om.getOMNodeId()));
    }
  }

  /**
   * MiniOzoneHAService is a helper class used for both SCM and OM HA.
   * This class keeps track of active and inactive OM/SCM services
   * @param <Type>
   */
  static class MiniOzoneHAService<Type> {
    private Map<String, Type> serviceMap;
    private List<Type> services;
    private String serviceId;
    private String serviceName;

    // Active services s denote OM/SCM services which are up and running
    private List<Type> activeServices;
    private List<Type> inactiveServices;

    // Function to extract the Id from service
    private Function<Type, String> serviceIdProvider;

    MiniOzoneHAService(String name, List<Type> activeList,
                       List<Type> inactiveList, String serviceId,
                       Function<Type, String> idProvider) {
      this.serviceName = name;
      this.serviceMap = Maps.newHashMap();
      this.serviceIdProvider = idProvider;
      if (activeList != null) {
        for (Type service : activeList) {
          this.serviceMap.put(idProvider.apply(service), service);
        }
      }
      if (inactiveList != null) {
        for (Type service : inactiveList) {
          this.serviceMap.put(idProvider.apply(service), service);
        }
      }
      this.services = new ArrayList<>(serviceMap.values());
      this.activeServices = activeList;
      this.inactiveServices = inactiveList;
      this.serviceId = serviceId;

      // If the serviceID is null, then this should be a non-HA cluster.
      if (serviceId == null) {
        Preconditions.checkArgument(services.size() <= 1);
      }
    }

    public String getServiceId() {
      return serviceId;
    }

    public List<Type> getServices() {
      return services;
    }

    public List<Type> getActiveServices() {
      return activeServices;
    }

    public boolean removeInstance(Type t) {
      boolean result =  services.remove(t);
      serviceMap.remove(serviceIdProvider.apply(t));
      activeServices.remove(t);
      inactiveServices.remove(t);
      return result;
    }

    public void addInstance(Type t, boolean isActive) {
      services.add(t);
      serviceMap.put(serviceIdProvider.apply(t), t);
      if (isActive) {
        activeServices.add(t);
      } else {
        inactiveServices.add(t);
      }
    }

    public void activate(Type t) {
      activeServices.add(t);
      inactiveServices.remove(t);
    }

    public void deactivate(Type t) {
      activeServices.remove(t);
      inactiveServices.add(t);
    }

    public boolean isServiceActive(String id) {
      return activeServices.contains(serviceMap.get(id));
    }

    public Iterator<Type> inactiveServices() {
      return new ArrayList<>(inactiveServices).iterator();
    }

    public Type getServiceByIndex(int index) {
      return this.services.get(index);
    }

    public Type getServiceById(String id) {
      return this.serviceMap.get(id);
    }

    public void startInactiveService(String id,
        CheckedConsumer<Type, IOException> serviceStarter) throws IOException {
      Type service = serviceMap.get(id);
      if (!inactiveServices.contains(service)) {
        throw new IOException(serviceName + " is already active.");
      } else {
        serviceStarter.accept(service);
        activeServices.add(service);
        inactiveServices.remove(service);
      }
    }
  }

  static class OMHAService extends MiniOzoneHAService<OzoneManager> {
    OMHAService(List<OzoneManager> activeList, List<OzoneManager> inactiveList,
                String serviceId) {
      super("OM", activeList, inactiveList, serviceId,
          OzoneManager::getOMNodeId);
    }
  }

  static class SCMHAService extends
      MiniOzoneHAService<StorageContainerManager> {
    SCMHAService(List<StorageContainerManager> activeList,
                 List<StorageContainerManager> inactiveList,
                 String serviceId) {
      super("SCM", activeList, inactiveList, serviceId,
          StorageContainerManager::getSCMNodeId);
    }
  }

  public List<StorageContainerManager> getStorageContainerManagers() {
    return new ArrayList<>(this.scmhaService.getServices());
  }

  public StorageContainerManager getStorageContainerManager() {
    return getStorageContainerManagers().get(0);
  }

  private static final class ExitManagerForOM extends ExitManager {

    private MultiOMMiniOzoneHACluster cluster;
    private String omNodeId;

    private ExitManagerForOM(MultiOMMiniOzoneHACluster cluster, String nodeId) {
      this.cluster = cluster;
      this.omNodeId = nodeId;
    }

    public void exitSystem(int haServiceIndex,
                           int status, String message, Throwable throwable,
                           Logger log) throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message, throwable);
      cluster.stopOzoneManager(haServiceIndex, omNodeId);
      throw new IOException(throwable);
    }

    public void exitSystem(int haServiceIndex,
                           int status, String message, Logger log)
        throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message);
      cluster.stopOzoneManager(haServiceIndex, omNodeId);
      throw new IOException(message);
    }
  }

}
