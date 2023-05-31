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
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.ha.CheckedConsumer;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.recon.ConfigurationProvider;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION;
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
      LoggerFactory.getLogger(MultiOMMiniOzoneHACluster.class);

  private List<OzoneConfiguration> configurations;
  private List<OMHAService> omhaServiceList;
  private List<String> clusterMetaPaths;

  private final SCMHAService scmhaService;
  private final List<HddsDatanodeService> hddsDatanodes;
  private final ReconServer reconServer;


  // Timeout for the cluster to be ready
  private int waitForClusterToBeReadyTimeout = 120000; // 2 min
  private CertificateClient caClient;

  private static final Random RANDOM = new Random();
  private static final int RATIS_RPC_TIMEOUT = 1000; // 1 second
  private static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  @SuppressWarnings("checkstyle:ParameterNumber")
  public MultiOMMiniOzoneHACluster(
      List<OMHAClusterSetup> omhaClusterSetups,
      HDDSClusterSetup hddsClusterSetup,
      ReconServer reconServer) {
    this.reconServer = reconServer;
    this.configurations = new ArrayList<>();
    this.omhaServiceList = new ArrayList<>();
    this.clusterMetaPaths = new ArrayList<>();

    for (OMHAClusterSetup omhaClusterSetup: omhaClusterSetups) {
      omhaServiceList.add(new OMHAService(
              omhaClusterSetup.getActiveOMList(),
              omhaClusterSetup.getInactiveOMList(),
              omhaClusterSetup.getOmServiceId()
          )
      );
      configurations.add(omhaClusterSetup.getConfiguration());
      clusterMetaPaths.add(omhaClusterSetup.getClusterPath());
    }
    this.scmhaService = new SCMHAService(
        hddsClusterSetup.getActiveSCMList(),
        hddsClusterSetup.getInactiveSCMList(),
        hddsClusterSetup.getScmServiceId()
    );
    this.hddsDatanodes = hddsClusterSetup.getHddsDatanodes();
  }


  public OzoneConfiguration getConf(int clusterIndex) {
    return configurations.get(clusterIndex);
  }

  public void setConf(int clusterIndex, OzoneConfiguration newConf) {
    this.configurations.set(clusterIndex, newConf);
  }
  
  public String getOMServiceId(int clusterIndex) {
    return omhaServiceList.get(clusterIndex).getServiceId();
  }

  
  public String getSCMServiceId() {
    return scmhaService.getServiceId();
  }

  
  public OzoneClient getRpcClient(int clusterIndex) throws IOException {
    String omServiceId = omhaServiceList.get(clusterIndex).getServiceId();
    if (omServiceId == null) {
      // Non-HA cluster.
      return OzoneClientFactory.getRpcClient(getConf(clusterIndex));
    } else {
      // HA cluster
      return OzoneClientFactory.getRpcClient(omServiceId,
          getConf(clusterIndex));
    }
  }

  public boolean isOMActive(int clusterIndex, String omNodeId) {
    return omhaServiceList.get(clusterIndex).isServiceActive(omNodeId);
  }

  public boolean isSCMActive(String scmNodeId) {
    return scmhaService.isServiceActive(scmNodeId);
  }

  public StorageContainerManager getSCM(String scmNodeId) {
    return this.scmhaService.getServiceById(scmNodeId);
  }

  public OzoneManager getOzoneManager(int clusterIndex, int serviceIndex) {
    return this.omhaServiceList.get(clusterIndex)
        .getServiceByIndex(serviceIndex);
  }

  public OzoneManager getOzoneManager(int clusterIndex, String omNodeId) {
    return this.omhaServiceList.get(clusterIndex)
        .getServiceById(omNodeId);
  }

  public List<OzoneManager> getOzoneManagersList(int clusterIndex) {
    return this.omhaServiceList.get(clusterIndex).getServices();
  }

  public List<StorageContainerManager> getStorageContainerManagersList() {
    return scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager(int index) {
    return this.scmhaService.getServiceByIndex(index);
  }

  public OzoneManager getOMLeader(int clusterIndex,
      boolean waitForLeaderElection)
      throws TimeoutException, InterruptedException {
    if (waitForLeaderElection) {
      final OzoneManager[] om = new OzoneManager[1];
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          om[0] = getOMLeader(clusterIndex);
          return om[0] != null;
        }
      }, 200, waitForClusterToBeReadyTimeout);
      return om[0];
    } else {
      return getOMLeader(clusterIndex);
    }
  }

  /**
   * Get OzoneManager leader object.
   * @return OzoneManager object, null if there isn't one or more than one
   */
  public OzoneManager getOMLeader(int clusterIndex) {
    OzoneManager res = null;
    for (OzoneManager ozoneManager : this.omhaServiceList.get(clusterIndex)
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
  public void startInactiveOM(int clusterIndex, String omNodeID)
      throws IOException {
    omhaServiceList.get(clusterIndex).startInactiveService(omNodeID,
        OzoneManager::start);
  }

  /**
   * Start a previously inactive SCM.
   */
  public void startInactiveSCM(String scmNodeId) throws IOException {
    scmhaService
        .startInactiveService(scmNodeId, StorageContainerManager::start);
  }

  public void startHddsDatanodes() {
    hddsDatanodes.forEach((datanode) -> {
      datanode.setCertificateClient(getCAClient());
      datanode.start();
    });
  }

  private CertificateClient getCAClient() {
    return this.caClient;
  }

  
  public void restartOzoneManager(int clusterIndex) throws IOException {
    for (OzoneManager ozoneManager : this.omhaServiceList.get(clusterIndex)
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
    scmhaService.deactivate(scm);
  }

  public void restartStorageContainerManager(
      StorageContainerManager scm, boolean waitForSCM)
      throws IOException, TimeoutException,
      InterruptedException, AuthenticationException {
    LOG.info("Restarting SCM in cluster " + this.getClass());
    OzoneConfiguration scmConf = scm.getConfiguration();
    shutdownStorageContainerManager(scm);
    scm.join();
    scm = TestUtils.getScmSimple(scmConf);
    scmhaService.activate(scm);
    scm.start();
    if (waitForSCM) {
      waitForClusterToBeReady();
    }
  }

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

  public String getClusterId() throws IOException {
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

  public void shutdown() {
    try {
      LOG.info("Shutting down the Mini Ozone Cluster");
      File baseDir = new File(GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" +
              getClusterId()));
      stop();
      FileUtils.deleteDirectory(baseDir);
      for (OzoneConfiguration conf: configurations) {
        ContainerCache.getInstance(conf).shutdownCache();
      }
      DefaultMetricsSystem.shutdown();
    } catch (IOException e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }


  public void stop() {
    LOG.info("Stopping the Mini Ozone Cluster");
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

  private static void stopSCM(StorageContainerManager scm) {
    if (scm != null) {
      LOG.info("Stopping the StorageContainerManager");
      scm.stop();
      scm.join();
    }
  }

  private static void stopOM(OzoneManager om) {
    if (om != null) {
      LOG.info("Stopping the OzoneManager");
      om.stop();
      om.join();
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

  public void stopOzoneManager(int clusterIndex, int omNodeIndex) {
    OzoneManager om = omhaServiceList.get(clusterIndex).
        getServiceByIndex(omNodeIndex);
    om.stop();
    om.join();
    omhaServiceList.get(clusterIndex).deactivate(om);
  }

  public void stopOzoneManager(int clusterIndex, String omNodeId) {
    LOG.info("Stopping Ozone Manager with OM Node ID: {}", omNodeId);
    OzoneManager om = omhaServiceList.get(clusterIndex)
        .getServiceById(omNodeId);
    om.stop();
    om.join();
    omhaServiceList.get(clusterIndex).deactivate(om);
  }

  /**
   * Encapsulate OM HA Cluster information.
   */
  public static class OMHAClusterSetup {
    private String omServiceId;
    private List<OzoneManager> activeOMList;
    private List<OzoneManager> inactiveOMList;
    private String clusterPath;
    private OzoneConfiguration configuration;

    public OMHAClusterSetup(String omServiceId,
                            List<OzoneManager> activeOMList,
                            List<OzoneManager> inactiveOMList,
                            String clusterPath,
                            OzoneConfiguration conf) {
      this.omServiceId = omServiceId;
      this.activeOMList = activeOMList;
      this.inactiveOMList = inactiveOMList;
      this.clusterPath = clusterPath;
      this.configuration = conf;
    }

    public String getOmServiceId() {
      return omServiceId;
    }

    public List<OzoneManager> getActiveOMList() {
      return activeOMList;
    }

    public List<OzoneManager> getInactiveOMList() {
      return inactiveOMList;
    }

    public String getClusterPath() {
      return clusterPath;
    }

    public OzoneConfiguration getConfiguration() {
      return configuration;
    }
  }

  /**
   * Encapsulate HDDS Cluster Setup.
   */
  public static class HDDSClusterSetup {
    private String scmServiceId;
    private List<StorageContainerManager> activeSCMList;
    private List<StorageContainerManager> inactiveSCMList;
    private List<HddsDatanodeService> hddsDatanodes;

    public HDDSClusterSetup(String scmServiceId,
                            List<StorageContainerManager> activeSCMList,
                            List<StorageContainerManager> inactiveSCMList,
                            List<HddsDatanodeService> hddsDatanodes) {
      this.scmServiceId = scmServiceId;
      this.activeSCMList = activeSCMList;
      this.inactiveSCMList = inactiveSCMList;
      this.hddsDatanodes = hddsDatanodes;
    }

    public String getScmServiceId() {
      return scmServiceId;
    }

    public List<StorageContainerManager> getActiveSCMList() {
      return activeSCMList;
    }

    public List<StorageContainerManager> getInactiveSCMList() {
      return inactiveSCMList;
    }

    public List<HddsDatanodeService> getHddsDatanodes() {
      return hddsDatanodes;
    }
  }

  /**
   * Builder for configuring the MultiOMMiniOzoneHACluster to run.
   */
  public static class Builder {

    protected static final int DEFAULT_HB_INTERVAL_MS = 1000;
    protected static final int DEFAULT_HB_PROCESSOR_INTERVAL_MS = 100;
    protected static final int ACTIVE_OMS_NOT_SET = -1;
    protected static final int ACTIVE_SCMS_NOT_SET = -1;
    protected static final int DEFAULT_PIPELIME_LIMIT = 3;
    protected static final int DEFAULT_RATIS_RPC_TIMEOUT_SEC = 1;

    private OzoneConfiguration conf;
    private String path;

    private String clusterId;
    private List<String> omServiceIds;
    private int numOfOMClusters;
    private int numOfOMsPerCluster;
    private int numOfActiveOMsPerCluster = ACTIVE_OMS_NOT_SET;

    private String scmServiceId;
    private int numOfSCMs;
    private int numOfActiveSCMs = ACTIVE_SCMS_NOT_SET;

    private Optional<Boolean> enableTrace = Optional.of(false);
    private Optional<Integer> hbInterval = Optional.empty();
    private Optional<Integer> hbProcessorInterval = Optional.empty();
    private Optional<String> scmId = Optional.empty();
    private Optional<String> omId = Optional.empty();

    private Boolean randomContainerPort = true;
    private Optional<String> datanodeReservedSpace = Optional.empty();
    private Optional<Integer> chunkSize = Optional.empty();
    private OptionalInt streamBufferSize = OptionalInt.empty();
    private Optional<Long> streamBufferFlushSize = Optional.empty();
    private Optional<Long> streamBufferMaxSize = Optional.empty();
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
    private int pipelineNumLimit = DEFAULT_PIPELIME_LIMIT;

    private static final String OM_NODE_ID_PREFIX = "omNode-";
    private List<List<OzoneManager>> activeOMs = new ArrayList<>();
    private List<List<OzoneManager>> inactiveOMs = new ArrayList<>();

    private static final String SCM_NODE_ID_PREFIX = "scmNode-";
    private List<StorageContainerManager> activeSCMs = new ArrayList<>();
    private List<StorageContainerManager> inactiveSCMs = new ArrayList<>();


    protected void configureRecon() throws IOException {
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

      conf.set(OZONE_RECON_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.set(OZONE_RECON_DATANODE_ADDRESS_KEY, "0.0.0.0:0");

      ConfigurationProvider.setConfiguration(conf);
    }


    /**
     * Initializes the configuration required for starting
     * MultiOMMiniOzoneHACluster.
     *
     * @throws IOException
     */
    protected void initializeConfiguration() throws IOException {
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
      conf.setFromObject(clientConfig);

      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
          chunkSize.get(), streamBufferSizeUnit.get());

      conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, blockSize.get(),
          streamBufferSizeUnit.get());
      // MultiOMMiniOzoneHACluster should have global pipeline upper limit.
      conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
          pipelineNumLimit >= DEFAULT_PIPELIME_LIMIT ?
              pipelineNumLimit : DEFAULT_PIPELIME_LIMIT);
      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          DEFAULT_RATIS_RPC_TIMEOUT_SEC, TimeUnit.SECONDS);
      SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
      // default max retry timeout set to 30s
      scmClientConfig.setMaxRetryTimeout(30 * 1000);
      conf.setFromObject(scmClientConfig);
      // In this way safemode exit will happen only when atleast we have one
      // pipeline.
      conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
          numOfDatanodes >=3 ? 3 : 1);
      configureTrace();
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
        GenericTestUtils.setRootLogLevel(Level.TRACE);
      }
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }

    
    public MultiOMMiniOzoneHACluster build() throws IOException {
      if (numOfOMClusters <= 0) {
        throw new IllegalArgumentException(
            "Number of OM clusters must be more than zero.");
      }

      if (numOfActiveOMsPerCluster > numOfOMsPerCluster) {
        throw new IllegalArgumentException("Number of active OMs per cluster" +
            " cannot be more than the total number of OMs per cluster");
      }

      // If num of ActiveOMs is not set, set it to numOfOMs.
      if (numOfActiveOMsPerCluster == ACTIVE_OMS_NOT_SET) {
        numOfActiveOMsPerCluster = numOfOMsPerCluster;
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
      initializeConfiguration();
      initOMRatisConf();
      StorageContainerManager scm;
      ReconServer reconServer = null;
      try {
        createSCMService();
        createOMService();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException(
            "Unable to build MiniMultiOMOzoneHACluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          activeSCMs, reconServer);

      List<OMHAClusterSetup> omhaClusterSetups = new ArrayList<>();
      for (int i = 0; i < omServiceIds.size(); i++) {
        OMHAClusterSetup omhaClusterSetup = new OMHAClusterSetup(
            omServiceIds.get(i),
            activeOMs.get(i),
            inactiveOMs.get(i),
            path,
            conf
        );
        omhaClusterSetups.add(omhaClusterSetup);
      }

      HDDSClusterSetup hddsClusterSetup = new HDDSClusterSetup(
          scmServiceId,
          activeSCMs,
          inactiveSCMs,
          hddsDatanodes
      );
      MultiOMMiniOzoneHACluster cluster = new MultiOMMiniOzoneHACluster(
          omhaClusterSetups,
          hddsClusterSetup,
          reconServer
      );

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }

    protected String getSCMAddresses(List<StorageContainerManager> scms) {
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

    protected List<HddsDatanodeService> createHddsDatanodes(
        List<StorageContainerManager> scms, ReconServer reconServer)
        throws IOException {
      configureHddsDatanodes();
      String scmAddress = getSCMAddresses(scms);
      String[] args = new String[] {};
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, scmAddress);
      List<HddsDatanodeService> hddsDatanodes = new ArrayList<>();
      for (int i = 0; i < numOfDatanodes; i++) {
        OzoneConfiguration dnConf = new OzoneConfiguration(conf);
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

    private void configureHddsDatanodes() {
      conf.set(ScmConfigKeys.HDDS_REST_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.set(HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
          randomContainerPort);
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
          randomContainerPort);

      conf.setFromObject(new ReplicationConfig().setPort(0));
    }

    protected void configureSCM() {
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
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


    protected void initOMRatisConf() {
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

    private String getOmNodeId(int clusterIndex, int nodeId) {
      return OM_NODE_ID_PREFIX + clusterIndex + "-" + nodeId;
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected List<OzoneManager> createOMService() throws IOException,
        AuthenticationException {

      List<OzoneManager> omList = Lists.newArrayList();

      int retryCount = 0;
      int basePort;

      while (true) {
        try {
          initCommonOMHAConfig();
          for (int i = 0; i < numOfOMClusters; i++) {
            activeOMs.add(new ArrayList<>());
            inactiveOMs.add(new ArrayList<>());

            basePort = 10000 + RANDOM.nextInt(1000) * 4;
            initOMHAConfig(basePort, i);

            for (int j = 1; j <= numOfOMsPerCluster; j++) {
              // Set nodeId
              String nodeId = getOmNodeId(i, j);
              OzoneConfiguration config = new OzoneConfiguration(conf);
              config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);
              // Set the OM http(s) address to null so that the cluster picks
              // up the address set with service ID and node ID in initHAConfig
              config.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "");
              config.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "");

              // Set metadata/DB dir base path
              String metaDirPath = path + "/" + nodeId;
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

              if (j <= numOfOMsPerCluster) {
                om.start();
                activeOMs.get(i).add(om);
                LOG.info("Started OzoneManager {} RPC server at {}", nodeId,
                    om.getOmRpcServerAddr());
              } else {
                inactiveOMs.get(i).add(om);
                LOG.info("Intialized OzoneManager at {}. This OM is currently "
                    + "inactive (not running).", om.getOmRpcServerAddr());
              }
            }
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
              retryCount);
        }
      }
      return omList;
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected List<StorageContainerManager> createSCMService()
        throws IOException, AuthenticationException {
      List<StorageContainerManager> scmList = Lists.newArrayList();

      int retryCount = 0;
      int basePort = 12000;

      while (true) {
        try {
          basePort = 12000 + RANDOM.nextInt(1000) * 4;
          initSCMHAConfig(basePort);

          for (int i = 1; i<= numOfSCMs; i++) {
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
            StorageContainerManager scm = TestUtils.getScmSimple(scmConfig);
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
                  scm.getClientProtocolServer());
            } else {
              inactiveSCMs.add(scm);
              LOG.info("Intialized SCM at {}. This SCM is currently "
                  + "inactive (not running).", scm.getClientProtocolServer());
            }
          }


          break;
        } catch (BindException e) {
          for (StorageContainerManager scm : scmList) {
            scm.stop();
            scm.join();
            LOG.info("Stopping StorageContainerManager server at {}",
                scm.getClientProtocolServer());
          }
          scmList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount);
        }
      }
      return scmList;
    }

    /**
     * Initialize HA related configurations.
     */
    private void initSCMHAConfig(int basePort) throws IOException {
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

      int port = basePort;

      for (int i = 1; i <= numOfSCMs; i++, port+=10) {
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

        conf.set(scmAddrKey, "127.0.0.1");
        conf.set(scmHttpAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(scmHttpsAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(scmRatisPortKey, port + 4);
        //conf.setInt("ozone.scm.ha.ratis.bind.port", port + 4);
        conf.set(dnPortKey, "127.0.0.1:" + (port + 5));
        conf.set(blockClientKey, "127.0.0.1:" + (port + 6));
        conf.set(ssClientKey, "127.0.0.1:" + (port + 7));
        conf.setInt(scmGrpcPortKey, port + 8);
        scmNames.append(",").append("localhost:" + (port + 5));
        conf.set(ScmConfigKeys.
            OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:" + (port + 6));
      }

      conf.set(scmNodesKey, scmNodesKeyValue.substring(1));
      conf.set(ScmConfigKeys.OZONE_SCM_NAMES, scmNames.substring(1));
    }

    /**
     * Initialize HA related configurations.
     */
    private void initOMHAConfig(int basePort, int omClusterIndex)
        throws IOException {
      String omServiceId = omServiceIds.get(omClusterIndex);
      conf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, omServiceId);
      String omNodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      List<String> omNodeIds = new ArrayList<>();

      int port = basePort;

      for (int i = 1; i <= numOfOMsPerCluster; i++, port+=6) {
        String omNodeId = getOmNodeId(omClusterIndex, i);
        omNodeIds.add(omNodeId);

        String omAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpsAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
        String omRatisPortKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

        conf.set(omAddrKey, "127.0.0.1:" + port);
        conf.set(omHttpAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(omHttpsAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(omRatisPortKey, port + 4);
      }

      conf.set(omNodesKey, String.join(",", omNodeIds));
    }

    private void initCommonOMHAConfig() {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY,
          String.join(",", omServiceIds));
    }

    public Builder(OzoneConfiguration conf) {
      this.conf = conf;
      setClusterId(UUID.randomUUID().toString());
    }


    public Builder setConf(OzoneConfiguration config) {
      this.conf = config;
      return this;
    }

    /**
     * Sets the cluster Id.
     *
     * @param id cluster Id
     *
     * @return Builder
     */
    public Builder setClusterId(String id) {
      clusterId = id;
      path = GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" + clusterId);
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
     * Sets the SCM id.
     *
     * @param id SCM Id
     *
     * @return Builder
     */
    public Builder setScmId(String id) {
      scmId = Optional.of(id);
      return this;
    }

    /**
     * Sets the OM id.
     *
     * @param id OM Id
     *
     * @return Builder
     */
    public Builder setOmId(String id) {
      omId = Optional.of(id);
      return this;
    }

    /**
     * If set to true container service will be started in a random port.
     *
     * @param randomPort enable random port
     *
     * @return Builder
     */
    public Builder setRandomContainerPort(boolean randomPort) {
      randomContainerPort = randomPort;
      return this;
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MiniOzoneCluster.
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
     * @return {@link MiniOzoneCluster} Builder
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

    /**
     * Sets the block size for stream buffer.
     *
     * @return Builder
     */
    public Builder setBlockSize(long size) {
      blockSize = Optional.of(size);
      return this;
    }

    public Builder setNumOfOMClusters(int numOMClusters) {
      this.numOfOMClusters = numOMClusters;
      return this;
    }

    public Builder setNumOfOMPerCluster(int numOMs) {
      this.numOfOMsPerCluster = numOMs;
      return this;
    }

    public Builder setNumOfActiveOMs(int numActiveOMs) {
      this.numOfActiveOMsPerCluster = numActiveOMs;
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

    public List<OzoneManager> getActiveOMs(int clusterIndex) {
      return activeOMs.get(clusterIndex);
    }

    public List<OzoneManager> getInactiveOMs(int clusterIndex) {
      return inactiveOMs.get(clusterIndex);
    }
  }

  /**
   * Bootstrap new OM by updating existing OM configs.
   */
  public void bootstrapOzoneManager(int clusterIndex, String omNodeId)
      throws Exception {
    bootstrapOzoneManager(clusterIndex, omNodeId, true, false);
  }

  /**
   * Bootstrap new OM and add to existing OM HA service ring.
   * @param omNodeId nodeId of new OM
   * @param updateConfigs if true, update the old OM configs with new node
   *                      information
   * @param force if true, start new OM with FORCE_BOOTSTRAP option.
   *              Otherwise, start new OM with BOOTSTRAP option.
   */
  public void bootstrapOzoneManager(int clusterIndex, String omNodeId,
      boolean updateConfigs, boolean force) throws Exception {

    // Set testReloadConfigFlag to true so that
    // OzoneManager#reloadConfiguration does not reload config as it will
    // return the default configurations.
    OzoneManager.setTestReloadConfigFlag(true);

    int retryCount = 0;
    OzoneManager om = null;

    OzoneManager omLeader = getOMLeader(clusterIndex, true);
    long leaderSnapshotIndex = omLeader.getRatisSnapshotIndex();

    while (true) {
      try {
        List<Integer> portSet = getFreePortList(4);
        OzoneConfiguration newConf = addNewOMToConfig(clusterIndex,
            getOMServiceId(clusterIndex), omNodeId, portSet);

        if (updateConfigs) {
          updateOMConfigs(clusterIndex, newConf);
        }

        om = bootstrapNewOM(clusterIndex, omNodeId, newConf, force);

        LOG.info("Bootstrapped OzoneManager {} RPC server at {}", omNodeId,
            om.getOmRpcServerAddr());

        // Add new OMs to cluster's in memory map and update existing OMs conf.
        setConf(clusterIndex, newConf);

        break;
      } catch (IOException e) {
        // Existing OM config could have been updated with new conf. Reset it.
        for (OzoneManager existingOM : omhaServiceList.get(clusterIndex)
            .getServices()) {
          existingOM.setConfiguration(getConf(clusterIndex));
        }
        if (e instanceof BindException ||
            e.getCause() instanceof BindException) {
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount);
        } else {
          throw e;
        }
      }
    }

    waitForBootstrappedNodeToBeReady(om, leaderSnapshotIndex);
    if (updateConfigs) {
      waitForConfigUpdateOnActiveOMs(clusterIndex, omNodeId);
    }
  }

  /**
   * Set the configs for new OMs.
   */
  private OzoneConfiguration addNewOMToConfig(int clusterIndex,
      String omServiceId, String omNodeId, List<Integer> portList) {

    OzoneConfiguration newConf = new OzoneConfiguration(getConf(clusterIndex));
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    StringBuilder omNodesKeyValue = new StringBuilder();
    omNodesKeyValue.append(newConf.get(omNodesKey))
        .append(",").append(omNodeId);

    String omAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpsAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
    String omRatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

    newConf.set(omAddrKey, "127.0.0.1:" + portList.get(0));
    newConf.set(omHttpAddrKey, "127.0.0.1:" + portList.get(1));
    newConf.set(omHttpsAddrKey, "127.0.0.1:" + portList.get(2));
    newConf.setInt(omRatisPortKey, portList.get(3));

    newConf.set(omNodesKey, omNodesKeyValue.toString());

    return newConf;
  }

  /**
   * Update the configurations of the given list of OMs.
   */
  public void updateOMConfigs(int clusterIndex, OzoneConfiguration newConf) {
    for (OzoneManager om : omhaServiceList.get(clusterIndex)
        .getActiveServices()) {
      om.setConfiguration(newConf);
    }
  }

  /**
   * Start a new OM in Bootstrap mode. Configs (address and ports) for the new
   * OM must already be set in the newConf.
   */
  private OzoneManager bootstrapNewOM(int clusterIndex,
                                      String nodeId, OzoneConfiguration newConf,
                                      boolean force)
      throws IOException, AuthenticationException {

    OzoneConfiguration config = new OzoneConfiguration(newConf);

    // For bootstrapping node, set the nodeId config also.
    config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);

    // Set metadata/DB dir base path
    String metaDirPath = clusterMetaPaths.get(clusterIndex) + "/" + nodeId;
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
    omhaServiceList.get(clusterIndex).addInstance(om, false);

    om.start();
    omhaServiceList.get(clusterIndex).activate(om);

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

  private void waitForConfigUpdateOnActiveOMs(int clusterIndex,
      String newOMNodeId) throws Exception {
    OzoneManager newOMNode = omhaServiceList.get(clusterIndex)
        .getServiceById(newOMNodeId);
    OzoneManagerRatisServer newOMRatisServer = newOMNode.getOmRatisServer();
    GenericTestUtils.waitFor(() -> {
      // Each existing active OM should contain the new OM in its peerList.
      // Also, the new OM should contain each existing active OM in it's OM
      // peer list and RatisServer peerList.
      for (OzoneManager om : omhaServiceList.get(clusterIndex)
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

  public void setupExitManagerForTesting(int clusterIndex) {
    for (OzoneManager om : omhaServiceList.get(clusterIndex).getServices()) {
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
      return services.remove(t);
    }

    public void addInstance(Type t, boolean isActive) {
      services.add(t);
      serviceMap.put(serviceIdProvider.apply(t), t);
      if (isActive) {
        activeServices.add(t);
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
        serviceStarter.execute(service);
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
          StorageContainerManager::getScmId);
    }
  }

  public List<StorageContainerManager> getStorageContainerManagers() {
    return this.scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager() {
    return getStorageContainerManagers().get(0);
  }

  private List<Integer> getFreePortList(int size) {
    return org.apache.ratis.util.NetUtils.createLocalServerAddress(size)
        .stream()
        .map(inetSocketAddress -> inetSocketAddress.getPort())
        .collect(Collectors.toList());
  }

  private static final class ExitManagerForOM extends ExitManager {

    private MultiOMMiniOzoneHACluster cluster;
    private String omNodeId;

    private ExitManagerForOM(MultiOMMiniOzoneHACluster cluster, String nodeId) {
      this.cluster = cluster;
      this.omNodeId = nodeId;
    }

    
    public void exitSystem(int clusterIndex, int status, String message,
                           Throwable throwable, Logger log) throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message, throwable);
      cluster.stopOzoneManager(clusterIndex, omNodeId);
      throw new IOException(throwable);
    }

    
    public void exitSystem(int clusterIndex, int status, String message,
                           Logger log) throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message);
      cluster.stopOzoneManager(clusterIndex, omNodeId);
      throw new IOException(message);
    }
  }
}
