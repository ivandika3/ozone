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

package org.apache.hadoop.hdds.scm.block;

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.SENT;
import static org.apache.hadoop.hdds.scm.block.SCMDeletedBlockTransactionStatusManager.SCMDeleteBlocksCommandStatusManager.CmdStatus.TO_BE_SENT;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a class to manage the status of DeletedBlockTransaction,
 * the purpose of this class is to reduce the number of duplicate
 * DeletedBlockTransaction sent to the DN.
 */
public class SCMDeletedBlockTransactionStatusManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDeletedBlockTransactionStatusManager.class);
  // Maps txId to set of DNs which are successful in committing the transaction
  private final Map<Long, Set<DatanodeID>> transactionToDNsCommitMap;
  // Maps txId to its retry counts;
  private final Map<Long, Integer> transactionToRetryCountMap;
  // The access to DeletedBlocksTXTable is protected by
  // DeletedBlockLogStateManager.
  private final DeletedBlockLogStateManager deletedBlockLogStateManager;
  private final ContainerManager containerManager;
  private final ScmBlockDeletingServiceMetrics metrics;
  private final long scmCommandTimeoutMs;

  /**
   * Before the DeletedBlockTransaction is executed on DN and reported to
   * SCM, it is managed by this {@link SCMDeleteBlocksCommandStatusManager}.
   * After the DeletedBlocksTransaction in the DeleteBlocksCommand is
   * committed on the SCM, it is managed by
   * {@link SCMDeletedBlockTransactionStatusManager#transactionToDNsCommitMap}
   */
  private final SCMDeleteBlocksCommandStatusManager
      scmDeleteBlocksCommandStatusManager;

  public SCMDeletedBlockTransactionStatusManager(
      DeletedBlockLogStateManager deletedBlockLogStateManager,
      ContainerManager containerManager,
      ScmBlockDeletingServiceMetrics metrics, long scmCommandTimeoutMs) {
    // maps transaction to dns which have committed it.
    this.deletedBlockLogStateManager = deletedBlockLogStateManager;
    this.metrics = metrics;
    this.containerManager = containerManager;
    this.scmCommandTimeoutMs = scmCommandTimeoutMs;
    this.transactionToDNsCommitMap = new ConcurrentHashMap<>();
    this.transactionToRetryCountMap = new ConcurrentHashMap<>();
    this.scmDeleteBlocksCommandStatusManager =
        new SCMDeleteBlocksCommandStatusManager(metrics);
  }

  /**
   * A class that manages the status of a DeletedBlockTransaction based
   * on DeleteBlocksCommand.
   */
  protected static class SCMDeleteBlocksCommandStatusManager {
    private static final Logger LOG =
        LoggerFactory.getLogger(SCMDeleteBlocksCommandStatusManager.class);
    private final Map<DatanodeID, Map<Long, CmdStatusData>> scmCmdStatusRecord;

    private static final CmdStatus DEFAULT_STATUS = TO_BE_SENT;
    private static final Set<CmdStatus> STATUSES_REQUIRING_TIMEOUT = Collections.singleton(SENT);

    private ScmBlockDeletingServiceMetrics metrics;

    public SCMDeleteBlocksCommandStatusManager(ScmBlockDeletingServiceMetrics metrics) {
      this.scmCmdStatusRecord = new ConcurrentHashMap<>();
      this.metrics = metrics;
    }

    /**
     * Status of SCMDeleteBlocksCommand.
     */
    public enum CmdStatus {
      // The DeleteBlocksCommand has not yet been sent.
      // This is the initial status of the command after it's created.
      TO_BE_SENT,
      // If the DeleteBlocksCommand has been sent but has not been executed
      // completely by DN, the DeleteBlocksCommand's state will be SENT.
      // Note that the state of SENT includes the following possibilities.
      //   - The command was sent but not received
      //   - The command was sent and received by the DN,
      //     and is waiting to be executed.
      //   - The Command sent and being executed by DN
      SENT
    }

    protected static final class CmdStatusData {
      private final DatanodeID dnId;
      private final long scmCmdId;
      private final Set<Long> deletedBlocksTxIds;
      private Instant updateTime;
      private CmdStatus status;

      private CmdStatusData(DatanodeID dnId, long scmTxID, Set<Long> deletedBlocksTxIds) {
        this.dnId = dnId;
        this.scmCmdId = scmTxID;
        this.deletedBlocksTxIds = deletedBlocksTxIds;
        setStatus(DEFAULT_STATUS);
      }

      public Set<Long> getDeletedBlocksTxIds() {
        return Collections.unmodifiableSet(deletedBlocksTxIds);
      }

      DatanodeID getDnId() {
        return dnId;
      }

      public long getScmCmdId() {
        return scmCmdId;
      }

      public CmdStatus getStatus() {
        return status;
      }

      public void setStatus(CmdStatus status) {
        this.updateTime = Instant.now();
        this.status = status;
      }

      public Instant getUpdateTime() {
        return updateTime;
      }

      @Override
      public String toString() {
        return "ScmTxStateMachine" +
            "{dnId=" + dnId +
            ", scmTxID=" + scmCmdId +
            ", deletedBlocksTxIds=" + deletedBlocksTxIds +
            ", updateTime=" + updateTime +
            ", status=" + status +
            '}';
      }
    }

    protected static CmdStatusData createScmCmdStatusData(
        DatanodeID dnId, long scmCmdId, Set<Long> deletedBlocksTxIds) {
      return new CmdStatusData(dnId, scmCmdId, deletedBlocksTxIds);
    }

    protected void recordScmCommand(CmdStatusData statusData) {
      LOG.debug("Record ScmCommand: {}", statusData);
      scmCmdStatusRecord.computeIfAbsent(statusData.getDnId(), k ->
          new ConcurrentHashMap<>()).put(statusData.getScmCmdId(), statusData);
    }

    void onSent(DatanodeID dnId, long scmCmdId) {
      updateStatus(dnId, scmCmdId, CommandStatus.Status.PENDING);
    }

    void onDatanodeDead(DatanodeID dnId) {
      LOG.info("Clean SCMCommand record for Datanode: {}", dnId);
      scmCmdStatusRecord.remove(dnId);
    }

    void updateStatusByDNCommandStatus(DatanodeID dnId, long scmCmdId,
        CommandStatus.Status newState) {
      updateStatus(dnId, scmCmdId, newState);
    }

    protected void cleanAllTimeoutSCMCommand(long timeoutMs) {
      for (DatanodeID dnId : scmCmdStatusRecord.keySet()) {
        for (CmdStatus status : STATUSES_REQUIRING_TIMEOUT) {
          removeTimeoutScmCommand(
              dnId, getScmCommandIds(dnId, status), timeoutMs);
        }
      }
    }

    void cleanTimeoutSCMCommand(DatanodeID dnId, long timeoutMs) {
      for (CmdStatus status : STATUSES_REQUIRING_TIMEOUT) {
        removeTimeoutScmCommand(
            dnId, getScmCommandIds(dnId, status), timeoutMs);
      }
    }

    private Set<Long> getScmCommandIds(DatanodeID dnId, CmdStatus status) {
      Set<Long> scmCmdIds = new HashSet<>();
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null) {
        return scmCmdIds;
      }
      for (CmdStatusData statusData : record.values()) {
        if (statusData.getStatus().equals(status)) {
          scmCmdIds.add(statusData.getScmCmdId());
        }
      }
      return scmCmdIds;
    }

    private Instant getUpdateTime(DatanodeID dnId, long scmCmdId) {
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null || record.get(scmCmdId) == null) {
        return null;
      }
      return record.get(scmCmdId).getUpdateTime();
    }

    private void updateStatus(DatanodeID dnId, long scmCmdId,
        CommandStatus.Status newStatus) {
      Map<Long, CmdStatusData> recordForDn = scmCmdStatusRecord.get(dnId);
      if (recordForDn == null) {
        LOG.warn("Unknown Datanode: {} Scm Command ID: {} report status {}",
            dnId, scmCmdId, newStatus);
        return;
      }
      if (recordForDn.get(scmCmdId) == null) {
        // Because of the delay in the DN report, the DN sometimes report obsolete
        // Command status that are cleared by the SCM.
        LOG.debug("Unknown SCM Command ID: {} Datanode: {} report status {}",
            scmCmdId, dnId, newStatus);
        return;
      }

      boolean changed = false;
      CmdStatusData statusData = recordForDn.get(scmCmdId);
      CmdStatus oldStatus = statusData.getStatus();
      switch (newStatus) {
      case PENDING:
        if (oldStatus == TO_BE_SENT || oldStatus == SENT) {
          // TO_BE_SENT -> SENT: The DeleteBlocksCommand is sent by SCM,
          // The follow-up status has not been updated by Datanode.

          // SENT -> SENT: The DeleteBlocksCommand continues to wait to be
          // executed by Datanode.
          statusData.setStatus(SENT);
          changed = true;
        }
        break;
      case EXECUTED:
      case FAILED:
        if (oldStatus == SENT) {
          // Once the DN executes DeleteBlocksCommands, regardless of whether
          // DeleteBlocksCommands is executed successfully or not,
          // it will be deleted from record.
          // Successful DeleteBlocksCommands are recorded in
          // `transactionToDNsCommitMap`.
          removeScmCommand(dnId, scmCmdId);
          changed = true;
        }
        if (oldStatus == TO_BE_SENT) {
          // SCM receives a reply to an unsent transaction,
          // which should not normally occur.
          LOG.error("Received {} status for a command marked TO_BE_SENT. " +
                  "This indicates a potential issue in command handling. " +
                  "SCM Command ID: {}, Datanode: {}, Current status: {}",
              newStatus, scmCmdId, dnId, oldStatus);
          removeScmCommand(dnId, scmCmdId);
          changed = true;
        }
        break;
      default:
        LOG.error("Unexpected status from Datanode: {}. SCM Command ID: {} with status: {}.",
            dnId, scmCmdId, newStatus);
        break;
      }
      if (!changed) {
        LOG.warn("Cannot update illegal status for Datanode: {} SCM Command ID: {} " +
            "status {} by DN report status {}", dnId, scmCmdId, oldStatus, newStatus);
      } else {
        LOG.debug("Successful update Datanode: {} SCM Command ID: {} status From {} to" +
            " {}, DN report status {}", dnId, scmCmdId, oldStatus, statusData.getStatus(), newStatus);
      }
    }

    private void removeTimeoutScmCommand(DatanodeID dnId,
        Set<Long> scmCmdIds, long timeoutMs) {
      Instant now = Instant.now();
      for (Long scmCmdId : scmCmdIds) {
        Instant updateTime = getUpdateTime(dnId, scmCmdId);
        if (updateTime != null &&
            Duration.between(updateTime, now).toMillis() > timeoutMs) {
          CmdStatusData state = removeScmCommand(dnId, scmCmdId);
          metrics.incrDNCommandsTimeout(dnId, 1);
          LOG.warn("SCM BlockDeletionCommand {} for Datanode: {} was removed after {}ms without update",
              state, dnId, timeoutMs);
        }
      }
    }

    private CmdStatusData removeScmCommand(DatanodeID dnId, long scmCmdId) {
      Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
      if (record == null || record.get(scmCmdId) == null) {
        return null;
      }
      CmdStatusData statusData = record.remove(scmCmdId);
      LOG.debug("Remove ScmCommand {} for Datanode: {} ", statusData, dnId);
      return statusData;
    }

    Map<DatanodeID, Map<Long, CmdStatus>> getCommandStatusByTxId(Set<DatanodeID> dnIds) {
      final Map<DatanodeID, Map<Long, CmdStatus>> result = new HashMap<>(scmCmdStatusRecord.size());

      for (DatanodeID dnId : dnIds) {
        Map<Long, CmdStatusData> record = scmCmdStatusRecord.get(dnId);
        if (record == null) {
          continue;
        }
        Map<Long, CmdStatus> dnStatusMap = new HashMap<>();
        for (CmdStatusData statusData : record.values()) {
          CmdStatus status = statusData.getStatus();
          for (Long deletedBlocksTxId : statusData.getDeletedBlocksTxIds()) {
            dnStatusMap.put(deletedBlocksTxId, status);
          }
        }
        result.put(dnId, dnStatusMap);
      }

      return result;
    }

    private void clear() {
      scmCmdStatusRecord.clear();
    }

    @VisibleForTesting
    Map<DatanodeID, Map<Long, CmdStatusData>> getScmCmdStatusRecord() {
      return scmCmdStatusRecord;
    }
  }

  public void incrementRetryCount(List<Long> txIDs, long maxRetry)
      throws IOException {
    ArrayList<Long> txIDsToUpdate = new ArrayList<>();
    for (Long txID : txIDs) {
      int currentCount =
          transactionToRetryCountMap.getOrDefault(txID, 0);
      if (currentCount > maxRetry) {
        continue;
      } else {
        currentCount += 1;
        if (currentCount > maxRetry) {
          txIDsToUpdate.add(txID);
        }
        transactionToRetryCountMap.put(txID, currentCount);
      }
    }

    if (!txIDsToUpdate.isEmpty()) {
      deletedBlockLogStateManager
          .increaseRetryCountOfTransactionInDB(txIDsToUpdate);
    }
  }

  public void resetRetryCount(List<Long> txIDs) throws IOException {
    for (Long txID: txIDs) {
      transactionToRetryCountMap.computeIfPresent(txID, (key, value) -> 0);
    }
  }

  int getRetryCount(long txID) {
    return transactionToRetryCountMap.getOrDefault(txID, 0);
  }

  public void onSent(DatanodeDetails dnId, SCMCommand<?> scmCommand) {
    scmDeleteBlocksCommandStatusManager.onSent(dnId.getID(), scmCommand.getId());
  }

  Map<DatanodeID, Map<Long, CmdStatus>> getCommandStatusByTxId(Set<DatanodeID> dnIds) {
    return scmDeleteBlocksCommandStatusManager.getCommandStatusByTxId(dnIds);
  }

  void recordTransactionCreated(DatanodeID dnId, long scmCmdId, Set<Long> dnTxSet) {
    scmDeleteBlocksCommandStatusManager.recordScmCommand(
        SCMDeleteBlocksCommandStatusManager
            .createScmCmdStatusData(dnId, scmCmdId, dnTxSet));
    dnTxSet.forEach(txId -> transactionToDNsCommitMap
        .putIfAbsent(txId, new LinkedHashSet<>()));
  }

  public void clear() {
    transactionToRetryCountMap.clear();
    scmDeleteBlocksCommandStatusManager.clear();
    transactionToDNsCommitMap.clear();
  }

  public void cleanAllTimeoutSCMCommand(long timeoutMs) {
    scmDeleteBlocksCommandStatusManager.cleanAllTimeoutSCMCommand(timeoutMs);
  }

  void onDatanodeDead(DatanodeID dnId) {
    scmDeleteBlocksCommandStatusManager.onDatanodeDead(dnId);
  }

  boolean isDuplication(DatanodeID datanodeID, long tx, Map<DatanodeID, Map<Long, CmdStatus>> commandStatus) {
    if (alreadyExecuted(datanodeID, tx)) {
      return true;
    }
    return inProcessing(datanodeID, tx, commandStatus);
  }

  private boolean alreadyExecuted(DatanodeID dnId, long txId) {
    final Set<DatanodeID> dnsWithTransactionCommitted = transactionToDNsCommitMap.get(txId);
    return dnsWithTransactionCommitted != null && dnsWithTransactionCommitted
        .contains(dnId);
  }

  /**
   * Commits a transaction means to delete all footprints of a transaction
   * from the log. This method doesn't guarantee all transactions can be
   * successfully deleted, it tolerate failures and tries best efforts to.
   *  @param transactionResults - delete block transaction results.
   * @param dnId - ID of datanode which acknowledges the delete block command.
   */
  @VisibleForTesting
  public void commitTransactions(List<DeleteBlockTransactionResult> transactionResults, DatanodeID dnId) {

    ArrayList<Long> txIDsToBeDeleted = new ArrayList<>();
    for (DeleteBlockTransactionResult transactionResult :
        transactionResults) {
      if (isTransactionFailed(transactionResult)) {
        metrics.incrBlockDeletionTransactionFailureOnDatanodes();
        continue;
      }
      try {
        metrics.incrBlockDeletionTransactionSuccessOnDatanodes();
        long txID = transactionResult.getTxID();
        // set of dns which have successfully committed transaction txId.
        final Set<DatanodeID> dnsWithCommittedTxn = transactionToDNsCommitMap.get(txID);
        final ContainerID containerId = ContainerID.valueOf(
            transactionResult.getContainerID());
        if (dnsWithCommittedTxn == null) {
          // Mostly likely it's a retried delete command response.
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Transaction txId: {} commit by Datanode: {} for ContainerId: {}"
                    + " failed. Corresponding entry not found.", txID, dnId,
                containerId);
          }
          continue;
        }

        dnsWithCommittedTxn.add(dnId);
        final ContainerInfo container =
            containerManager.getContainer(containerId);
        final Set<ContainerReplica> replicas =
            containerManager.getContainerReplicas(containerId);
        // The delete entry can be safely removed from the log if all the
        // corresponding nodes commit the txn. It is required to check that
        // the nodes returned in the pipeline match the replication factor.
        if (min(replicas.size(), dnsWithCommittedTxn.size())
            >= container.getReplicationConfig().getRequiredNodes()) {
          final List<DatanodeID> containerDns = replicas.stream()
              .map(ContainerReplica::getDatanodeDetails)
              .map(DatanodeDetails::getID)
              .collect(Collectors.toList());
          if (dnsWithCommittedTxn.containsAll(containerDns)) {
            transactionToDNsCommitMap.remove(txID);
            transactionToRetryCountMap.remove(txID);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Purging txId: {} from block deletion log", txID);
            }
            txIDsToBeDeleted.add(txID);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Datanode txId: {} ContainerId: {} committed by Datanode: {}",
              txID, containerId, dnId);
        }
      } catch (IOException e) {
        LOG.warn("Could not commit delete block transaction: " +
            transactionResult.getTxID(), e);
      }
    }
    try {
      deletedBlockLogStateManager.removeTransactionsFromDB(txIDsToBeDeleted);
      metrics.incrBlockDeletionTransactionCompleted(txIDsToBeDeleted.size());
    } catch (IOException e) {
      LOG.warn("Could not commit delete block transactions: "
          + txIDsToBeDeleted, e);
    }
  }

  @VisibleForTesting
  void commitSCMCommandStatus(List<CommandStatus> deleteBlockStatus, DatanodeID dnId) {
    processSCMCommandStatus(deleteBlockStatus, dnId);
    scmDeleteBlocksCommandStatusManager.
        cleanTimeoutSCMCommand(dnId, scmCommandTimeoutMs);
  }

  static boolean inProcessing(DatanodeID dnId, long deletedBlocksTxId,
      Map<DatanodeID, Map<Long, CmdStatus>> commandStatus) {
    Map<Long, CmdStatus> deletedBlocksTxStatus = commandStatus.get(dnId);
    return deletedBlocksTxStatus != null &&
        deletedBlocksTxStatus.get(deletedBlocksTxId) != null;
  }

  private void processSCMCommandStatus(List<CommandStatus> deleteBlockStatus, DatanodeID dnID) {
    Map<Long, CommandStatus.Status> summary = new HashMap<>();
    // The CommandStatus is ordered in the report. So we can focus only on the
    // last status in the command report.
    deleteBlockStatus.forEach(cmdStatus -> {
      summary.put(cmdStatus.getCmdId(), cmdStatus.getStatus());
    });
    LOG.debug("CommandStatus {} from Datanode: {} ", summary, dnID);
    for (Map.Entry<Long, CommandStatus.Status> entry : summary.entrySet()) {
      final CommandStatus.Status status = entry.getValue();
      scmDeleteBlocksCommandStatusManager.updateStatusByDNCommandStatus(
          dnID, entry.getKey(), status);
    }
  }

  private boolean isTransactionFailed(DeleteBlockTransactionResult result) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Got block deletion ACK from datanode, TXIDs {}, " + "success {}",
          result.getTxID(), result.getSuccess());
    }
    if (!result.getSuccess()) {
      LOG.warn("Got failed ACK for TXID {}, prepare to resend the "
          + "TX in next interval", result.getTxID());
      return true;
    }
    return false;
  }

  public int getTransactionToDNsCommitMapSize() {
    return transactionToDNsCommitMap.size();
  }
}
