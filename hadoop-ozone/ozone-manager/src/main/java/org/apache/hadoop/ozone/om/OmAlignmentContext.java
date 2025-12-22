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

package org.apache.hadoop.ozone.om;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc_.AlignmentContext;
import org.apache.hadoop.ipc_.RetriableException;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the server side implementation responsible for passing
 * state alignment info to clients.
 * <p>
 * Unlike HDFS's ClientNamenodeProtocol that has a RPC method for each
 * distinct call, OM (OzoneManagerService) only contains a single RPC method
 * (i.e. submitRequest(OMRequest)). Therefore, we need to query the OMRequest
 * to get the OMRequest parameter from the RPC method get the corresponding cmdType
 * to check whether we can the request to be run on non-leader OMs.
 */
public class OmAlignmentContext implements AlignmentContext {

  /**
   * Only OzoneManager client protocol can be a coordinated call.
   * See OzoneManagerProtocolPB @ProtocolInfo annotation for the protocol name.
   */
  private static final String OM_PROTOCOL_NAME = OzoneManagerProtocol.class.getCanonicalName();

  /**
   * OzoneManager client protocol only supports submitRequest RPC call.
   */
  private static final String OM_PROTOCOL_METHOD = "submitRequest";

  private static final Logger LOG =
      LoggerFactory.getLogger(OmAlignmentContext.class);
  /**
   * Estimated number of journal transactions a typical OM can execute
   * per second. The number is used to estimate how long a client's
   * RPC request will wait in the call queue before the Observer catches up
   * with its state id.
   */
  private static final long ESTIMATED_TRANSACTIONS_PER_SECOND = 10000L;

  /**
   * The client wait time on an RPC request is composed of
   * the server execution time plus the communication time.
   * This is an expected fraction of the total wait time spent on
   * server execution.
   */
  private static final float ESTIMATED_SERVER_TIME_MULTIPLIER = 0.8f;

  private final OzoneManager ozoneManager;

  /**
   * Server side constructor.
   * @param ozoneManager server side state provider
   */
  OmAlignmentContext(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  /**
   * Server side implementation for providing state alignment info in responses.
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Using getCorrectLastAppliedOrWrittenTxId will acquire the lock on
    // FSEditLog. This is needed so that ANN will return the correct state id
    // it currently has. But this may not be necessary for Observer, may want
    // revisit for optimization. Same goes to receiveRequestState.
    header.setStateId(getLastSeenStateId());
  }

  /**
   * Server side implementation only provides state alignment info.
   * It does not receive state alignment info therefore this does nothing.
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }

  /**
   * Server side implementation only receives state alignment info.
   * It does not build RPC requests therefore this does nothing.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Server-side implementation for processing state alignment info in
   * requests.
   * For Follower/Listener it compares the client and the server states and determines
   * if it makes sense to wait until the server catches up with the client
   * state. If not the server throws RetriableException so that the client
   * could retry the call according to the retry policy with another Follower/Listener
   * or the Leader.
   *
   * @param header The RPC request header.
   * @param clientWaitTime time in milliseconds indicating how long client
   *    waits for the server response. It is used to verify if the client's
   *    state is too far ahead of the server's
   * @return the minimum of the state ids of the client or the server.
   * @throws RetriableException if Observer is too far behind.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,
      long clientWaitTime) throws IOException {
    RaftPeerRole selfRole;
    if (ozoneManager.getOmRatisServer() == null) {
      selfRole = RaftPeerRole.LEADER;
    } else {
      selfRole = ozoneManager.getSelfRole(ozoneManager.getOmRatisServer().getLeaderId());
    }

    if (!header.hasStateId() && !RaftPeerRole.LEADER.equals(selfRole)) {
      // This could happen if client configured with non-follower proxy provider
      // (e.g., ConfiguredFailoverProxyProvider) is accessing a cluster with.
      // In this case, we should let the client failover to the
      // leader node, rather than potentially serving stale result (client
      // stateId is 0 if not set).
      throw new IOException("Node received request without "
          + "stateId. This mostly likely is because client is not configured "
          + "with ObserverReadProxyProvider");
    }
    long serverStateId = getLastSeenStateId();
    long clientStateId = header.getStateId();
    LOG.trace("Client State ID= {} and Server State ID= {}",
        clientStateId, serverStateId);

    if (clientStateId > serverStateId &&
        RaftPeerRole.LEADER.equals(selfRole)) {
      LOG.warn("The client stateId: {} is greater than "
              + "the server stateId: {} This is unexpected. "
              + "Resetting client stateId to server stateId",
          clientStateId, serverStateId);
      return serverStateId;
    }
    if ((RaftPeerRole.FOLLOWER.equals(selfRole) || RaftPeerRole.LISTENER.equals(selfRole)) &&
        clientStateId - serverStateId >
            ESTIMATED_TRANSACTIONS_PER_SECOND
                * TimeUnit.MILLISECONDS.toSeconds(clientWaitTime)
                * ESTIMATED_SERVER_TIME_MULTIPLIER) {
      throw new RetriableException(
          "Follower / Listener Node is too far behind: serverStateId = "
              + serverStateId + " clientStateId = " + clientStateId);
    }
    return clientStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return ozoneManager.getOmRatisServer().getLastAppliedTermIndex().getIndex();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName, Message payload) {
    if (!protocolName.equals(OM_PROTOCOL_NAME) ||
        !(methodName.equals(OM_PROTOCOL_METHOD)) ||
        !(payload instanceof OMRequest)) {
      return false;
    }

    OMRequest omRequest = (OMRequest) payload;

    return OmUtils.isReadOnly(omRequest);
  }

}
