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
import java.util.function.LongSupplier;

import org.apache.hadoop.ipc_.AlignmentContext;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side AlignmentContext for OM follower reads.
 * 
 * This context provides the server's current state (last applied Ratis index)
 * and handles coordination for requests that need to wait for the follower
 * to catch up to the client's expected state.
 * 
 * The key mechanism is:
 * 1. When a request comes in, the client's expected state ID is extracted
 * 2. The server checks if its current state >= client's expected state
 * 3. If not, the RPC handler will requeue the call until the server catches up
 * 
 * Similar to Hadoop's GlobalStateIdContext for HDFS observer reads.
 */
public class OMServerAlignmentContext implements AlignmentContext {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMServerAlignmentContext.class);

  /**
   * Protocol name for OzoneManager client protocol.
   */
  private static final String OM_PROTOCOL_NAME =
      "org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol";

  /**
   * Supplier that provides the current last applied Ratis log index.
   * This is typically connected to OzoneManagerStateMachine.getLastAppliedTermIndex().
   */
  private final LongSupplier lastAppliedIndexSupplier;

  /**
   * Creates an OMServerAlignmentContext.
   * 
   * @param lastAppliedIndexSupplier supplier for the current last applied index
   */
  public OMServerAlignmentContext(LongSupplier lastAppliedIndexSupplier) {
    this.lastAppliedIndexSupplier = lastAppliedIndexSupplier;
  }

  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Set the current server state (last applied index) in the response
    // This allows clients to track the server's state
    long currentStateId = getLastSeenStateId();
    header.setStateId(currentStateId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting response stateId to {}", currentStateId);
    }
  }

  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Server-side: not used (client-side method)
  }

  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Server-side: not used (client-side method)
  }

  @Override
  public long receiveRequestState(RpcRequestHeaderProto header, long threshold)
      throws IOException {
    // Extract the client's expected state ID from the request
    if (header.hasStateId()) {
      long clientStateId = header.getStateId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received request with client stateId: {}, " +
            "current server stateId: {}", clientStateId, getLastSeenStateId());
      }
      return clientStateId;
    }
    // No state ID in request, return MIN_VALUE to indicate no coordination needed
    return Long.MIN_VALUE;
  }

  @Override
  public long getLastSeenStateId() {
    // Return the current last applied Ratis log index
    return lastAppliedIndexSupplier.getAsLong();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    // Only coordinate calls for the OM client protocol
    if (!OM_PROTOCOL_NAME.equals(protocolName)) {
      return false;
    }
    
    // For follower reads, we coordinate read operations.
    // The method name from protobuf is "submitRequest", so we need to
    // determine if the actual OM request is a read operation.
    // Since we can't easily determine the request type from the method name,
    // we always return true and let the handler check the actual request.
    // The Server.Handler will requeue if client stateId > server stateId.
    return "submitRequest".equals(method);
  }

  /**
   * Check if a specific OM request type should be coordinated.
   * Read-only requests to followers should be coordinated.
   * 
   * @param cmdType the OM command type
   * @return true if the request should wait for follower to catch up
   */
  public boolean isCoordinatedRequest(OzoneManagerProtocolProtos.Type cmdType) {
    // Create a minimal request to check if it's read-only
    OzoneManagerProtocolProtos.OMRequest.Builder requestBuilder =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(cmdType)
            .setClientId("check");
    return OmUtils.isReadOnly(requestBuilder.build());
  }
}

