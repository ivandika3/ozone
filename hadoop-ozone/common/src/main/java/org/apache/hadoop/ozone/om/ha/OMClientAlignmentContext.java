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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.ipc_.AlignmentContext;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side AlignmentContext for OM follower reads.
 * 
 * This context tracks the last seen state ID (Ratis log index) received from
 * OM responses. When making requests to OM followers, this state ID is sent
 * along with the request so the follower knows the minimum state it needs
 * to have applied before processing the request.
 * 
 * Similar to Hadoop's ClientGSIContext for HDFS observer reads.
 */
public class OMClientAlignmentContext implements AlignmentContext {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMClientAlignmentContext.class);

  /**
   * The last seen state ID from OM responses.
   * This is updated when receiving responses from OM.
   */
  private final AtomicLong lastSeenStateId = new AtomicLong(Long.MIN_VALUE);

  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Client-side: not used (server-side method)
  }

  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Update the lastSeenStateId if the response contains a higher state ID
    if (header.hasStateId()) {
      long responseStateId = header.getStateId();
      updateLastSeenStateId(responseStateId);
    }
  }

  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Set the client's last seen state ID in the request header
    // This tells the server (follower) what state the client expects
    long stateId = lastSeenStateId.get();
    if (stateId != Long.MIN_VALUE) {
      header.setStateId(stateId);
    }
  }

  @Override
  public long receiveRequestState(RpcRequestHeaderProto header, long threshold) {
    // Client-side: not used (server-side method)
    return 0;
  }

  @Override
  public long getLastSeenStateId() {
    return lastSeenStateId.get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    // For follower reads, coordinated calls are read operations
    // that need to wait for follower to catch up to client's state.
    // All read operations sent to followers should be coordinated.
    // The proxy provider decides whether to send to follower or leader,
    // but if sent to follower, reads should be coordinated.
    return true;
  }

  /**
   * Update the last seen state ID if the new value is higher.
   * Thread-safe update using compare-and-swap.
   * 
   * @param newStateId the new state ID from response
   */
  private void updateLastSeenStateId(long newStateId) {
    while (true) {
      long current = lastSeenStateId.get();
      if (newStateId <= current) {
        // Already have a higher or equal state ID
        break;
      }
      if (lastSeenStateId.compareAndSet(current, newStateId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated lastSeenStateId from {} to {}",
              current, newStateId);
        }
        break;
      }
      // CAS failed, retry
    }
  }

  /**
   * Update the last seen state ID to at least the given value.
   * Used after msync to establish a baseline.
   * 
   * @param stateId the state ID from msync response
   */
  public void advanceStateId(long stateId) {
    updateLastSeenStateId(stateId);
  }

  /**
   * Check if this request type should be sent to a follower.
   * Read-only operations can be sent to followers.
   * 
   * @param cmdType the OM request command type
   * @return true if the request can be handled by a follower
   */
  public static boolean isFollowerReadRequest(Type cmdType) {
    // Create a minimal OMRequest to check if it's read-only
    OMRequest.Builder requestBuilder = OMRequest.newBuilder()
        .setCmdType(cmdType)
        .setClientId("check");
    return OmUtils.isReadOnly(requestBuilder.build());
  }
}

