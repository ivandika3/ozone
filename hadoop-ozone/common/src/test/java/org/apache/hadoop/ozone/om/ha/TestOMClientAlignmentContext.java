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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMClientAlignmentContext - client-side state tracking for
 * follower reads.
 */
public class TestOMClientAlignmentContext {

  private OMClientAlignmentContext alignmentContext;

  @BeforeEach
  public void setUp() {
    alignmentContext = new OMClientAlignmentContext();
  }

  @Test
  public void testInitialStateId() {
    // Initially, state ID should be Long.MIN_VALUE
    assertEquals(Long.MIN_VALUE, alignmentContext.getLastSeenStateId());
  }

  @Test
  public void testReceiveResponseState() {
    // Simulate receiving a response with stateId = 100
    RpcResponseHeaderProto header = RpcResponseHeaderProto.newBuilder()
        .setStateId(100)
        .setCallId(1)
        .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();

    alignmentContext.receiveResponseState(header);
    assertEquals(100, alignmentContext.getLastSeenStateId());
  }

  @Test
  public void testReceiveResponseStateUpdatesOnlyIfHigher() {
    // First response with stateId = 100
    RpcResponseHeaderProto header1 = RpcResponseHeaderProto.newBuilder()
        .setStateId(100)
        .setCallId(1)
        .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();
    alignmentContext.receiveResponseState(header1);
    assertEquals(100, alignmentContext.getLastSeenStateId());

    // Second response with lower stateId = 50 should not update
    RpcResponseHeaderProto header2 = RpcResponseHeaderProto.newBuilder()
        .setStateId(50)
        .setCallId(2)
        .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();
    alignmentContext.receiveResponseState(header2);
    assertEquals(100, alignmentContext.getLastSeenStateId());

    // Third response with higher stateId = 150 should update
    RpcResponseHeaderProto header3 = RpcResponseHeaderProto.newBuilder()
        .setStateId(150)
        .setCallId(3)
        .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();
    alignmentContext.receiveResponseState(header3);
    assertEquals(150, alignmentContext.getLastSeenStateId());
  }

  @Test
  public void testUpdateRequestState() {
    // First set a state ID by receiving a response
    RpcResponseHeaderProto header = RpcResponseHeaderProto.newBuilder()
        .setStateId(100)
        .setCallId(1)
        .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
        .build();
    alignmentContext.receiveResponseState(header);

    // Now check that request state is updated
    RpcRequestHeaderProto.Builder requestBuilder = 
        RpcRequestHeaderProto.newBuilder()
            .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
            .setRpcOp(OperationProto.RPC_FINAL_PACKET)
            .setCallId(2);

    alignmentContext.updateRequestState(requestBuilder);
    RpcRequestHeaderProto request = requestBuilder.build();

    assertTrue(request.hasStateId());
    assertEquals(100, request.getStateId());
  }

  @Test
  public void testUpdateRequestStateNotSetWhenMinValue() {
    // When state ID is MIN_VALUE, it should not be set in request
    RpcRequestHeaderProto.Builder requestBuilder = 
        RpcRequestHeaderProto.newBuilder()
            .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
            .setRpcOp(OperationProto.RPC_FINAL_PACKET)
            .setCallId(1);

    alignmentContext.updateRequestState(requestBuilder);
    RpcRequestHeaderProto request = requestBuilder.build();

    // stateId should not be set when it's MIN_VALUE
    assertEquals(false, request.hasStateId());
  }

  @Test
  public void testAdvanceStateId() {
    // Test advanceStateId used after msync
    alignmentContext.advanceStateId(200);
    assertEquals(200, alignmentContext.getLastSeenStateId());

    // Lower value should not update
    alignmentContext.advanceStateId(100);
    assertEquals(200, alignmentContext.getLastSeenStateId());

    // Higher value should update
    alignmentContext.advanceStateId(300);
    assertEquals(300, alignmentContext.getLastSeenStateId());
  }

  @Test
  public void testIsCoordinatedCall() {
    // All calls should be coordinated for follower reads
    assertTrue(alignmentContext.isCoordinatedCall("anyProtocol", "anyMethod"));
  }
}

