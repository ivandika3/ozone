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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMServerAlignmentContext - server-side state tracking for
 * follower reads.
 */
public class TestOMServerAlignmentContext {

  private OMServerAlignmentContext alignmentContext;
  private AtomicLong lastAppliedIndex;

  @BeforeEach
  public void setUp() {
    lastAppliedIndex = new AtomicLong(0);
    alignmentContext = new OMServerAlignmentContext(lastAppliedIndex::get);
  }

  @Test
  public void testGetLastSeenStateId() {
    assertEquals(0, alignmentContext.getLastSeenStateId());

    lastAppliedIndex.set(100);
    assertEquals(100, alignmentContext.getLastSeenStateId());

    lastAppliedIndex.set(200);
    assertEquals(200, alignmentContext.getLastSeenStateId());
  }

  @Test
  public void testUpdateResponseState() {
    lastAppliedIndex.set(150);

    RpcResponseHeaderProto.Builder responseBuilder =
        RpcResponseHeaderProto.newBuilder()
            .setCallId(1)
            .setStatus(RpcResponseHeaderProto.RpcStatusProto.SUCCESS);

    alignmentContext.updateResponseState(responseBuilder);
    RpcResponseHeaderProto response = responseBuilder.build();

    assertTrue(response.hasStateId());
    assertEquals(150, response.getStateId());
  }

  @Test
  public void testReceiveRequestState() throws IOException {
    // Request with stateId = 100
    RpcRequestHeaderProto header = RpcRequestHeaderProto.newBuilder()
        .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
        .setRpcOp(OperationProto.RPC_FINAL_PACKET)
        .setCallId(1)
        .setStateId(100)
        .build();

    long clientStateId = alignmentContext.receiveRequestState(header, 0);
    assertEquals(100, clientStateId);
  }

  @Test
  public void testReceiveRequestStateNoStateId() throws IOException {
    // Request without stateId
    RpcRequestHeaderProto header = RpcRequestHeaderProto.newBuilder()
        .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
        .setRpcOp(OperationProto.RPC_FINAL_PACKET)
        .setCallId(1)
        .build();

    long clientStateId = alignmentContext.receiveRequestState(header, 0);
    assertEquals(Long.MIN_VALUE, clientStateId);
  }

  @Test
  public void testIsCoordinatedCallForOMProtocol() {
    String omProtocol = "org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol";
    
    // submitRequest should be coordinated
    assertTrue(alignmentContext.isCoordinatedCall(omProtocol, "submitRequest"));
    
    // Other methods should not be coordinated
    assertFalse(alignmentContext.isCoordinatedCall(omProtocol, "otherMethod"));
  }

  @Test
  public void testIsCoordinatedCallForNonOMProtocol() {
    // Non-OM protocols should not be coordinated
    assertFalse(alignmentContext.isCoordinatedCall(
        "org.apache.hadoop.other.Protocol", "submitRequest"));
    assertFalse(alignmentContext.isCoordinatedCall(
        "org.apache.hadoop.other.Protocol", "anyMethod"));
  }
}

