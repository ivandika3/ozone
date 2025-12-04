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
import java.util.concurrent.atomic.LongAccumulator;
import com.google.protobuf.Message;
import org.apache.hadoop.ipc_.AlignmentContext;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc_.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * Global State Id context for the client.
 * <p>
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
public class ClientAlignmentContext implements AlignmentContext {

  private final LongAccumulator lastSeenStateId;

  public ClientAlignmentContext() {
    this(new LongAccumulator(Math::max, Long.MIN_VALUE));
  }

  public ClientAlignmentContext(LongAccumulator lastSeenStateId) {
    this.lastSeenStateId = lastSeenStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return lastSeenStateId.get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method, Message payload) {
    throw new UnsupportedOperationException(
        "Client should not be checking uncoordinated call");
  }

  /**
   * Client side implementation only receives state alignment info.
   * It does not provide state alignment info therefore this does nothing.
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Client side implementation for receiving state alignment info
   * in responses.
   */
  @Override
  public synchronized void receiveResponseState(RpcResponseHeaderProto header) {
    lastSeenStateId.accumulate(header.getStateId());
  }

  /**
   * Client side implementation for providing state alignment info in requests.
   */
  @Override
  public synchronized void updateRequestState(RpcRequestHeaderProto.Builder header) {
    if (lastSeenStateId.get() != Long.MIN_VALUE) {
      header.setStateId(lastSeenStateId.get());
    }
  }

  /**
   * Client side implementation only provides state alignment info in requests.
   * Client does not receive RPC requests therefore this does nothing.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header, long threshold)
      throws IOException {
    // Do nothing.
    return 0;
  }
}
