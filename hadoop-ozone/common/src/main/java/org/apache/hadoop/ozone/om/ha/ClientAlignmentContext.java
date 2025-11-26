package org.apache.hadoop.ozone.om.ha;

import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;

public class ClientAlignmentContext implements AlignmentContext {

  private final LongAccumulator lastSeenStateId;

  public ClientAlignmentContext(LongAccumulator lastSeenStateId) {
    this.lastSeenStateId = lastSeenStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return lastSeenStateId.get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
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
