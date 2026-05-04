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
 * See the LICENSE file distributed with this work for additional
 * information regarding copyright ownership.
 */

package org.apache.hadoop.ozone.om.protocolPB;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link OzoneManagerProtocolClientSideTranslatorPB}.
 */
class TestOzoneManagerProtocolClientSideTranslatorPB {

  @Test
  void submitRequestAddsThreadLocalReadConsistencyHint() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.setThreadLocalReadConsistency(ReadConsistency.LOCAL_LEASE);

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getReadConsistency())
        .isEqualTo(ReadConsistency.LOCAL_LEASE.toProto());
  }

  @Test
  void submitRequestAddsThreadLocalLocalLeaseContext() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.setThreadLocalReadConsistency(ReadConsistency.LOCAL_LEASE,
        10L, 100L);

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getReadConsistency())
        .isEqualTo(ReadConsistency.LOCAL_LEASE.toProto());
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .hasLocalLeaseContext())
        .isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getLocalLeaseContext()
        .getLogLimit())
        .isEqualTo(10L);
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getLocalLeaseContext()
        .getLeaseTimeMs())
        .isEqualTo(100L);
  }

  @Test
  void submitRequestOmitsReadConsistencyHintByDefault() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isFalse();
  }

  private static final class CapturingTransport implements OmTransport {
    private OMRequest lastRequest;

    @Override
    public OMResponse submitRequest(OMRequest payload) {
      lastRequest = payload;
      return OMResponse.newBuilder()
          .setCmdType(payload.getCmdType())
          .setStatus(Status.OK)
          .setSuccess(true)
          .setServiceListResponse(ServiceListResponse.newBuilder())
          .build();
    }

    @Override
    public Text getDelegationTokenService() {
      return new Text();
    }

    @Override
    public void close() throws IOException {
    }

    private OMRequest getLastRequest() {
      return lastRequest;
    }
  }
}
