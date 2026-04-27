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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.ListBucketForksResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests bucket fork client-side protocol translation.
 */
public class TestBucketForkClientSideTranslatorPB {

  @Test
  public void testCreateBucketForkRequest() throws IOException {
    OmTransport transport = Mockito.mock(OmTransport.class);
    BucketForkInfo forkInfo = createForkInfo("fork");
    when(transport.submitRequest(any())).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.CreateBucketFork)
        .setSuccess(true)
        .setStatus(Status.OK)
        .setCreateBucketForkResponse(
            OzoneManagerProtocolProtos.CreateBucketForkResponse.newBuilder()
                .setBucketForkInfo(forkInfo.getProtobuf()))
        .build());

    OzoneManagerProtocolClientSideTranslatorPB translator =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client");

    BucketForkInfo response = translator.createBucketFork(
        "source-vol", "source-bucket", "target-vol", "fork", "snap");

    ArgumentCaptor<OMRequest> requestCaptor =
        ArgumentCaptor.forClass(OMRequest.class);
    Mockito.verify(transport).submitRequest(requestCaptor.capture());
    OMRequest request = requestCaptor.getValue();
    assertEquals(Type.CreateBucketFork, request.getCmdType());
    assertEquals("source-vol",
        request.getCreateBucketForkRequest().getSourceVolumeName());
    assertEquals("snap",
        request.getCreateBucketForkRequest().getBaseSnapshotName());
    assertEquals(forkInfo, response);
  }

  @Test
  public void testListBucketForksRequest() throws IOException {
    OmTransport transport = Mockito.mock(OmTransport.class);
    BucketForkInfo first = createForkInfo("fork-a");
    BucketForkInfo second = createForkInfo("fork-b");
    when(transport.submitRequest(any())).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.ListBucketForks)
        .setSuccess(true)
        .setStatus(Status.OK)
        .setListBucketForksResponse(
            OzoneManagerProtocolProtos.ListBucketForksResponse.newBuilder()
                .addBucketForkInfo(first.getProtobuf())
                .addBucketForkInfo(second.getProtobuf())
                .setLastBucketName("fork-b"))
        .build());

    OzoneManagerProtocolClientSideTranslatorPB translator =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client");

    ListBucketForksResponse response = translator.listBucketForks(
        "vol", "fork", "fork-0", 2);

    ArgumentCaptor<OMRequest> requestCaptor =
        ArgumentCaptor.forClass(OMRequest.class);
    Mockito.verify(transport).submitRequest(requestCaptor.capture());
    OMRequest request = requestCaptor.getValue();
    assertEquals(Type.ListBucketForks, request.getCmdType());
    assertEquals("fork",
        request.getListBucketForksRequest().getBucketNamePrefix());
    assertEquals("fork-0",
        request.getListBucketForksRequest().getPrevBucketName());
    assertEquals(Arrays.asList(first, second), response.getBucketForkInfos());
    assertEquals("fork-b", response.getLastBucketName());
  }

  private static BucketForkInfo createForkInfo(String targetBucketName) {
    return BucketForkInfo.newBuilder()
        .setForkId(UUID.randomUUID())
        .setSourceVolumeName("source-vol")
        .setSourceBucketName("source-bucket")
        .setTargetVolumeName("target-vol")
        .setTargetBucketName(targetBucketName)
        .setBaseSnapshotId(UUID.randomUUID())
        .setBaseSnapshotName("snap")
        .setStatus(BucketForkInfo.BucketForkStatus.BUCKET_FORK_ACTIVE)
        .build();
  }
}
