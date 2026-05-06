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

package org.apache.hadoop.ozone.shell.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests bucket fork shell handlers.
 */
public class TestBucketForkHandlers {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCreateBucketForkDelegatesToObjectStore()
      throws IOException {
    CreateBucketForkHandler handler = new CreateBucketForkHandler();
    new CommandLine(handler).parseArgs("--base-snapshot", "snap",
        "o3://om/source-vol/source-bucket", "o3://om/target-vol/fork");
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    BucketForkInfo forkInfo = createForkInfo("fork");
    when(objectStore.createBucketFork("source-vol", "source-bucket",
        "target-vol", "fork", "snap")).thenReturn(forkInfo);

    handler.execute(client, new OzoneAddress("o3://om/target-vol/fork"));

    verify(objectStore).createBucketFork("source-vol", "source-bucket",
        "target-vol", "fork", "snap");
    assertEquals("fork", readOutputJson().get("targetBucketName").asText());
  }

  @Test
  public void testInfoBucketForkDelegatesToObjectStore() throws IOException {
    InfoBucketForkHandler handler = new InfoBucketForkHandler();
    new CommandLine(handler).parseArgs("o3://om/vol/fork");
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    BucketForkInfo forkInfo = createForkInfo("fork");
    when(objectStore.getBucketForkInfo("vol", "fork")).thenReturn(forkInfo);

    handler.execute(client, new OzoneAddress("o3://om/vol/fork"));

    verify(objectStore).getBucketForkInfo("vol", "fork");
    assertEquals("fork", readOutputJson().get("targetBucketName").asText());
  }

  @Test
  public void testDeleteBucketForkDelegatesToObjectStore() throws IOException {
    DeleteBucketForkHandler handler = new DeleteBucketForkHandler();
    new CommandLine(handler).parseArgs("o3://om/vol/fork");
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    handler.execute(client, new OzoneAddress("o3://om/vol/fork"));

    verify(objectStore).deleteBucketFork("vol", "fork");
  }

  @Test
  public void testListBucketForksDelegatesToObjectStore() throws IOException {
    ListBucketForkHandler handler = new ListBucketForkHandler();
    new CommandLine(handler).parseArgs("--prefix", "fork", "--start",
        "fork-0", "--length", "2", "o3://om/vol");
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(objectStore.listBucketForks("vol", "fork", "fork-0"))
        .thenReturn(Arrays.asList(
            createForkInfo("fork-a"), createForkInfo("fork-b")).iterator());

    handler.execute(client, new OzoneAddress("o3://om/vol"));

    verify(objectStore).listBucketForks("vol", "fork", "fork-0");
    JsonNode json = readOutputJson();
    assertEquals(2, json.size());
    assertEquals("fork-a", json.get(0).get("targetBucketName").asText());
  }

  private JsonNode readOutputJson() throws IOException {
    return new ObjectMapper().readTree(
        outContent.toString(DEFAULT_ENCODING));
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
