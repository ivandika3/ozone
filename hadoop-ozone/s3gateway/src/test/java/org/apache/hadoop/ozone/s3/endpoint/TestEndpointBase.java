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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.LOCAL_LEASE_LOG_LIMIT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.LOCAL_LEASE_TIME_MS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ClientProtocolStub;
import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

/**
 * Tests the s3 EndpointBase class methods.
 * Test methods of the EndpointBase.
 */
public class TestEndpointBase {

  /**
   * Verify s3 metadata key "gdprEnabled" can't be set up directly
   * from the normal client's request,
   * it should be decided on the server side.
   */
  @Test
  public void testFilterGDPRFromCustomMetadataHeaders()
          throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders
            = new MultivaluedHashMap<>();
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + OzoneConsts.GDPR_FLAG, "true");

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> filteredCustomMetadata =
            endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);
    assertThat(filteredCustomMetadata).containsKey("custom-key1");
    assertEquals(
            "custom-value1", filteredCustomMetadata.get("custom-key1"));
    assertThat(filteredCustomMetadata).containsKey("custom-key2");
    assertEquals(
            "custom-value2", filteredCustomMetadata.get("custom-key2"));
    assertThat(filteredCustomMetadata).doesNotContainKey(OzoneConsts.GDPR_FLAG);
  }

  /**
   * Verify s3 request metadata size should be smaller than 2 KB.
   */
  @Test
  public void testCustomMetadataHeadersSizeOverbig() {
    MultivaluedMap<String, String> s3requestHeaders
            = new MultivaluedHashMap<>();
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key3",
            new String(new byte[3000], StandardCharsets.UTF_8));

    EndpointBase endpointBase = new EndpointBase() {
    };

    OS3Exception e = assertThrows(OS3Exception.class, () -> endpointBase
        .getCustomMetadataFromHeaders(s3requestHeaders),
        "getCustomMetadataFromHeaders should fail." +
            " Expected OS3Exception not thrown");
    assertThat(e.getCode()).contains("MetadataTooLarge");
  }

  @Test
  public void testCustomMetadataHeadersWithUpperCaseHeaders() throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    String key = "CUSTOM-KEY";
    String value = "custom-value1";
    s3requestHeaders.add(CUSTOM_METADATA_HEADER_PREFIX.toUpperCase(Locale.ROOT) + key, value);

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> customMetadata = endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);

    assertEquals(value, customMetadata.get(key));
  }

  @Test
  public void testReadConsistencyHeaderLocalLease() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("local-lease");

    RootEndpoint endpoint = newRootEndpoint(headers);

    ClientProtocol clientProtocol =
        endpoint.getClient().getObjectStore().getClientProxy();
    assertEquals(ReadConsistency.LOCAL_LEASE,
        clientProtocol.getThreadLocalReadConsistency());
  }

  @Test
  public void testReadConsistencyHeaderLocalLeaseContext() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("local-lease");
    when(headers.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER))
        .thenReturn("10");
    when(headers.getHeaderString(LOCAL_LEASE_TIME_MS_HEADER))
        .thenReturn("100");

    RootEndpoint endpoint = newRootEndpoint(headers);

    ClientProtocolStub clientProtocol = (ClientProtocolStub) endpoint
        .getClient().getObjectStore().getClientProxy();
    assertEquals(ReadConsistency.LOCAL_LEASE,
        clientProtocol.getThreadLocalReadConsistency());
    assertEquals(10L, clientProtocol.getThreadLocalLocalLeaseLogLimit());
    assertEquals(100L, clientProtocol.getThreadLocalLocalLeaseTimeMs());
  }

  @Test
  public void testReadConsistencyHeaderLocalLeaseContextAllowsMinusOne() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("local-lease");
    when(headers.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER))
        .thenReturn("-1");
    when(headers.getHeaderString(LOCAL_LEASE_TIME_MS_HEADER))
        .thenReturn("-1");

    RootEndpoint endpoint = newRootEndpoint(headers);

    ClientProtocolStub clientProtocol = (ClientProtocolStub) endpoint
        .getClient().getObjectStore().getClientProxy();
    assertEquals(-1L, clientProtocol.getThreadLocalLocalLeaseLogLimit());
    assertEquals(-1L, clientProtocol.getThreadLocalLocalLeaseTimeMs());
  }

  @Test
  public void testReadConsistencyHeaderLinearizableFollower() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("linearizable-follower");

    RootEndpoint endpoint = newRootEndpoint(headers);

    ClientProtocol clientProtocol =
        endpoint.getClient().getObjectStore().getClientProxy();
    assertEquals(ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER,
        clientProtocol.getThreadLocalReadConsistency());
  }

  @Test
  public void testReadConsistencyHeaderUnsetByDefault() {
    RootEndpoint endpoint = newRootEndpoint(null);

    ClientProtocol clientProtocol =
        endpoint.getClient().getObjectStore().getClientProxy();
    assertThat(clientProtocol.getThreadLocalReadConsistency()).isNull();
  }

  @Test
  public void testInvalidReadConsistencyHeader() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("eventual");

    OS3Exception e = assertThrows(OS3Exception.class,
        () -> EndpointBuilder.newRootEndpointBuilder()
            .setHeaders(headers)
            .build());
    assertThat(e.getCode()).isEqualTo("InvalidArgument");
  }

  @Test
  public void testInvalidLocalLeaseContextWithoutLocalLease() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("linearizable-follower");
    when(headers.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER))
        .thenReturn("10");

    OS3Exception e = assertThrows(OS3Exception.class,
        () -> EndpointBuilder.newRootEndpointBuilder()
            .setHeaders(headers)
            .build());
    assertThat(e.getCode()).isEqualTo("InvalidArgument");
  }

  @Test
  public void testInvalidLocalLeaseContextValue() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(READ_CONSISTENCY_HEADER))
        .thenReturn("local-lease");
    when(headers.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER))
        .thenReturn("abc");

    OS3Exception e = assertThrows(OS3Exception.class,
        () -> EndpointBuilder.newRootEndpointBuilder()
            .setHeaders(headers)
            .build());
    assertThat(e.getCode()).isEqualTo("InvalidArgument");
  }

  private static RootEndpoint newRootEndpoint(HttpHeaders headers) {
    ClientProtocol clientProtocol = new ClientProtocolStub(null);
    ObjectStoreStub objectStore =
        new ObjectStoreStub(new OzoneConfiguration(), clientProtocol);
    EndpointBuilder<RootEndpoint> builder = EndpointBuilder
        .newRootEndpointBuilder()
        .setClient(new OzoneClientStub(objectStore));
    if (headers != null) {
      builder.setHeaders(headers);
    }
    return builder.build();
  }

}
