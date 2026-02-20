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

package org.apache.hadoop.ozone.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EmptyContentTypeFilter}.
 */
public class TestEmptyContentTypeFilter {

  @Test
  public void emptyContentTypeIsRemoved() throws Exception {
    EmptyContentTypeFilter filter = new EmptyContentTypeFilter();
    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle("Content-Type", "");
    when(ctx.getHeaders()).thenReturn(headers);

    filter.filter(ctx);

    assertFalse(headers.containsKey("Content-Type"));
  }

  @Test
  public void nonEmptyContentTypeIsKept() throws Exception {
    EmptyContentTypeFilter filter = new EmptyContentTypeFilter();
    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle("Content-Type", "application/xml");
    when(ctx.getHeaders()).thenReturn(headers);

    filter.filter(ctx);

    assertTrue(headers.containsKey("Content-Type"));
    assertEquals("application/xml", headers.getFirst("Content-Type"));
  }

  @Test
  public void noContentTypeHeader() throws Exception {
    EmptyContentTypeFilter filter = new EmptyContentTypeFilter();
    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    when(ctx.getHeaders()).thenReturn(headers);

    filter.filter(ctx);

    assertNull(headers.getFirst("Content-Type"));
  }
}
