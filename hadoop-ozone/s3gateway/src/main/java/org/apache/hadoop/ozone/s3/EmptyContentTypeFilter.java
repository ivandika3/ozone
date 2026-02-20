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

import java.io.IOException;
import java.util.List;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

/**
 * JAX-RS filter to handle requests with empty string content-type (ruby sdk).
 *
 * Strips the Content-Type header when its value is an empty string,
 * preventing Jersey from rejecting the request.
 */
@Provider
@PreMatching
@Priority(EmptyContentTypeFilter.PRIORITY)
public class EmptyContentTypeFilter implements ContainerRequestFilter {

  public static final int PRIORITY = 1;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    MultivaluedMap<String, String> headers = requestContext.getHeaders();
    List<String> contentTypes = headers.get("Content-Type");
    if (contentTypes != null && contentTypes.size() == 1
        && "".equals(contentTypes.get(0))) {
      headers.remove("Content-Type");
    }
  }
}
