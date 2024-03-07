/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.s3.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Class that handles all exceptions that do not have corresponding
 * exception mapper.
 */
@Provider
public class GlobalExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOG =
      LoggerFactory.getLogger(GlobalExceptionMapper.class);

  @Context
  private HttpServletRequest httpServletRequest;

  @Override
  public Response toResponse(Throwable exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Uncaught exception: {}", exception.toString());
    }
    return Response.status(S3ErrorTable.INTERNAL_ERROR.getHttpCode())
        .entity(
            S3ErrorTable.newError(
                    S3ErrorTable.INTERNAL_ERROR,
                    httpServletRequest.getRequestURI(),
                    exception)
                .toXml()
        ).build();
  }
}
