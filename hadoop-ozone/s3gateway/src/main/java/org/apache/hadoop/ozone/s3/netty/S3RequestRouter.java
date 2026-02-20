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

package org.apache.hadoop.ozone.s3.netty;

import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes incoming S3 HTTP requests to the appropriate handler
 * based on the URI path structure and HTTP method.
 *
 * S3 URL patterns:
 * <ul>
 *   <li>{@code GET /} - List buckets</li>
 *   <li>{@code PUT /<bucket>} - Create bucket</li>
 *   <li>{@code HEAD /<bucket>} - Head bucket</li>
 *   <li>{@code DELETE /<bucket>} - Delete bucket</li>
 *   <li>{@code GET /<bucket>} - List objects</li>
 *   <li>{@code PUT /<bucket>/<key>} - Put object</li>
 *   <li>{@code GET /<bucket>/<key>} - Get object</li>
 *   <li>{@code HEAD /<bucket>/<key>} - Head object</li>
 *   <li>{@code DELETE /<bucket>/<key>} - Delete object</li>
 * </ul>
 */
public class S3RequestRouter extends SimpleChannelInboundHandler<FullHttpRequest> {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3RequestRouter.class);

  private final S3ObjectHandler objectHandler;
  private final S3BucketHandler bucketHandler;

  public S3RequestRouter(OzoneConfiguration conf, OzoneClient ozoneClient) {
    this.objectHandler = new S3ObjectHandler(conf, ozoneClient);
    this.bucketHandler = new S3BucketHandler(conf, ozoneClient);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
    String path = decoder.path();
    HttpMethod method = request.method();

    LOG.debug("{} {}", method, path);

    // Parse path: /<bucket>/<key...>
    String[] segments = parsePath(path);

    try {
      if (segments.length == 0) {
        // Root endpoint: GET / → list buckets
        if (method == HttpMethod.GET) {
          bucketHandler.listBuckets(ctx, request);
        } else {
          S3ResponseHelper.sendError(ctx, METHOD_NOT_ALLOWED,
              "MethodNotAllowed", "The specified method is not allowed", path);
        }
      } else if (segments.length == 1) {
        // Bucket endpoint: /<bucket>
        String bucketName = segments[0];
        if (method == HttpMethod.GET) {
          bucketHandler.listObjects(ctx, request, bucketName);
        } else if (method == HttpMethod.PUT) {
          bucketHandler.createBucket(ctx, request, bucketName);
        } else if (method == HttpMethod.HEAD) {
          bucketHandler.headBucket(ctx, request, bucketName);
        } else if (method == HttpMethod.DELETE) {
          bucketHandler.deleteBucket(ctx, request, bucketName);
        } else {
          S3ResponseHelper.sendError(ctx, METHOD_NOT_ALLOWED,
              "MethodNotAllowed", "The specified method is not allowed", path);
        }
      } else {
        // Object endpoint: /<bucket>/<key...>
        String bucketName = segments[0];
        String keyPath = path.substring(bucketName.length() + 2); // skip /<bucket>/
        if (method == HttpMethod.GET) {
          objectHandler.getObject(ctx, request, bucketName, keyPath);
        } else if (method == HttpMethod.PUT) {
          objectHandler.putObject(ctx, request, bucketName, keyPath);
        } else if (method == HttpMethod.HEAD) {
          objectHandler.headObject(ctx, request, bucketName, keyPath);
        } else if (method == HttpMethod.DELETE) {
          objectHandler.deleteObject(ctx, request, bucketName, keyPath);
        } else {
          S3ResponseHelper.sendError(ctx, NOT_IMPLEMENTED,
              "NotImplemented",
              "The requested method is not implemented", path);
        }
      }
    } catch (Exception e) {
      LOG.error("Error processing {} {}", method, path, e);
      S3ResponseHelper.sendInternalError(ctx, e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Unhandled exception in S3 pipeline", cause);
    if (ctx.channel().isActive()) {
      S3ResponseHelper.sendInternalError(ctx,
          cause instanceof Exception ? (Exception) cause
              : new RuntimeException(cause));
    }
  }

  private static String[] parsePath(String path) {
    if (path == null || path.equals("/") || path.isEmpty()) {
      return new String[0];
    }
    String trimmed = path.startsWith("/") ? path.substring(1) : path;
    if (trimmed.isEmpty()) {
      return new String[0];
    }
    int slashIdx = trimmed.indexOf('/');
    if (slashIdx == -1) {
      return new String[]{trimmed};
    }
    return new String[]{trimmed.substring(0, slashIdx), trimmed.substring(slashIdx + 1)};
  }
}
