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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;

/**
 * Utility methods for building S3-compatible Netty HTTP responses.
 */
public final class S3ResponseHelper {

  private S3ResponseHelper() {
  }

  public static FullHttpResponse xmlResponse(
      HttpResponseStatus status, String xmlBody) {
    byte[] bytes = xmlBody.getBytes(StandardCharsets.UTF_8);
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
    response.headers().set(HttpHeaderNames.SERVER, "Ozone");
    response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
    return response;
  }

  public static FullHttpResponse emptyResponse(HttpResponseStatus status) {
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    response.headers().set(HttpHeaderNames.SERVER, "Ozone");
    response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
    return response;
  }

  public static FullHttpResponse binaryResponse(
      HttpResponseStatus status, byte[] data, String contentType) {
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status, Unpooled.wrappedBuffer(data));
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, data.length);
    response.headers().set(HttpHeaderNames.SERVER, "Ozone");
    response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
    return response;
  }

  public static void sendError(
      ChannelHandlerContext ctx, HttpResponseStatus status, String code, String message, String resource) {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<Error>"
        + "<Code>" + code + "</Code>"
        + "<Message>" + escapeXml(message) + "</Message>"
        + "<Resource>" + escapeXml(resource) + "</Resource>"
        + "</Error>";
    FullHttpResponse response = xmlResponse(status, xml);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  public static void sendNotFound(ChannelHandlerContext ctx, String resource) {
    sendError(ctx, NOT_FOUND, "NoSuchKey",
        "The specified key does not exist.", resource);
  }

  public static void sendInternalError(ChannelHandlerContext ctx, Exception e) {
    sendError(ctx, INTERNAL_SERVER_ERROR, "InternalError",
        e.getMessage() != null ? e.getMessage() : "Internal server error", "/");
  }

  private static String escapeXml(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;");
  }
}
