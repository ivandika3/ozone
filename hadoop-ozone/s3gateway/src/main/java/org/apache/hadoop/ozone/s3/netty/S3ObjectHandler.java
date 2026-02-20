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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import javax.xml.bind.DatatypeConverter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3 object-level operations (GET, PUT, HEAD, DELETE) in pure Netty.
 */
public class S3ObjectHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ObjectHandler.class);

  private final OzoneConfiguration conf;
  private final OzoneClient ozoneClient;

  public S3ObjectHandler(OzoneConfiguration conf, OzoneClient ozoneClient) {
    this.conf = conf;
    this.ozoneClient = ozoneClient;
  }

  public void getObject(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName, String keyPath) throws Exception {
    OzoneKeyDetails keyDetails;
    try {
      keyDetails = ozoneClient.getProxy().getS3KeyDetails(bucketName, keyPath);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND) {
        S3ResponseHelper.sendNotFound(ctx, keyPath);
        return;
      } else if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND) {
        S3ResponseHelper.sendError(ctx, NOT_FOUND,
            "NoSuchBucket", "The specified bucket does not exist", bucketName);
        return;
      }
      throw ex;
    }

    long length = keyDetails.getDataSize();
    byte[] data;
    try (OzoneInputStream inputStream = keyDetails.getContent()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream((int) Math.min(length, 8 * 1024 * 1024));
      IOUtils.copy(inputStream, baos);
      data = baos.toByteArray();
    }

    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(data));
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "binary/octet-stream");
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, data.length);
    response.headers().set(HttpHeaderNames.SERVER, "Ozone");
    response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
    response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");

    String eTag = keyDetails.getMetadata().get(OzoneConsts.ETAG);
    if (eTag != null) {
      response.headers().set(HttpHeaderNames.ETAG, "\"" + eTag + "\"");
    }

    ZonedDateTime lastModified = keyDetails.getModificationTime()
        .atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));
    response.headers().set(HttpHeaderNames.LAST_MODIFIED,
        RFC1123Util.FORMAT.format(lastModified));

    ctx.writeAndFlush(response);
  }

  public void putObject(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName, String keyPath) throws Exception {
    OzoneBucket bucket;
    try {
      bucket = ozoneClient.getObjectStore().getS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.VOLUME_NOT_FOUND) {
        S3ResponseHelper.sendError(ctx, NOT_FOUND,
            "NoSuchBucket", "The specified bucket does not exist", bucketName);
        return;
      }
      throw ex;
    }

    ByteBuf content = request.content();
    byte[] data = new byte[content.readableBytes()];
    content.readBytes(data);

    MessageDigest md5 = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
    md5.update(data);
    String md5Hash = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();

    try (OzoneOutputStream outputStream = bucket.createKey(
        keyPath, data.length)) {
      outputStream.write(data);
      outputStream.getMetadata().put(OzoneConsts.ETAG, md5Hash);
    }

    FullHttpResponse response = S3ResponseHelper.emptyResponse(OK);
    response.headers().set(HttpHeaderNames.ETAG, "\"" + md5Hash + "\"");
    ctx.writeAndFlush(response);
  }

  public void headObject(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName, String keyPath) throws Exception {
    OzoneKeyDetails keyDetails;
    try {
      keyDetails = (OzoneKeyDetails) ozoneClient.getProxy()
          .headS3Object(bucketName, keyPath);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND) {
        ctx.writeAndFlush(S3ResponseHelper.emptyResponse(NOT_FOUND));
        return;
      } else if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND) {
        S3ResponseHelper.sendError(ctx, NOT_FOUND,
            "NoSuchBucket", "The specified bucket does not exist", bucketName);
        return;
      }
      throw ex;
    }

    S3StorageType s3StorageType = keyDetails.getReplicationConfig() == null
        ? S3StorageType.STANDARD
        : S3StorageType.fromReplicationConfig(keyDetails.getReplicationConfig());

    FullHttpResponse response = S3ResponseHelper.emptyResponse(OK);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH,
        Long.toString(keyDetails.getDataSize()));
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "binary/octet-stream");
    response.headers().set("x-amz-storage-class", s3StorageType.toString());

    String eTag = keyDetails.getMetadata().get(OzoneConsts.ETAG);
    if (eTag != null) {
      response.headers().set(HttpHeaderNames.ETAG, "\"" + eTag + "\"");
    }

    ZonedDateTime lastModified = keyDetails.getModificationTime()
        .atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));
    response.headers().set(HttpHeaderNames.LAST_MODIFIED,
        RFC1123Util.FORMAT.format(lastModified));

    ctx.writeAndFlush(response);
  }

  public void deleteObject(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName, String keyPath) throws Exception {
    try {
      ozoneClient.getProxy().deleteKey(
          ozoneClient.getObjectStore().getS3Volume().getName(),
          bucketName, keyPath, false);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND) {
        // S3 returns 204 even for missing keys
      } else if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND) {
        S3ResponseHelper.sendError(ctx, NOT_FOUND,
            "NoSuchBucket", "The specified bucket does not exist", bucketName);
        return;
      } else {
        throw ex;
      }
    }
    ctx.writeAndFlush(S3ResponseHelper.emptyResponse(NO_CONTENT));
  }
}
