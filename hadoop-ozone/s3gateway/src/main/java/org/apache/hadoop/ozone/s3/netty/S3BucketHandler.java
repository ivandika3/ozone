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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3 bucket-level operations in pure Netty.
 */
public class S3BucketHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketHandler.class);

  private static final DateTimeFormatter ISO_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
          .withZone(ZoneOffset.UTC);

  private final OzoneConfiguration conf;
  private final OzoneClient ozoneClient;

  public S3BucketHandler(OzoneConfiguration conf, OzoneClient ozoneClient) {
    this.conf = conf;
    this.ozoneClient = ozoneClient;
  }

  public void listBuckets(ChannelHandlerContext ctx, FullHttpRequest request)
      throws Exception {
    OzoneVolume volume = ozoneClient.getObjectStore().getS3Volume();
    Iterator<? extends OzoneBucket> bucketIterator = volume.listBuckets(null);

    StringBuilder xml = new StringBuilder();
    xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.append("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.append("<Owner>");
    xml.append("<ID>").append(escapeXml(volume.getOwner())).append("</ID>");
    xml.append("<DisplayName>").append(escapeXml(volume.getOwner())).append("</DisplayName>");
    xml.append("</Owner>");
    xml.append("<Buckets>");

    while (bucketIterator.hasNext()) {
      OzoneBucket bucket = bucketIterator.next();
      xml.append("<Bucket>");
      xml.append("<Name>").append(escapeXml(bucket.getName())).append("</Name>");
      xml.append("<CreationDate>")
          .append(ISO_FORMATTER.format(bucket.getCreationTime()))
          .append("</CreationDate>");
      xml.append("</Bucket>");
    }

    xml.append("</Buckets>");
    xml.append("</ListAllMyBucketsResult>");

    FullHttpResponse response = S3ResponseHelper.xmlResponse(OK, xml.toString());
    ctx.writeAndFlush(response);
  }

  public void listObjects(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName) throws Exception {
    QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
    String prefix = getQueryParam(decoder, "prefix", "");
    String delimiter = getQueryParam(decoder, "delimiter", null);
    int maxKeys = getIntQueryParam(decoder, "max-keys", 1000);

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

    Iterator<? extends OzoneKey> keyIterator =
        bucket.listKeys(prefix, null, false);

    StringBuilder xml = new StringBuilder();
    xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.append("<Name>").append(escapeXml(bucketName)).append("</Name>");
    xml.append("<Prefix>").append(escapeXml(prefix)).append("</Prefix>");
    xml.append("<MaxKeys>").append(maxKeys).append("</MaxKeys>");
    xml.append("<IsTruncated>false</IsTruncated>");

    int count = 0;
    while (keyIterator.hasNext() && count < maxKeys) {
      OzoneKey key = keyIterator.next();
      xml.append("<Contents>");
      xml.append("<Key>").append(escapeXml(key.getName())).append("</Key>");
      xml.append("<Size>").append(key.getDataSize()).append("</Size>");
      xml.append("<LastModified>")
          .append(ISO_FORMATTER.format(key.getModificationTime()))
          .append("</LastModified>");
      xml.append("</Contents>");
      count++;
    }

    xml.append("<KeyCount>").append(count).append("</KeyCount>");
    xml.append("</ListBucketResult>");

    FullHttpResponse response = S3ResponseHelper.xmlResponse(OK, xml.toString());
    ctx.writeAndFlush(response);
  }

  public void createBucket(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName) throws Exception {
    try {
      ozoneClient.getObjectStore().createS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
        LOG.debug("Bucket {} already exists, returning 200 OK", bucketName);
      } else {
        throw ex;
      }
    }
    FullHttpResponse response = S3ResponseHelper.emptyResponse(OK);
    response.headers().set("Location", "/" + bucketName);
    ctx.writeAndFlush(response);
  }

  public void headBucket(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName) throws Exception {
    try {
      ozoneClient.getObjectStore().getS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.VOLUME_NOT_FOUND) {
        FullHttpResponse response = S3ResponseHelper.emptyResponse(NOT_FOUND);
        ctx.writeAndFlush(response);
        return;
      }
      throw ex;
    }
    FullHttpResponse response = S3ResponseHelper.emptyResponse(OK);
    ctx.writeAndFlush(response);
  }

  public void deleteBucket(ChannelHandlerContext ctx, FullHttpRequest request,
      String bucketName) throws Exception {
    try {
      ozoneClient.getObjectStore().deleteS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.VOLUME_NOT_FOUND) {
        S3ResponseHelper.sendError(ctx, NOT_FOUND,
            "NoSuchBucket", "The specified bucket does not exist", bucketName);
        return;
      }
      throw ex;
    }
    FullHttpResponse response = S3ResponseHelper.emptyResponse(NO_CONTENT);
    ctx.writeAndFlush(response);
  }

  private static String getQueryParam(
      QueryStringDecoder decoder, String key, String defaultValue) {
    List<String> values = decoder.parameters().get(key);
    if (values != null && !values.isEmpty()) {
      return values.get(0);
    }
    return defaultValue;
  }

  private static int getIntQueryParam(
      QueryStringDecoder decoder, String key, int defaultValue) {
    String val = getQueryParam(decoder, key, null);
    if (val != null) {
      try {
        return Integer.parseInt(val);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private static String escapeXml(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;");
  }
}
