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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;

/**
 * Initializes the Netty channel pipeline for S3 HTTP request handling.
 */
public class S3HttpInitializer extends ChannelInitializer<SocketChannel> {

  private static final int MAX_CONTENT_LENGTH = 64 * 1024 * 1024;

  private final OzoneConfiguration conf;
  private final OzoneClient ozoneClient;

  public S3HttpInitializer(OzoneConfiguration conf, OzoneClient ozoneClient) {
    this.conf = conf;
    this.ozoneClient = ozoneClient;
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ChannelPipeline p = ch.pipeline();
    p.addLast("codec", new HttpServerCodec());
    p.addLast("aggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH));
    p.addLast("chunked-write", new ChunkedWriteHandler());
    p.addLast("s3-router", new S3RequestRouter(conf, ozoneClient));
  }
}
