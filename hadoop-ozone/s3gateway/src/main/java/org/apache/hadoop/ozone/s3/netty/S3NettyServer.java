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

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.OptionalInt;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pure Netty HTTP server for the S3 Gateway (PoC).
 *
 * Runs an NIO event loop serving S3 API requests directly through
 * Netty channel handlers, without Jersey/JAX-RS or a servlet container.
 */
public class S3NettyServer implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3NettyServer.class);

  private final OzoneConfiguration conf;
  private final OzoneClient ozoneClient;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;
  private InetSocketAddress httpAddress;

  public S3NettyServer(OzoneConfiguration conf, OzoneClient ozoneClient) {
    this.conf = conf;
    this.ozoneClient = ozoneClient;
    this.httpAddress = getHttpBindAddress();
  }

  public void start() throws InterruptedException {
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new S3HttpInitializer(conf, ozoneClient))
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_BACKLOG, 1024);

    serverChannel = bootstrap.bind(httpAddress).sync().channel();

    InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.localAddress();
    if (boundAddress != null) {
      httpAddress = boundAddress;
    }

    String realAddress = NetUtils.getHostPortString(
        NetUtils.getConnectAddress(httpAddress));
    LOG.info("Pure Netty S3 Gateway PoC listening at http://{}", realAddress);
  }

  public void stop() throws InterruptedException {
    LOG.info("Stopping pure Netty S3 Gateway PoC");
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully().sync();
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully().sync();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while stopping S3 Netty server", e);
    }
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  private InetSocketAddress getHttpBindAddress() {
    final Optional<String> bindHost = getHostNameFromConfigKeys(conf,
        S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_KEY);
    final OptionalInt addressPort = getPortNumberFromConfigKeys(conf,
        S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY);
    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY);

    String hostName = bindHost.orElse(
        addressHost.orElse(S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_DEFAULT));
    int port = addressPort.orElse(S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_PORT_DEFAULT);

    return NetUtils.createSocketAddr(hostName + ":" + port);
  }
}
