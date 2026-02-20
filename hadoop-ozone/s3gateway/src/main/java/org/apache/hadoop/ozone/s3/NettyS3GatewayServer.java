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

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import io.netty.channel.Channel;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.net.NetUtils;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty-based HTTP server for the S3 Gateway API.
 *
 * Replaces the Jetty/Servlet-based {@link S3GatewayHttpServer} with a
 * non-blocking Netty transport while preserving Jersey JAX-RS routing
 * and Weld CDI dependency injection.
 */
public class NettyS3GatewayServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(NettyS3GatewayServer.class);

  private final MutableConfigurationSource conf;
  private Channel serverChannel;
  private SeContainer cdiContainer;
  private InetSocketAddress httpAddress;

  public NettyS3GatewayServer(MutableConfigurationSource conf) {
    this.conf = conf;
    this.httpAddress = getHttpBindAddress();
  }

  public void start() throws IOException {
    LOG.info("Starting Netty-based S3 Gateway HTTP server");

    cdiContainer = SeContainerInitializer.newInstance()
        .addPackages(true, NettyS3GatewayServer.class)
        .initialize();

    ResourceConfig config = ResourceConfig.forApplication(new GatewayApplication());

    URI baseUri = URI.create(
        "http://" + httpAddress.getHostString() + ":" + httpAddress.getPort() + "/");

    serverChannel = NettyHttpContainerProvider.createServer(
        baseUri, config, false);

    InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.localAddress();
    if (boundAddress != null) {
      httpAddress = boundAddress;
    }

    String realAddress = NetUtils.getHostPortString(
        NetUtils.getConnectAddress(httpAddress));
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, realAddress);

    LOG.info("Netty S3 Gateway HTTP server listening at http://{}", realAddress);
  }

  public void stop() throws Exception {
    LOG.info("Stopping Netty S3 Gateway HTTP server");
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    if (cdiContainer != null && cdiContainer.isRunning()) {
      cdiContainer.close();
    }
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    // HTTPS not yet supported on Netty path; returns null
    return null;
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
