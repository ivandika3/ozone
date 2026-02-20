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

/**
 * Pure Netty-based S3 Gateway server implementation (PoC).
 *
 * This package provides a from-scratch Netty HTTP server that directly
 * handles S3 API requests without Jersey/JAX-RS, removing the overhead
 * of the servlet container and JAX-RS reflection.
 *
 * Currently implements: GET/PUT/HEAD object, list buckets.
 */
package org.apache.hadoop.ozone.s3.netty;
