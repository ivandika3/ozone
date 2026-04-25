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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3RequestContext}.
 */
public class TestS3RequestContext {

  @Test
  public void getBucketCachesLoadedBuckets() throws Exception {
    EndpointBase endpoint = mock(EndpointBase.class);
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(endpoint.getVolume()).thenReturn(volume);
    when(volume.getBucket("bucket")).thenReturn(bucket);

    S3RequestContext context = new S3RequestContext(endpoint, S3GAction.GET_BUCKET);

    assertSame(bucket, context.getBucket("bucket"));
    assertSame(bucket, context.getBucket("bucket"));
    verify(endpoint, times(1)).getVolume();
    verify(volume, times(1)).getBucket("bucket");
    verify(endpoint, times(1)).cacheBucket("bucket", bucket);
  }
}
