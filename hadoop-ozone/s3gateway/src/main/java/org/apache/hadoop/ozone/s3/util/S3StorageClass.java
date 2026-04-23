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

package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;

/**
 * Maps S3 storage classes to Ozone storage policies.
 */
public enum S3StorageClass {
  STANDARD(OzoneStoragePolicy.HOT),
  STANDARD_IA(OzoneStoragePolicy.WARM),
  GLACIER(OzoneStoragePolicy.COLD),
  DEEP_ARCHIVE(OzoneStoragePolicy.COLD);

  private final StoragePolicy storagePolicy;

  S3StorageClass(StoragePolicy storagePolicy) {
    this.storagePolicy = storagePolicy;
  }

  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  public static S3StorageClass fromS3StorageClass(String storageClass) {
    return valueOf(storageClass);
  }

  public static S3StorageClass fromStoragePolicy(StoragePolicy storagePolicy) {
    if (storagePolicy == null) {
      return STANDARD;
    }
    if (OzoneStoragePolicy.COLD.equals(storagePolicy)) {
      return GLACIER;
    }
    if (OzoneStoragePolicy.WARM.equals(storagePolicy)) {
      return STANDARD_IA;
    }
    return STANDARD;
  }
}
