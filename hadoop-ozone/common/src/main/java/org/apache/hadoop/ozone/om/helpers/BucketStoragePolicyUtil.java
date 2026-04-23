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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.StorageType;

/**
 * Compatibility helpers for the bucket-level transition from storageType to
 * storagePolicy.
 */
public final class BucketStoragePolicyUtil {

  private BucketStoragePolicyUtil() {
  }

  public static StoragePolicy fromStorageType(StorageType storageType) {
    if (storageType == null) {
      return null;
    }
    switch (storageType) {
    case SSD:
    case RAM_DISK:
      return OzoneStoragePolicy.HOT;
    case ARCHIVE:
      return OzoneStoragePolicy.COLD;
    case DISK:
    default:
      return OzoneStoragePolicy.WARM;
    }
  }

  public static StorageType toStorageType(StoragePolicy storagePolicy) {
    if (storagePolicy == null) {
      return null;
    }
    switch ((OzoneStoragePolicy) storagePolicy) {
    case HOT:
      return StorageType.SSD;
    case COLD:
      return StorageType.ARCHIVE;
    case WARM:
    default:
      return StorageType.DISK;
    }
  }
}
