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

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Utility class for converting between Hadoop's {@link StorageType} and
 * Ozone's protobuf representation {@link HddsProtos.StorageType}.
 */
public final class StorageTypeUtils {
  private StorageTypeUtils() {
  }

  public static HddsProtos.StorageType getStorageTypeProto(StorageType type)
      throws IllegalArgumentException {
    switch (type) {
    case SSD:
      return HddsProtos.StorageType.SSD_TYPE;
    case DISK:
      return HddsProtos.StorageType.DISK_TYPE;
    case ARCHIVE:
      return HddsProtos.StorageType.ARCHIVE_TYPE;
    case PROVIDED:
      return HddsProtos.StorageType.PROVIDED_TYPE;
    case RAM_DISK:
      return HddsProtos.StorageType.RAM_DISK_TYPE;
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }

  public static StorageType getFromProtobuf(HddsProtos.StorageType proto) throws
      IllegalArgumentException {
    switch (proto) {
    case SSD_TYPE:
      return StorageType.SSD;
    case DISK_TYPE:
      return StorageType.DISK;
    case ARCHIVE_TYPE:
      return StorageType.ARCHIVE;
    case PROVIDED_TYPE:
      return StorageType.PROVIDED;
    case RAM_DISK_TYPE:
      return StorageType.RAM_DISK;
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }

  /**
   * Returns protobuf StorageType enum value corresponding to the int ID value.
   * @param storageTypeID StorageType int ID value
   * @return HddsProtos.StorageType
   */
  public static HddsProtos.StorageType getStorageTypeProtoFromID(int storageTypeID) throws
      IllegalArgumentException {
    if (HddsProtos.StorageType.valueOf(storageTypeID) == null) {
      throw new IllegalArgumentException("Illegal storageTypeID " + storageTypeID);
    }
    return HddsProtos.StorageType.valueOf(storageTypeID);
  }

  /**
   * Returns Filesystem StorageType enum value corresponding to the int ID value.
   * @param storageTypeID StorageType int ID value
   * @return StorageType
   */
  public static StorageType getStorageTypeFromID(int storageTypeID) throws
      IllegalArgumentException {
    if (HddsProtos.StorageType.valueOf(storageTypeID) == null) {
      throw new IllegalArgumentException("Illegal storageTypeID " + storageTypeID);
    }
    return getFromProtobuf(HddsProtos.StorageType.valueOf(storageTypeID));
  }

  /**
   * Returns integer representation of protobuf StorageType.
   * @return storageType int ID value
   */
  public static int getIDFromProtobuf(HddsProtos.StorageType proto) throws
      IllegalArgumentException {
    switch (proto) {
    case SSD_TYPE:
    case DISK_TYPE:
    case ARCHIVE_TYPE:
    case PROVIDED_TYPE:
    case RAM_DISK_TYPE:
      return proto.getNumber();
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }

  public static int getID(StorageType storageType) throws
      IllegalArgumentException {
    return getStorageTypeProto(storageType).getNumber();
  }
}
