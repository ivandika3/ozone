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

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

/**
 * Utility methods for working with {@link StorageTier}.
 */
public final class StorageTierUtil {

  private StorageTierUtil() {
  }

  public static void validateNotEmpty(StorageTier storageTier)
      throws SCMException {
    if (storageTier == StorageTier.EMPTY) {
      throw new SCMException("Cannot create a pipeline for the EMPTY tier",
          SCMException.ResultCodes.CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER);
    }
  }

  /**
   * Returns the backing {@link StorageType} for a uniform tier.
   */
  public static StorageType getStorageTypeForUniformStorageTier(StorageTier storageTier)
      throws SCMException {
    validateNotEmpty(storageTier);
    if (!storageTier.isUniformStorageType()) {
      throw new SCMException(
          "Unsupported non-uniform storage tier " + storageTier,
          SCMException.ResultCodes.UNSUPPORTED_NON_UNIFORM_STORAGE_TIER);
    }
    return storageTier.getStorageTypes(1).get(0);
  }

  /**
   * Returns the backing {@link StorageType} for a uniform tier.
   */
  public static StorageType getStorageTypeForUniformStorageTier(
      StorageTier storageTier, ReplicationConfig config) throws SCMException {
    validateNotEmpty(storageTier);
    if (!storageTier.isUniformStorageType()) {
      throw new SCMException(
          "Unsupported non-uniform storage tier " + storageTier,
          SCMException.ResultCodes.UNSUPPORTED_NON_UNIFORM_STORAGE_TIER);
    }
    List<StorageType> storageTypes = storageTier.getStorageTypes(config);
    return storageTypes.get(0);
  }

  public static List<StorageTier> findSupportedStorageTiers(
      List<Set<StorageType>> dnStorageTypes) {
    List<StorageTier> supportedStorageTiers = new ArrayList<>();
    Set<List<StorageType>> combinations = Sets.cartesianProduct(dnStorageTypes);
    for (List<StorageType> combination : combinations) {
      long id = StorageTier.computeId(combination);
      StorageTier tier = StorageTier.fromID(dnStorageTypes.size(), id);
      if (tier != null && tier != StorageTier.EMPTY) {
        supportedStorageTiers.add(tier);
      }
    }
    return supportedStorageTiers;
  }

  public static boolean shouldFallBack(StorageTier storageTier, SCMException scmException) {
    if (storageTier == null || storageTier.getFallbackStorageTypes().isEmpty()) {
      return false;
    }
    return scmException.getResult().equals(FAILED_TO_FIND_NODES_WITH_SPACE) ||
        scmException.getResult().equals(FAILED_TO_FIND_SUITABLE_NODE);
  }

  public static List<StorageType> getAvailableStorageTypeOrdered(StorageTier storageTier)
      throws SCMException {
    ArrayList<StorageType> availableStorageTypes = new ArrayList<>();
    if (storageTier == null) {
      return Collections.singletonList(
          getStorageTypeForUniformStorageTier(StorageTier.getDefaultTier()));
    }
    availableStorageTypes.add(getStorageTypeForUniformStorageTier(storageTier));
    availableStorageTypes.addAll(storageTier.getFallbackStorageTypes());
    return availableStorageTypes;
  }

  /**
   * Represents information about a StorageType and its required count.
   */
  public static class StorageTypeInfo {
    private final StorageType storageType;
    private final int count;

    public StorageTypeInfo(StorageType storageType, int count) {
      this.storageType = storageType;
      this.count = count;
    }

    /**
     * Returns the storage type.
     *
     * @return the storage type
     */
    public StorageType getStorageType() {
      return storageType;
    }

    /**
     * Returns the required count of this storage type.
     *
     * @return the required count
     */
    public int getCount() {
      return count;
    }
  }
}
