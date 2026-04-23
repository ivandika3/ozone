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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTierProto;

/**
 * Ozone specific storage tiers.
 */
public enum StorageTier {
  SSD("SSD", fallback(StorageType.DISK, StorageType.ARCHIVE), StorageType.SSD),
  DISK("DISK", fallback(StorageType.ARCHIVE), StorageType.DISK),
  ARCHIVE("ARCHIVE", noFallback(), StorageType.ARCHIVE),
  EMPTY("EMPTY");

  private final String tierName;
  private final List<StorageType> storageTypes;
  private final List<StorageType> fallbackStorageTypes;
  private final boolean isUniform;
  private static final Map<StorageTier, Map<Integer, List<StorageType>>>
      CACHE = new EnumMap<>(StorageTier.class);
  private static final int MAX_NODE_COUNT = 20;
  private static StorageTier defaultTier = DISK;
  private static final Map<StorageType, Integer> STORAGE_TYPE_PRIME_MAP =
      new EnumMap<>(StorageType.class);
  private static final Map<Integer, Map<Long, StorageTier>>
      NODE_COUNT_TO_STORAGE_TIER_MAP = new HashMap<>();

  StorageTier(String tierName) {
    this.tierName = tierName;
    this.storageTypes = Collections.emptyList();
    this.fallbackStorageTypes = Collections.emptyList();
    this.isUniform = true;
  }

  // Constructor for uniform storage tiers
  StorageTier(String tierName, List<StorageType> fallbackStorageTypes,
      StorageType uniformStorageType) {
    this.tierName = tierName;
    this.storageTypes = Collections.singletonList(uniformStorageType);
    this.fallbackStorageTypes = fallbackStorageTypes;
    this.isUniform = true;
  }

  // Constructor for non-uniform storage tiers
  StorageTier(String tierName, List<StorageType> fallbackStorageTypes,
      StorageType... storageTypes) {
    this.tierName = tierName;
    if (Arrays.stream(storageTypes).distinct().count() <= 1) {
      throw new IllegalArgumentException("StorageTier '" + tierName +
          "' requires at least two different StorageType instances." +
          " but only " + Arrays.stream(storageTypes).distinct().count() +
          " StorageType were provided.");
    }
    this.storageTypes = Arrays.asList(storageTypes);
    this.fallbackStorageTypes = fallbackStorageTypes;
    this.isUniform = false;
  }

  static {
    int prime = 2;
    for (StorageType type : StorageType.values()) {
      STORAGE_TYPE_PRIME_MAP.put(type, prime);
      prime = nextPrime(prime + 1);
    }

    // Precompute storage type mappings for common node counts.
    for (StorageTier tier : StorageTier.values()) {
      Map<Integer, List<StorageType>> tierCache = new HashMap<>();
      for (int nodeCount = 0; nodeCount <= MAX_NODE_COUNT; nodeCount++) {
        List<StorageType> storageTypes = tier.computeStorageTypes(nodeCount);
        tierCache.put(nodeCount, storageTypes);
        NODE_COUNT_TO_STORAGE_TIER_MAP
            .computeIfAbsent(nodeCount, k -> new HashMap<>())
            .put(computeId(storageTypes), tier);
      }
      CACHE.put(tier, tierCache);
    }
  }

  /**
   * Get the fallback StorageTypes allowed by the current StorageTier.
   * If no fallback StorageType is allowed, return an empty List
   * @return fallback StorageTypes
   */
  public List<StorageType> getFallbackStorageTypes() {
    return fallbackStorageTypes;
  }

  public static StorageTier getDefaultTier() {
    return defaultTier;
  }

  public StorageTierProto toProto() {
    switch (this) {
    case SSD:
      return StorageTierProto.SSD_TIER;
    case DISK:
      return StorageTierProto.DISK_TIER;
    case ARCHIVE:
      return StorageTierProto.ARCHIVE_TIER;
    default:
      throw new IllegalStateException(
          "Illegal StorageTier: " + this);
    }
  }

  public static StorageTier fromProto(StorageTierProto tier) {
    if (tier == null) {
      return getDefaultTier();
    }
    switch (tier) {
    case SSD_TIER:
      return SSD;
    case DISK_TIER:
      return DISK;
    case ARCHIVE_TIER:
      return ARCHIVE;
    case UNKNOWN_TIER:
      return getDefaultTier();
    default:
      return getDefaultTier();
    }
  }

  public String getTierName() {
    return tierName;
  }

  public boolean isUniformStorageType() {
    return isUniform;
  }

  /**
   * Computes the list of StorageTypes based on replication configuration.
   *
   * @param replicationConfig The replication configuration.
   * @return The list of StorageTypes for the given tier and replication configuration.
   */
  private List<StorageType> computeStorageTypes(int nodeCount) {
    if (isUniformStorageType()) {
      if (storageTypes.isEmpty()) {
        return Collections.emptyList();
      }
      return Collections.nCopies(nodeCount, storageTypes.get(0));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported not UniformStorage Storage Tier: " + this);
    }
  }

  /**
   * Maps a StorageTier to its corresponding StorageType based on replication type.
   *
   * @param replicationConfig The replication configuration.
   * @return The list of StorageTypes corresponding to the given tier and replication configuration.
   * @throws IllegalArgumentException if the replication configuration is not supported.
   */
  public List<StorageType> getStorageTypes(
      ReplicationConfig replicationConfig) {
    return getStorageTypes(replicationConfig.getRequiredNodes());
  }

  public List<StorageType> getStorageTypes(int nodeCount) {
    Map<Integer, List<StorageType>> tierCache = CACHE.get(this);

    if (tierCache != null) {
      List<StorageType> cachedStorageType = tierCache.get(nodeCount);
      if (cachedStorageType != null) {
        return cachedStorageType;
      }
    }

    return computeStorageTypes(nodeCount);
  }

  /**
   * Calculates the multiplication of the IDs of the given StorageType List.
   *
   * @param storageTypes the StorageType List need to calculate
   * @return int value;
   */
  public static long computeId(Collection<StorageType> storageTypes) {
    long computedId = 1;
    for (StorageType type : storageTypes) {
      long prime = STORAGE_TYPE_PRIME_MAP.get(type);
      if (computedId > Long.MAX_VALUE / prime) {
        throw new ArithmeticException("Overflow detected when calculating ID for StorageType.");
      }
      computedId *= prime;
    }
    return computedId;
  }

  public static StorageTier fromID(int nodeCount, long id) {
    if (nodeCount > MAX_NODE_COUNT) {
      throw new IllegalArgumentException("Not support node count: " + nodeCount
          + "Max support node count: " + MAX_NODE_COUNT);
    }
    if (NODE_COUNT_TO_STORAGE_TIER_MAP.get(nodeCount) != null) {
      return NODE_COUNT_TO_STORAGE_TIER_MAP.get(nodeCount).get(id);
    }
    return null;
  }

  public static void setDefault(StorageTier storageTier) {
    defaultTier = storageTier;
  }

  private static List<StorageType> fallback(StorageType... fallbackTypes) {
    return Arrays.asList(fallbackTypes);
  }

  private static List<StorageType> noFallback() {
    return Collections.emptyList();
  }

  public List<StorageType> getStorageTypes() {
    return storageTypes;
  }

  private static int nextPrime(int value) {
    while (!isPrime(value)) {
      value++;
    }
    return value;
  }

  private static boolean isPrime(int value) {
    if (value < 2) {
      return false;
    }
    for (int i = 2; i * i <= value; i++) {
      if (value % i == 0) {
        return false;
      }
    }
    return true;
  }
}
