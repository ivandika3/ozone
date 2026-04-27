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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

/**
 * Coordinates bucket-fork metadata lookups for future overlay reads.
 */
public class BucketForkManager {
  private final OMMetadataManager metadataManager;

  public BucketForkManager(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  public BucketForkInfo getActiveForkInfo(String volumeName,
      String bucketName) throws IOException {
    BucketForkInfo forkInfo = metadataManager.getBucketForkTable()
        .get(BucketForkInfo.getTableKey(volumeName, bucketName));
    if (forkInfo == null || !forkInfo.isActive()) {
      return null;
    }
    return forkInfo;
  }

  public boolean isForkBucket(String volumeName, String bucketName)
      throws IOException {
    return getActiveForkInfo(volumeName, bucketName) != null;
  }

  public BucketForkTombstoneInfo getTombstoneInfo(
      BucketForkInfo forkInfo, String logicalPath) throws IOException {
    return metadataManager.getBucketForkTombstoneTable().get(
        BucketForkTombstoneInfo.getTableKey(
            forkInfo.getTargetVolumeName(),
            forkInfo.getTargetBucketName(),
            logicalPath));
  }

  public OmKeyInfo lookupBaseKey(BucketForkInfo forkInfo,
      OmKeyArgs targetArgs, IOmMetadataReader baseReader) throws IOException {
    if (getTombstoneInfo(forkInfo, targetArgs.getKeyName()) != null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }

    OmKeyArgs sourceArgs = targetArgs.toBuilder()
        .setVolumeName(forkInfo.getSourceVolumeName())
        .setBucketName(forkInfo.getSourceBucketName())
        .build();
    OmKeyInfo baseKeyInfo = baseReader.lookupKey(sourceArgs);
    return rewriteBaseKeyInfo(forkInfo, baseKeyInfo, targetArgs.getKeyName());
  }

  public ListKeysResult listKeys(BucketForkInfo forkInfo,
      ListKeysResult forkLocalKeys, String startKey, String keyPrefix,
      int maxKeys, IOmMetadataReader baseReader) throws IOException {
    List<OmKeyInfo> result = new ArrayList<>();
    if (maxKeys <= 0) {
      return new ListKeysResult(result, false);
    }

    int readLimit = getListKeysReadLimit(maxKeys);
    ListKeysResult baseKeys = baseReader.listKeys(
        forkInfo.getSourceVolumeName(), forkInfo.getSourceBucketName(),
        startKey, keyPrefix, readLimit);

    TreeMap<String, OmKeyInfo> mergedKeys = new TreeMap<>();
    for (OmKeyInfo forkLocalKey : forkLocalKeys.getKeys()) {
      mergedKeys.put(forkLocalKey.getKeyName(), forkLocalKey);
    }

    for (OmKeyInfo baseKey : baseKeys.getKeys()) {
      String logicalKeyName = logicalBaseKeyName(forkInfo,
          baseKey.getKeyName());
      if (!mergedKeys.containsKey(logicalKeyName)
          && getTombstoneInfo(forkInfo, logicalKeyName) == null) {
        mergedKeys.put(logicalKeyName,
            rewriteBaseKeyInfo(forkInfo, baseKey, logicalKeyName));
      }
    }

    int currentCount = 0;
    for (Map.Entry<String, OmKeyInfo> entry : mergedKeys.entrySet()) {
      result.add(entry.getValue());
      currentCount++;
      if (currentCount == maxKeys) {
        break;
      }
    }

    boolean isTruncated = mergedKeys.size() > maxKeys
        || forkLocalKeys.isTruncated() || baseKeys.isTruncated();
    return new ListKeysResult(result, isTruncated);
  }

  public int getListKeysReadLimit(int maxKeys) {
    return maxKeys == Integer.MAX_VALUE ? maxKeys : maxKeys + 1;
  }

  private OmKeyInfo rewriteBaseKeyInfo(BucketForkInfo forkInfo,
      OmKeyInfo baseKeyInfo, String logicalKeyName) {
    return baseKeyInfo.toBuilder()
        .setVolumeName(forkInfo.getTargetVolumeName())
        .setBucketName(forkInfo.getTargetBucketName())
        .setKeyName(logicalKeyName)
        .build();
  }

  private String logicalBaseKeyName(BucketForkInfo forkInfo, String keyName) {
    String snapshotPrefix = OmSnapshotManager.getSnapshotPrefix(
        forkInfo.getBaseSnapshotName());
    if (keyName != null && keyName.startsWith(snapshotPrefix)) {
      return keyName.substring(snapshotPrefix.length());
    }
    return keyName;
  }
}
