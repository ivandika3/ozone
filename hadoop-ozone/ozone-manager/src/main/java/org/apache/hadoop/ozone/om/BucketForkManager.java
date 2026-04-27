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
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

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
    if (baseKeyInfo == null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }
    return rewriteBaseKeyInfo(forkInfo, baseKeyInfo, targetArgs.getKeyName());
  }

  public OzoneFileStatus lookupBaseFileStatus(BucketForkInfo forkInfo,
      OmKeyArgs targetArgs, IOmMetadataReader baseReader) throws IOException {
    if (getTombstoneInfo(forkInfo, targetArgs.getKeyName()) != null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }

    OmKeyArgs sourceArgs = targetArgs.toBuilder()
        .setVolumeName(forkInfo.getSourceVolumeName())
        .setBucketName(forkInfo.getSourceBucketName())
        .build();
    OzoneFileStatus baseFileStatus = baseReader.getFileStatus(sourceArgs);
    if (baseFileStatus == null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }
    return rewriteBaseFileStatus(forkInfo, baseFileStatus);
  }

  public OmKeyInfo lookupBaseFile(BucketForkInfo forkInfo,
      OmKeyArgs targetArgs, IOmMetadataReader baseReader) throws IOException {
    if (getTombstoneInfo(forkInfo, targetArgs.getKeyName()) != null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }

    OmKeyArgs sourceArgs = targetArgs.toBuilder()
        .setVolumeName(forkInfo.getSourceVolumeName())
        .setBucketName(forkInfo.getSourceBucketName())
        .build();
    OmKeyInfo baseKeyInfo = baseReader.lookupFile(sourceArgs);
    if (baseKeyInfo == null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }
    return rewriteBaseKeyInfo(forkInfo, baseKeyInfo, targetArgs.getKeyName());
  }

  public OmKeyInfo getForkBaseKeyForCopyOnWrite(OzoneManager ozoneManager,
      String volumeName, String bucketName, String keyName,
      long forkObjectId, long updateId) throws IOException {
    BucketForkInfo forkInfo = getActiveForkInfo(volumeName, bucketName);
    if (forkInfo == null) {
      return null;
    }

    OmKeyArgs targetArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    OmKeyInfo baseKeyInfo;
    try (UncheckedAutoCloseableSupplier<? extends IOmMetadataReader> snapshot =
             ozoneManager.getOmSnapshotManager().getSnapshot(
                 forkInfo.getBaseSnapshotId())) {
      baseKeyInfo = lookupBaseKey(forkInfo, targetArgs, snapshot.get());
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND) {
        return null;
      }
      throw ex;
    }

    OzoneManagerProtocolProtos.KeyInfo forkKeyProto =
        baseKeyInfo.getProtobuf(ClientVersion.CURRENT_VERSION).toBuilder()
            .setObjectID(forkObjectId)
            .setUpdateID(updateId)
            .build();
    return OmKeyInfo.getFromProtobuf(forkKeyProto);
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

  private OzoneFileStatus rewriteBaseFileStatus(BucketForkInfo forkInfo,
      OzoneFileStatus baseFileStatus) {
    OmKeyInfo baseKeyInfo = baseFileStatus.getKeyInfo();
    OmKeyInfo forkKeyInfo = baseKeyInfo == null ? null
        : rewriteBaseKeyInfo(forkInfo, baseKeyInfo,
            logicalBaseKeyName(forkInfo, baseKeyInfo.getKeyName()));
    return new OzoneFileStatus(forkKeyInfo, baseFileStatus.getBlockSize(),
        baseFileStatus.isDirectory());
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
