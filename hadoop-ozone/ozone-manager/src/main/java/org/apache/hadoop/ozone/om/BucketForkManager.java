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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.om.helpers.BucketForkTombstoneInfo;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Coordinates bucket-fork metadata overlay reads.
 *
 * This class owns fork-active checks, tombstone filtering, base snapshot
 * fallback, namespace rewriting, and sorted read merges. Request classes still
 * own write-side actions such as copy-on-write, tombstone creation, quota
 * accounting, and cache updates because those depend on request locks,
 * transaction indexes, and per-operation response semantics.
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
    BucketForkTombstoneInfo exactTombstoneInfo =
        getExactTombstoneInfo(forkInfo, logicalPath);
    if (exactTombstoneInfo != null) {
      return exactTombstoneInfo;
    }

    String parentPath = normalizeLogicalPath(logicalPath);
    int slashIndex = parentPath.lastIndexOf('/');
    while (slashIndex > 0) {
      parentPath = parentPath.substring(0, slashIndex);
      BucketForkTombstoneInfo ancestorTombstoneInfo =
          getExactTombstoneInfo(forkInfo, parentPath);
      if (isSubtreeTombstone(ancestorTombstoneInfo)) {
        return ancestorTombstoneInfo;
      }
      slashIndex = parentPath.lastIndexOf('/');
    }
    return null;
  }

  private BucketForkTombstoneInfo getExactTombstoneInfo(
      BucketForkInfo forkInfo, String logicalPath) throws IOException {
    return metadataManager.getBucketForkTombstoneTable().get(
        BucketForkTombstoneInfo.getTableKey(
            forkInfo.getTargetVolumeName(),
            forkInfo.getTargetBucketName(),
            logicalPath));
  }

  private boolean isSubtreeTombstone(
      BucketForkTombstoneInfo tombstoneInfo) {
    if (tombstoneInfo == null) {
      return false;
    }
    return tombstoneInfo.getType()
        == BucketForkTombstoneInfo.BucketForkTombstoneType.DIRECTORY
        || tombstoneInfo.getType()
        == BucketForkTombstoneInfo.BucketForkTombstoneType.PREFIX;
  }

  private String normalizeLogicalPath(String logicalPath) {
    String normalizedPath = logicalPath;
    while (normalizedPath.startsWith("/")) {
      normalizedPath = normalizedPath.substring(1);
    }
    return normalizedPath;
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

  public KeyInfoWithVolumeContext getBaseKeyInfo(BucketForkInfo forkInfo,
      OmKeyArgs targetArgs, IOmMetadataReader baseReader,
      boolean assumeS3Context) throws IOException {
    if (getTombstoneInfo(forkInfo, targetArgs.getKeyName()) != null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }

    OmKeyArgs sourceArgs = targetArgs.toBuilder()
        .setVolumeName(forkInfo.getSourceVolumeName())
        .setBucketName(forkInfo.getSourceBucketName())
        .build();
    KeyInfoWithVolumeContext baseKeyInfo =
        baseReader.getKeyInfo(sourceArgs, assumeS3Context);
    if (baseKeyInfo == null || baseKeyInfo.getKeyInfo() == null) {
      throw new OMException("Key:" + targetArgs.getKeyName() + " not found",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }
    return KeyInfoWithVolumeContext.newBuilder()
        .setKeyInfo(rewriteBaseKeyInfo(forkInfo, baseKeyInfo.getKeyInfo(),
            targetArgs.getKeyName()))
        .setVolumeArgs(baseKeyInfo.getVolumeArgs().orElse(null))
        .setUserPrincipal(baseKeyInfo.getUserPrincipal().orElse(null))
        .build();
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

    TreeMap<String, OmKeyInfo> mergedKeys = new TreeMap<>();
    for (OmKeyInfo forkLocalKey : forkLocalKeys.getKeys()) {
      mergedKeys.put(forkLocalKey.getKeyName(), forkLocalKey);
    }

    int readLimit = getListKeysReadLimit(maxKeys);
    String baseStartKey = startKey;
    boolean baseTruncated = false;
    boolean baseMayHaveMore;
    do {
      ListKeysResult baseKeys = baseReader.listKeys(
          forkInfo.getSourceVolumeName(), forkInfo.getSourceBucketName(),
          baseStartKey, keyPrefix, readLimit);
      baseTruncated = baseKeys.isTruncated();

      String lastLogicalKeyName = null;
      for (OmKeyInfo baseKey : baseKeys.getKeys()) {
        String logicalKeyName = logicalBaseKeyName(forkInfo,
            baseKey.getKeyName());
        lastLogicalKeyName = logicalKeyName;
        if (!mergedKeys.containsKey(logicalKeyName)
            && getTombstoneInfo(forkInfo, logicalKeyName) == null) {
          mergedKeys.put(logicalKeyName,
              rewriteBaseKeyInfo(forkInfo, baseKey, logicalKeyName));
        }
      }

      baseMayHaveMore = baseTruncated && lastLogicalKeyName != null
          && !lastLogicalKeyName.equals(baseStartKey);
      baseStartKey = lastLogicalKeyName;
    } while (baseMayHaveMore && mergedKeys.size() <= maxKeys);

    int currentCount = 0;
    for (Map.Entry<String, OmKeyInfo> entry : mergedKeys.entrySet()) {
      result.add(entry.getValue());
      currentCount++;
      if (currentCount == maxKeys) {
        break;
      }
    }

    boolean isTruncated = mergedKeys.size() > maxKeys
        || forkLocalKeys.isTruncated() || baseTruncated;
    return new ListKeysResult(result, isTruncated);
  }

  public List<OzoneFileStatus> listStatus(BucketForkInfo forkInfo,
      List<OzoneFileStatus> forkLocalStatuses, ListStatusContext context,
      IOmMetadataReader baseReader) throws IOException {
    if (context.maxEntries <= 0) {
      return Collections.emptyList();
    }

    OmKeyArgs sourceArgs = context.targetArgs.toBuilder()
        .setVolumeName(forkInfo.getSourceVolumeName())
        .setBucketName(forkInfo.getSourceBucketName())
        .build();
    TreeMap<String, OzoneFileStatus> mergedStatuses = new TreeMap<>();
    for (OzoneFileStatus forkLocalStatus : forkLocalStatuses) {
      mergedStatuses.put(statusKeyName(forkLocalStatus,
          context.targetArgs.getKeyName()), forkLocalStatus);
    }

    long readLimit = getListStatusReadLimit(context.maxEntries);
    String baseStartKey = context.startKey;
    boolean baseMayHaveMore;
    do {
      List<OzoneFileStatus> baseStatuses;
      try {
        baseStatuses = baseReader.listStatus(sourceArgs, context.recursive,
            baseStartKey, readLimit, context.allowPartialPrefixes);
      } catch (OMException ex) {
        if (!isFileOrKeyNotFound(ex) || context.forkLocalMissing) {
          throw ex;
        }
        baseStatuses = Collections.emptyList();
      }

      String lastLogicalKeyName = null;
      for (OzoneFileStatus baseStatus : baseStatuses) {
        String logicalKeyName = logicalStatusKeyName(forkInfo, baseStatus,
            context.targetArgs.getKeyName());
        lastLogicalKeyName = logicalKeyName;
        if (!mergedStatuses.containsKey(logicalKeyName)
            && getTombstoneInfo(forkInfo, logicalKeyName) == null) {
          mergedStatuses.put(logicalKeyName,
              rewriteBaseFileStatus(forkInfo, baseStatus));
        }
      }

      baseMayHaveMore = baseStatuses.size() == readLimit
          && lastLogicalKeyName != null
          && !lastLogicalKeyName.equals(baseStartKey);
      baseStartKey = lastLogicalKeyName;
    } while (baseMayHaveMore && mergedStatuses.size() <= context.maxEntries);

    List<OzoneFileStatus> result = new ArrayList<>();
    long currentCount = 0;
    for (OzoneFileStatus status : mergedStatuses.values()) {
      result.add(status);
      currentCount++;
      if (currentCount == context.maxEntries) {
        break;
      }
    }
    return result;
  }

  public int getListKeysReadLimit(int maxKeys) {
    return maxKeys == Integer.MAX_VALUE ? maxKeys : maxKeys + 1;
  }

  public long getListStatusReadLimit(long maxEntries) {
    return maxEntries == Long.MAX_VALUE ? maxEntries : maxEntries + 1;
  }

  private boolean isFileOrKeyNotFound(OMException ex) {
    return ex.getResult() == OMException.ResultCodes.KEY_NOT_FOUND
        || ex.getResult() == OMException.ResultCodes.FILE_NOT_FOUND;
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

  private String logicalStatusKeyName(BucketForkInfo forkInfo,
      OzoneFileStatus status, String fallbackKeyName) {
    return logicalBaseKeyName(forkInfo, statusKeyName(status, fallbackKeyName));
  }

  private String statusKeyName(OzoneFileStatus status, String fallbackKeyName) {
    return status.getKeyInfo() == null ? fallbackKeyName
        : status.getKeyInfo().getKeyName();
  }

  private String logicalBaseKeyName(BucketForkInfo forkInfo, String keyName) {
    String snapshotPrefix = OmSnapshotManager.getSnapshotPrefix(
        forkInfo.getBaseSnapshotName());
    if (keyName != null && keyName.startsWith(snapshotPrefix)) {
      return keyName.substring(snapshotPrefix.length());
    }
    return keyName;
  }

  /**
   * Immutable arguments for merging fork-local and base snapshot file listings.
   */
  public static final class ListStatusContext {
    private final OmKeyArgs targetArgs;
    private final boolean recursive;
    private final String startKey;
    private final long maxEntries;
    private final boolean allowPartialPrefixes;
    private final boolean forkLocalMissing;

    public ListStatusContext(OmKeyArgs targetArgs, boolean recursive,
        String startKey, long maxEntries, boolean allowPartialPrefixes,
        boolean forkLocalMissing) {
      this.targetArgs = targetArgs;
      this.recursive = recursive;
      this.startKey = startKey;
      this.maxEntries = maxEntries;
      this.allowPartialPrefixes = allowPartialPrefixes;
      this.forkLocalMissing = forkLocalMissing;
    }
  }
}
