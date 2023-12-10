/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;

import java.io.IOException;

/**
 * This class help to get metadata keys.
 */
public final class OmGetKey {

  private final String volumeName;
  private final long volumeID;
  private final String bucketName;
  private final long bucketID;
  private final String keyName;
  private final String fileName;
  private final long parentID;
  private final OMMetadataManager omMetadataManager;

  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmGetKey(String volumeName, long volumeID, String bucketName,
       long bucketID, String keyName, String fileName, long parentID,
       OMMetadataManager omMetadataManager) {
    this.volumeName = volumeName;
    this.volumeID = volumeID;
    this.bucketName = bucketName;
    this.bucketID = bucketID;
    this.keyName = keyName;
    this.fileName = fileName;
    this.parentID = parentID;
    this.omMetadataManager = omMetadataManager;
  }

  /**
   * Builder class for OmGetKey.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private OMMetadataManager omMetadataManager;
    private String errMsg;

    public Builder() {
      this.errMsg = null;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder setOmMetadataManager(OMMetadataManager omMetadataManager) {
      this.omMetadataManager = omMetadataManager;
      return this;
    }

    public Builder setErrMsg(String errMsg) {
      this.errMsg = errMsg;
      return this;
    }

    public OmGetKey build() throws IOException {
      final String fileName = OzoneFSUtils.getFileName(this.keyName);
      final long volumeID = omMetadataManager.getVolumeId(this.volumeName);
      final long bucketID = omMetadataManager.getBucketId(this.volumeName,
          this.bucketName);
      long parentID = OMFileRequest
          .getParentID(volumeID, bucketID, this.keyName,
              this.omMetadataManager, this.errMsg);

      return new OmGetKey(volumeName, volumeID, bucketName, bucketID,
          keyName, fileName, parentID, omMetadataManager);
    }
  }

  public String getVolumeName() {
    return this.volumeName;
  }

  public String getBucketName() {
    return this.bucketName;
  }

  public String getKeyName() {
    return this.keyName;
  }

  public OMMetadataManager getOmMetadataManager() {
    return this.omMetadataManager;
  }


  public long getVolumeID() {
    return volumeID;
  }

  public long getBucketID() {
    return bucketID;
  }

  public String getFileName() {
    return fileName;
  }

  public long getParentID() {
    return parentID;
  }

  public String getFileDBKey() {
    return omMetadataManager.getOzonePathKey(volumeID, bucketID, parentID,
       fileName);
  }

  public String getOpenFileDBKey(long clientID) {
    return omMetadataManager.getOpenFileName(volumeID, bucketID,
        parentID, fileName, clientID);
  }

  public String getMultipartKey(String uploadID) {
    return omMetadataManager.getMultipartKey(volumeID, bucketID, parentID,
        fileName, uploadID);
  }
}
