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

package org.apache.hadoop.ozone.s3.awssdk.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;



/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from
 * - https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/javav2/example_code/s3/src/main/java/com/example/s3
 * - https://github.com/ceph/s3-tests
 *
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class AbstractS3SDKV2Tests extends OzoneTestBase {

  private static MiniOzoneCluster cluster = null;
  private static S3Client s3Client = null;

  /**
   * Create a MiniOzoneCluster with S3G enabled for testing.
   * @param conf Configurations to start the cluster
   * @throws Exception exception thrown when waiting for the cluster to be ready.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .includeS3G(true)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    s3Client = cluster.newS3ClientV2();
  }

  /**
   * Shutdown the MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static String calculateChecksum(InputStream is, String algorithm) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    try (DigestInputStream dis = new DigestInputStream(is, md)) {
      byte[] buffer = new byte[8192];
      int numBytesRead = 0;
      while (numBytesRead != -1) {
        numBytesRead = dis.read(buffer);
      }
      return Base64.getEncoder().encodeToString(md.digest());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testPutObjectWithPrecalculatedChecksum() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(bucketName));
//    final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
        .bucket(bucketName)
        .key(keyName)
        .checksumAlgorithm(ChecksumAlgorithm.CRC32),
        RequestBody.fromString(content));

//    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", putObjectResponse.eTag());

//    GetObjectResponse getObjectResponse = s3Client.getObject(
//        b -> b.bucket(bucketName).key(keyName)).response();

    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(keyName)
    );

    assertEquals(content, objectBytes.asUtf8String());
  }

  @Test
  public void testMultipartUploadWithPrecalculatedChecksum() {

  }

  private String getBucketName() {
    return getBucketName(null);
  }

  private String getBucketName(String suffix) {
    return (getTestName() + "bucket" + suffix).toLowerCase(Locale.ROOT);
  }

  private String getKeyName() {
    return getKeyName(null);
  }

  private String getKeyName(String suffix) {
    return (getTestName() +  "key" + suffix).toLowerCase(Locale.ROOT);
  }

}
