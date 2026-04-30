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

package org.apache.hadoop.ozone.om.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.response.util.OMEchoRPCWriteResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reflections.Reflections;

/**
 * The test checks whether all {@link OMClientResponse} have defined the
 * {@link CleanupTableInfo} annotation.
 * For certain requests it check whether it is properly defined not just the
 * fact that it is defined.
 */
@ExtendWith(MockitoExtension.class)
public class TestCleanupTableInfo {
  private static final String TEST_VOLUME_NAME = "testVol";
  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final String TEST_KEY = "/foo/bar/baz/key";
  private static final HddsProtos.BlockID TEST_BLOCK_ID =
      new BlockID(1, 1).getProtobuf();
  public static final String OM_RESPONSE_PACKAGE =
      "org.apache.hadoop.ozone.om.response";
  private static final String OM_REQUEST_SOURCE_PATH =
      "hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request";
  private static final Pattern TABLE_GETTER_CACHE_MUTATION =
      Pattern.compile("\\.(get\\w+Table)\\s*\\([^;]*?\\)\\s*\\.addCacheEntry",
          Pattern.DOTALL);
  private static final Pattern TABLE_ALIAS =
      Pattern.compile("(?:Table(?:<[^;]+>)?)\\s+(\\w+)\\s*=\\s*[^;]*" +
          "\\.(get\\w+Table)\\s*\\(", Pattern.DOTALL);
  private static final Pattern RESPONSE_CREATION =
      Pattern.compile("new\\s+([A-Z][A-Za-z0-9]*Response(?:WithFSO)?)\\s*\\(");
  private static final Map<String, String> TABLE_GETTER_TO_NAME =
      tableGetterToName();

  @TempDir
  private Path folder;

  @Mock
  private OMMetrics omMetrics;

  @Mock
  private OMPerformanceMetrics perfMetrics;

  @Mock
  private OzoneManager om;

  /**
   * Creates a mock Ozone Manager object.
   * Defined behaviour in the mock:
   *  - returns the specified metrics instance
   *  - returns the specified metadataManager
   *  - resolves the bucket links to themselves (no symlinks)
   *  - disables ACLs
   *  - provides an audit logger
   *
   * @throws IOException should not happen but declared in mocked methods
   */
  public void setupOzoneManagerMock()
      throws IOException {
    OMMetadataManager metaMgr = createOMMetadataManagerSpy();
    when(om.getMetrics()).thenReturn(omMetrics);
    when(om.getMetadataManager()).thenReturn(metaMgr);
    when(om.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    when(om.getConfig()).thenReturn(new OmConfig());
    addVolumeToMetaTable(aVolumeArgs());
    addBucketToMetaTable(aBucketInfo());
  }

  @Test
  public void checkAnnotationAndTableName() throws IOException {
    setupOzoneManagerMock();
    OMMetadataManager omMetadataManager = om.getMetadataManager();

    Set<String> tables = omMetadataManager.listTableNames();
    Set<Class<? extends OMClientResponse>> subTypes = responseClasses();
    // OMEchoRPCWriteResponse does not need CleanupTable.
    subTypes.remove(OMEchoRPCWriteResponse.class);
    subTypes.remove(DummyOMClientResponse.class);
    subTypes.forEach(aClass -> {
      if (Modifier.isAbstract(aClass.getModifiers())) {
        assertFalse(aClass.isAnnotationPresent(CleanupTableInfo.class),
            aClass + " is an abstract class and should not contain CleanupTableInfo annotations");
        return;
      } else {
        assertTrue(aClass.isAnnotationPresent(CleanupTableInfo.class),
            aClass + " does not have annotation of" +
                " CleanupTableInfo");
      }
      CleanupTableInfo annotation =
          aClass.getAnnotation(CleanupTableInfo.class);
      assertNotNull(annotation, "CleanupTableInfo is null for class " + aClass.getSimpleName());
      String[] cleanupTables = annotation.cleanupTables();
      boolean cleanupAll = annotation.cleanupAll();
      if (cleanupTables.length >= 1) {
        assertTrue(
            Arrays.stream(cleanupTables).allMatch(tables::contains)
        );

      } else {
        assertTrue(cleanupAll);
      }
    });
    reset(om);
  }

  private Set<Class<? extends OMClientResponse>> responseClasses() {
    Reflections reflections = new Reflections(OM_RESPONSE_PACKAGE);
    return reflections.getSubTypesOf(OMClientResponse.class);
  }

  @Test
  public void requestSourcesDeclareCleanupForDirectlyMutatedCacheTables()
      throws IOException {
    Map<String, Class<? extends OMClientResponse>> responsesBySimpleName =
        responseClasses().stream().collect(Collectors.toMap(
            Class::getSimpleName, responseClass -> responseClass));
    List<String> missingCleanupTables = new ArrayList<>();

    try (Stream<Path> sources = Files.walk(requestSourcePath())) {
      List<Path> sourceFiles = sources
          .filter(path -> path.toString().endsWith(".java"))
          .collect(Collectors.toList());

      for (Path sourceFile : sourceFiles) {
        String source = new String(Files.readAllBytes(sourceFile),
            StandardCharsets.UTF_8);
        Set<String> mutatedTables = directCacheMutatedTables(source,
            isFileSystemOptimizedRequest(source, sourceFile));
        if (mutatedTables.isEmpty()) {
          continue;
        }

        Set<String> responseClassNames = responseClassNames(source);
        for (String responseClassName : responseClassNames) {
          Class<? extends OMClientResponse> responseClass =
              responsesBySimpleName.get(responseClassName);
          if (responseClass == null) {
            continue;
          }
          CleanupTableInfo cleanupTableInfo =
              responseClass.getAnnotation(CleanupTableInfo.class);
          assertNotNull(cleanupTableInfo,
              responseClass.getName() + " does not have CleanupTableInfo");
          if (cleanupTableInfo.cleanupAll()) {
            continue;
          }
          List<String> cleanupTables =
              Arrays.asList(cleanupTableInfo.cleanupTables());
          for (String table : mutatedTables) {
            if (!cleanupTables.contains(table)) {
              missingCleanupTables.add(sourceFile + " creates " +
                  responseClassName + " but mutates " + table +
                  " without listing it in CleanupTableInfo");
            }
          }
        }
      }
    }

    assertTrue(missingCleanupTables.isEmpty(),
        String.join(System.lineSeparator(), missingCleanupTables));
  }

  @ParameterizedTest
  @EnumSource(CleanupTableInfoTestCase.class)
  public void testRequestsSetAllTouchedTableCachesForEviction(
      CleanupTableInfoTestCase testCase) throws IOException {
    setupOzoneManagerMock();
    OMClientRequest request = requestFor(testCase);
    Map<String, Integer> cacheItemCount = recordCacheItemCounts();

    request.validateAndUpdateCache(om, 1);

    assertCacheItemCounts(cacheItemCount, testCase.responseClass);
    verifyMetrics(testCase);
  }

  private enum CleanupTableInfoTestCase {
    FILE_CREATE(OMFileCreateResponse.class),
    KEY_CREATE(OMKeyCreateResponse.class),
    VOLUME_CREATE(OMVolumeCreateResponse.class);

    private final Class<? extends OMClientResponse> responseClass;

    CleanupTableInfoTestCase(
        Class<? extends OMClientResponse> responseClass) {
      this.responseClass = responseClass;
    }
  }

  private OMClientRequest requestFor(CleanupTableInfoTestCase testCase) {
    switch (testCase) {
    case FILE_CREATE:
      stubDefaultReplicationConfig();
      return anOMFileCreateRequest();
    case KEY_CREATE:
      stubDefaultReplicationConfig();
      when(om.getEnableFileSystemPaths()).thenReturn(true);
      when(om.getOzoneLockProvider()).thenReturn(
          new OzoneLockProvider(false, false));
      when(om.getPerfMetrics()).thenReturn(perfMetrics);
      return anOMKeyCreateRequest();
    case VOLUME_CREATE:
      return anOMVolumeCreateRequest();
    default:
      throw new IllegalArgumentException("Unexpected test case: " + testCase);
    }
  }

  private void stubDefaultReplicationConfig() {
    when(om.getDefaultReplicationConfig()).thenReturn(ReplicationConfig
        .getDefault(new OzoneConfiguration()));
  }

  private Path requestSourcePath() {
    Path fromRepoRoot = Paths.get(OM_REQUEST_SOURCE_PATH);
    if (Files.exists(fromRepoRoot)) {
      return fromRepoRoot;
    }
    return Paths.get("src/main/java/org/apache/hadoop/ozone/om/request");
  }

  private Set<String> directCacheMutatedTables(String source,
      boolean fileSystemOptimizedRequest) {
    Set<String> mutatedTables = new LinkedHashSet<>();
    Map<String, String> tableGetterToName =
        tableGetterToName(fileSystemOptimizedRequest);
    Matcher directMatcher = TABLE_GETTER_CACHE_MUTATION.matcher(source);
    while (directMatcher.find()) {
      String tableName = tableGetterToName.get(directMatcher.group(1));
      if (tableName != null) {
        mutatedTables.add(tableName);
      }
    }

    Matcher aliasMatcher = TABLE_ALIAS.matcher(source);
    Map<String, String> tableAliases = new HashMap<>();
    while (aliasMatcher.find()) {
      String tableName = tableGetterToName.get(aliasMatcher.group(2));
      if (tableName != null) {
        tableAliases.put(aliasMatcher.group(1), tableName);
      }
    }
    tableAliases.forEach((alias, tableName) -> {
      Pattern aliasCacheMutation =
          Pattern.compile("\\b" + Pattern.quote(alias) +
              "\\s*\\.addCacheEntry");
      if (aliasCacheMutation.matcher(source).find()) {
        mutatedTables.add(tableName);
      }
    });
    return mutatedTables;
  }

  private Set<String> responseClassNames(String source) {
    Set<String> responseClassNames = new LinkedHashSet<>();
    Matcher responseMatcher = RESPONSE_CREATION.matcher(source);
    while (responseMatcher.find()) {
      responseClassNames.add(responseMatcher.group(1));
    }
    return responseClassNames;
  }

  private boolean isFileSystemOptimizedRequest(String source, Path sourceFile) {
    return sourceFile.toString().contains("WithFSO") ||
        source.contains("FILE_SYSTEM_OPTIMIZED");
  }

  private static Map<String, String> tableGetterToName(
      boolean fileSystemOptimizedRequest) {
    Map<String, String> tableGetterToName =
        new LinkedHashMap<>(TABLE_GETTER_TO_NAME);
    if (fileSystemOptimizedRequest) {
      tableGetterToName.put("getKeyTable", OMDBDefinition.FILE_TABLE);
      tableGetterToName.put("getOpenKeyTable",
          OMDBDefinition.OPEN_FILE_TABLE);
    }
    return tableGetterToName;
  }

  private static Map<String, String> tableGetterToName() {
    Map<String, String> tableGetterToName = new LinkedHashMap<>();
    for (Field field : OMDBDefinition.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) &&
          field.getType().equals(String.class) &&
          field.getName().endsWith("_TABLE")) {
        String getterName = tableGetterName(field.getName());
        if (hasTableGetter(getterName)) {
          tableGetterToName.put(getterName, tableName(field));
        }
      }
    }
    return tableGetterToName;
  }

  private static boolean hasTableGetter(String getterName) {
    for (Method method : OMMetadataManager.class.getMethods()) {
      if (method.getName().equals(getterName)) {
        return true;
      }
    }
    return false;
  }

  private static String tableGetterName(String tableConstantName) {
    String tableName = tableConstantName.substring(0,
        tableConstantName.length() - "_TABLE".length());
    StringBuilder getterName = new StringBuilder("get");
    for (String word : tableName.toLowerCase(Locale.ROOT).split("_")) {
      getterName.append(Character.toUpperCase(word.charAt(0)))
          .append(word.substring(1));
    }
    return getterName.append("Table").toString();
  }

  private static String tableName(Field tableNameField) {
    try {
      return (String)tableNameField.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot read table name from " +
          tableNameField, e);
    }
  }

  private void verifyMetrics(CleanupTableInfoTestCase testCase) {
    switch (testCase) {
    case FILE_CREATE:
      verify(omMetrics, times(1)).incNumCreateFile();
      break;
    case KEY_CREATE:
      verify(omMetrics, times(1)).incNumKeyAllocates();
      break;
    case VOLUME_CREATE:
      verify(omMetrics, times(1)).incNumVolumeCreates();
      break;
    default:
      throw new IllegalArgumentException("Unexpected test case: " + testCase);
    }
  }

  private Map<String, Integer> recordCacheItemCounts() {
    Map<String, Integer> cacheItemCount = new HashMap<>();
    for (String tableName : om.getMetadataManager().listTableNames()) {
      cacheItemCount.put(
          tableName,
          Iterators.size(
              om.getMetadataManager().getTable(tableName).cacheIterator()
          )
      );
    }
    return cacheItemCount;
  }

  private void assertCacheItemCounts(
      Map<String, Integer> cacheItemCount,
      Class<? extends OMClientResponse> responseClass
  ) {
    CleanupTableInfo ann = responseClass.getAnnotation(CleanupTableInfo.class);
    assertNotNull(ann, "CleanupTableInfo is null for class " +
        responseClass.getSimpleName());
    if (ann.cleanupAll()) {
      return;
    }
    List<String> cleanup = Arrays.asList(ann.cleanupTables());
    for (String tableName : om.getMetadataManager().listTableNames()) {
      if (!cleanup.contains(tableName)) {
        assertEquals(cacheItemCount.get(tableName).intValue(),
            Iterators.size(
                om.getMetadataManager().getTable(tableName).cacheIterator()
            ), "Cache item count of table " + tableName);
      }
    }
  }

  /**
   * Adds the volume info to the volumeTable in the MetadataManager, and also
   * add the value to the table's cache.
   *
   * @param volumeArgs the OMVolumeArgs object specifying the volume propertes
   * @throws IOException if an IO issue occurs while wrtiing to RocksDB
   */
  private void addVolumeToMetaTable(OmVolumeArgs volumeArgs)
      throws IOException {
    String volumeKey = om.getMetadataManager().getVolumeKey(TEST_VOLUME_NAME);
    om.getMetadataManager().getVolumeTable().put(volumeKey, volumeArgs);
    om.getMetadataManager().getVolumeTable().addCacheEntry(
        new CacheKey<>(volumeKey),
        CacheValue.get(2, volumeArgs)
    );
  }

  /**
   * Adds the bucket info to the bucketTable in the MetadataManager, and also
   * adds the value to the table's cache.
   *
   * @param bucketInfo the OMBucketInfo object specifying the bucket properties
   * @throws IOException if an IO issue occurs while writing to RocksDB
   */
  private void addBucketToMetaTable(OmBucketInfo bucketInfo)
      throws IOException {
    String bucketKey = om.getMetadataManager()
        .getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    om.getMetadataManager().getBucketTable().put(bucketKey, bucketInfo);
    om.getMetadataManager().getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey),
        CacheValue.get(1, bucketInfo)
    );
  }

  /**
   * Creates a spy object over an instantiated OMMetadataManager, giving the
   * possibility to redefine behaviour. In the current implementation
   * there isn't any behaviour which is redefined.
   *
   * @return the OMMetadataManager spy instance created.
   * @throws IOException if I/O error occurs in setting up data store for the
   *                     metadata manager.
   */
  private OMMetadataManager createOMMetadataManagerSpy() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return spy(new OmMetadataManagerImpl(conf, null));
  }

  private OMFileCreateRequest anOMFileCreateRequest() {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateFileRequest()).thenReturn(aCreateFileRequest());
    when(protoRequest.getCmdType()).thenReturn(Type.CreateFile);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMFileCreateRequest(protoRequest,
        aBucketInfo().getBucketLayout());
  }

  private OMKeyCreateRequest anOMKeyCreateRequest() {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateKeyRequest()).thenReturn(aKeyCreateRequest());
    when(protoRequest.getCmdType()).thenReturn(Type.CreateKey);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMKeyCreateRequest(protoRequest,
        aBucketInfo().getBucketLayout());
  }

  private OMVolumeCreateRequest anOMVolumeCreateRequest() {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateVolumeRequest())
        .thenReturn(aCreateVolumeRequest());
    when(protoRequest.getCmdType()).thenReturn(Type.CreateVolume);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMVolumeCreateRequest(protoRequest);
  }

  private OmBucketInfo aBucketInfo() {
    return OmBucketInfo.newBuilder()
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.DEFAULT)
        .build();
  }

  private OmVolumeArgs aVolumeArgs() {
    return OmVolumeArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName("owner")
        .setVolume(TEST_VOLUME_NAME)
        .build();
  }

  private CreateFileRequest aCreateFileRequest() {
    return CreateFileRequest.newBuilder()
        .setKeyArgs(aKeyArgs())
        .setIsRecursive(true)
        .setIsOverwrite(false)
        .setClientID(1L)
        .build();
  }

  private CreateKeyRequest aKeyCreateRequest() {
    return CreateKeyRequest.newBuilder()
        .setKeyArgs(aKeyArgs())
        .setClientID(1L)
        .build();
  }

  private CreateVolumeRequest aCreateVolumeRequest() {
    VolumeInfo volumeInfo = VolumeInfo.newBuilder()
        .setVolume(TEST_VOLUME_NAME + "-new")
        .setAdminName("admin")
        .setOwnerName("owner")
        .build();
    return CreateVolumeRequest.newBuilder()
        .setVolumeInfo(volumeInfo)
        .build();
  }

  private KeyArgs aKeyArgs() {
    return KeyArgs.newBuilder()
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .setKeyName(TEST_KEY)
        .setDataSize(512L)
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
        .build();
  }

  private KeyLocation aKeyLocation(
      HddsProtos.BlockID blockID) {
    return KeyLocation.newBuilder()
        .setBlockID(blockID)
        .setOffset(0)
        .setLength(512)
        .setCreateVersion(0)
        .setPipeline(aPipeline())
        .build();
  }

  private Pipeline aPipeline() {
    return Pipeline.newBuilder()
        .setId(aPipelineID())
        .addMembers(aDatanodeDetailsProto("192.168.1.1", "host1"))
        .addMembers(aDatanodeDetailsProto("192.168.1.2", "host2"))
        .addMembers(aDatanodeDetailsProto("192.168.1.3", "host3"))
        .build();
  }

  private DatanodeDetailsProto aDatanodeDetailsProto(String s,
                                                     String host1) {
    return DatanodeDetailsProto.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress(s)
        .setHostName(host1)
        .build();
  }

  private HddsProtos.PipelineID aPipelineID() {
    return HddsProtos.PipelineID.newBuilder()
        .setId(UUID.randomUUID().toString())
        .build();
  }
}
