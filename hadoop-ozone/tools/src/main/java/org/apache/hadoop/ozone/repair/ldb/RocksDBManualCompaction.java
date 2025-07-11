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

package org.apache.hadoop.ozone.repair.ldb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.hadoop.util.Time;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import picocli.CommandLine;

/**
 * Tool to perform compaction on a table.
 */
@CommandLine.Command(
    name = "compact",
    description = "CLI to compact a column-family in the DB while the service is offline.\n" +
        "Note: If om.db is compacted with this tool then it will negatively impact " +
        "the Ozone Manager's efficient snapshot diff.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class RocksDBManualCompaction extends RepairTool {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--column-family", "--column_family", "--cf"},
      required = true,
      description = "Column family name")
  private String columnFamilyName;

  @Override
  public void execute() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);

    ServiceLoader<DBDefinition> dbDefinitionServiceLoader = ServiceLoader.load(DBDefinition.class);
    DBDefinition dbDefinition = null;
    Set<String> dbColumnFamilyNames =
        cfDescList.stream().map(ColumnFamilyDescriptor::getName).map(StringUtils::bytes2String)
            .collect(Collectors.toSet());
    for (DBDefinition def : dbDefinitionServiceLoader) {
      Set<String> definitionColumnFamilyNames = new HashSet<>(def.getColumnFamilyNames());
      if (definitionColumnFamilyNames.containsAll(dbColumnFamilyNames)) {
        dbDefinition = def;
        break;
      }
    }

    if (dbDefinition == null) {
      throw new IllegalArgumentException("The DB column family does not match any known DB definition, " +
          "column families: " + Arrays.toString(dbColumnFamilyNames.toArray()));
    }

    File dbDir = new File(dbPath);
    try (RDBStore db = DBStoreBuilder.newBuilder(getOzoneConf(), dbDefinition, dbDir).build()) {
      ColumnFamily cfh = db.getDb().getColumnFamily(columnFamilyName);
      if (cfh == null) {
        throw new IllegalArgumentException(columnFamilyName +
            " is not in a column family in DB for the given path.");
      }

      info("Running compaction on " + columnFamilyName);
      long startTime = Time.monotonicNow();
      if (!isDryRun()) {
        ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions();
        compactOptions.setBottommostLevelCompaction(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
        db.compactTable(columnFamilyName, compactOptions);
      }
      long duration = Time.monotonicNow() - startTime;
      info("Compaction completed in " + duration + "ms.");
    } catch (RocksDatabaseException exception) {
      error("Exception: " + exception);
      String errorMsg = "Failed to compact RocksDB for the given path: " + dbPath +
          ", column family: " + columnFamilyName;
      throw new IOException(errorMsg, exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }
  }
}
