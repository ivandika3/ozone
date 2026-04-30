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

package org.apache.hadoop.hdds.utils.db;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tracks table caches updated by the current thread.
 */
public final class TableCacheUpdateTracker {

  private static final ThreadLocal<Set<String>> UPDATED_TABLES =
      new ThreadLocal<>();
  private static final Set<String> TRACKING = Collections.emptySet();

  private TableCacheUpdateTracker() {
  }

  public static void startTracking() {
    UPDATED_TABLES.set(TRACKING);
  }

  public static Set<String> stopTracking() {
    Set<String> tables = UPDATED_TABLES.get();
    UPDATED_TABLES.remove();
    if (tables == null || tables == TRACKING || tables.isEmpty()) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(new LinkedHashSet<>(tables));
  }

  public static void recordCacheUpdate(String tableName) {
    Set<String> tables = UPDATED_TABLES.get();
    if (tables != null && tableName != null && !tableName.isEmpty()) {
      if (tables == TRACKING) {
        tables = new LinkedHashSet<>();
        UPDATED_TABLES.set(tables);
      }
      tables.add(tableName);
    }
  }
}
