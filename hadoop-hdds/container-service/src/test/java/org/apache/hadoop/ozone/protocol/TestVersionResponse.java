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

package org.apache.hadoop.ozone.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for version response metadata.
 */
class TestVersionResponse {

  @ParameterizedTest
  @CsvSource({
      "true, true",
      "TRUE, true",
      "TrUe, true",
      "false, false",
      "FALSE, false",
      "invalid, false",
      "'', false",
      "' true ', false"
  })
  void testGetBooleanValue(String value, boolean expected) {
    VersionResponse response = VersionResponse.newBuilder().addValue("enabled", value).build();

    assertEquals(expected, response.getBooleanValue("enabled"));
  }

  @Test
  void testGetBooleanValueDefaultsToFalseWhenMissing() {
    VersionResponse response = new VersionResponse(1);

    assertFalse(response.getBooleanValue("enabled"));
  }
}
