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

package org.apache.hadoop.ozone.shell.bucket;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketForkInfo;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.PrefixFilterOption;
import org.apache.hadoop.ozone.shell.volume.VolumeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * List bucket fork handler.
 */
@Command(name = "list",
    aliases = "ls",
    description = "lists bucket forks in a volume")
public class ListBucketForkHandler extends VolumeHandler {

  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  @CommandLine.Mixin
  private PrefixFilterOption prefixFilter;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    Iterator<BucketForkInfo> iterator = client.getObjectStore()
        .listBucketForks(address.getVolumeName(), prefixFilter.getPrefix(),
            listOptions.getStartItem());
    int count = printAsJsonArray(toJsonIterator(iterator),
        listOptions.getLimit());
    if (isVerbose()) {
      err().printf("Found : %d bucket forks for volume : %s ", count,
          address.getVolumeName());
    }
  }

  private Iterator<BucketForkInfoJson> toJsonIterator(
      Iterator<BucketForkInfo> iterator) {
    return new Iterator<BucketForkInfoJson>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public BucketForkInfoJson next() {
        return BucketForkInfoJson.of(iterator.next());
      }
    };
  }
}
