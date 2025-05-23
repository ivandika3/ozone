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

package org.apache.hadoop.hdds.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * BlockID returned by SCM during allocation of block (containerID + localID).
 */
public class ContainerBlockID {
  private final long containerID;
  private final long localID;

  public ContainerBlockID(long containerID, long localID) {
    this.containerID = containerID;
    this.localID = localID;
  }

  public long getContainerID() {
    return containerID;
  }

  public long getLocalID() {
    return localID;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(48);
    appendTo(sb);
    return sb.toString();
  }

  public void appendTo(StringBuilder sb) {
    sb.append("conID: ").append(containerID)
        .append(" locID: ").append(localID);
  }

  @JsonIgnore
  public HddsProtos.ContainerBlockID getProtobuf() {
    return HddsProtos.ContainerBlockID.newBuilder().
        setContainerID(containerID).setLocalID(localID).build();
  }

  @JsonIgnore
  public static ContainerBlockID getFromProtobuf(
      HddsProtos.ContainerBlockID containerBlockID) {
    return new ContainerBlockID(containerBlockID.getContainerID(),
        containerBlockID.getLocalID());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerBlockID blockID = (ContainerBlockID) o;
    return containerID == blockID.containerID && localID == blockID.localID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerID, localID);
  }
}
