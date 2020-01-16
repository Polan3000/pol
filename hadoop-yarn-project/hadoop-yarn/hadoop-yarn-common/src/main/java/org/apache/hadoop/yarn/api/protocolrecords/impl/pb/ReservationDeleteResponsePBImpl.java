/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteResponseProto;

import org.apache.hadoop.thirdparty.protobuf_3_7.TextFormat;

public class ReservationDeleteResponsePBImpl extends ReservationDeleteResponse {

  ReservationDeleteResponseProto proto = ReservationDeleteResponseProto
      .getDefaultInstance();
  ReservationDeleteResponseProto.Builder builder = null;
  boolean viaProto = false;

  public ReservationDeleteResponsePBImpl() {
    builder = ReservationDeleteResponseProto.newBuilder();
  }

  public ReservationDeleteResponsePBImpl(ReservationDeleteResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationDeleteResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}
