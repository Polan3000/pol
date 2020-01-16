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
package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import org.apache.hadoop.thirdparty.protobuf_3_7.Message;

/**
 * Protobuf implementation of the state store API object
 * AddMountTableEntryResponse.
 */
public class AddMountTableEntryResponsePBImpl
    extends AddMountTableEntryResponse implements PBRecord {

  private FederationProtocolPBTranslator<AddMountTableEntryResponseProto,
      AddMountTableEntryResponseProto.Builder,
      AddMountTableEntryResponseProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<AddMountTableEntryResponseProto,
              AddMountTableEntryResponseProto.Builder,
              AddMountTableEntryResponseProtoOrBuilder>(
                  AddMountTableEntryResponseProto.class);

  public AddMountTableEntryResponsePBImpl() {
  }

  public AddMountTableEntryResponsePBImpl(
      AddMountTableEntryResponseProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public AddMountTableEntryResponseProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public boolean getStatus() {
    return this.translator.getProtoOrBuilder().getStatus();
  }

  @Override
  public void setStatus(boolean result) {
    this.translator.getBuilder().setStatus(result);
  }
}