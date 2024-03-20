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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.http.HttpClientConnection;
import org.apache.http.client.protocol.HttpClientContext;

public class AbfsManagedHttpContext extends HttpClientContext {

  Long connectTime;

  long readTime;

  long sendTime;

  HttpClientConnection httpClientConnection;

  long expect100HeaderSendTime = 0L;

  long expect100ResponseTime;

  long keepAliveTime;

  Boolean isBeingRead = false;

  final AbfsRestOperationType abfsRestOperationType;

  public AbfsManagedHttpContext(AbfsRestOperationType operationType) {
    this.abfsRestOperationType = operationType;
  }

  /**
   * This to be used only in tests to get connection level activity.
   */
  protected HttpClientConnection interceptConnectionActivity(
      HttpClientConnection httpClientConnection) {
    return httpClientConnection;
  }
}
