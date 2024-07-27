/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;

public class ThreadLocalContext {
  private final Server.Call call;
  private final CallerContext context;
  private final long startOpTime;
  private final long proxyOpTime;

  public ThreadLocalContext() {
    this.call = Server.getCurCall().get();
    this.context = CallerContext.getCurrent();
    this.startOpTime = FederationRPCPerformanceMonitor.getStartOpTime();
    this.proxyOpTime =  FederationRPCPerformanceMonitor.getProxyOpTime();
  }

  public void transfer() {
    if (call != null) {
      Server.getCurCall().set(call);
    }
    if (context != null) {
      CallerContext.setCurrent(context);
    }
    if (startOpTime != -1L) {
      FederationRPCPerformanceMonitor.setStartOpTime(startOpTime);
    }
    if (proxyOpTime != -1L) {
      FederationRPCPerformanceMonitor.setProxyOpTime(proxyOpTime);
    }
  }

  @Override
  public String toString() {
    return "ThreadLocalContext{" +
        "call=" + call +
        ", context=" + context +
        ", startOpTime=" + startOpTime +
        ", proxyOpTime=" + proxyOpTime +
        '}';
  }
}
