/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

public class MockAbfsClientThrottlingAnalyzer
    extends AbfsClientThrottlingAnalyzer {

  private ThreadLocal<Integer> failedInstances = new ThreadLocal<>();

  public MockAbfsClientThrottlingAnalyzer(final String name)
      throws IllegalArgumentException {
    super(name);
  }

  @Override
  public void addBytesTransferred(final long count,
      final boolean isFailedOperation) {
    if (isFailedOperation) {
      Integer current = failedInstances.get();
      if (current == null) {
        current = 1;
      } else {
        current++;
      }
      failedInstances.set(current);
    }
    super.addBytesTransferred(count, isFailedOperation);
  }

  public Integer getFailedInstances() {
    return failedInstances.get();
  }
}
