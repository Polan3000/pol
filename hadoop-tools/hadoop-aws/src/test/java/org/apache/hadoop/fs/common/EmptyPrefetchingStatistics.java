 /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */

package org.apache.hadoop.fs.common;

import java.time.Duration;

public final class EmptyPrefetchingStatistics implements PrefetchingStatistics {
  @Override
  public void prefetchOperationStarted() {

  }

  @Override
  public void blockAddedToFileCache() {

  }

  @Override
  public void blockRemovedFromFileCache() {

  }

  @Override
  public void prefetchOperationCompleted(boolean prefetchSucceeded) {

  }

  @Override
  public void executorAcquired(Duration timeInQueue) {

  }

  @Override
  public void memoryAllocated(int size) {

  }

  @Override
  public void memoryFreed(int size) {

  }
}
