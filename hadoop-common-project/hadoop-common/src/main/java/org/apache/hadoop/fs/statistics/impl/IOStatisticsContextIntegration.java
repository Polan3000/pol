/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.statistics.impl;

import java.lang.ref.WeakReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static org.apache.hadoop.fs.CommonConfigurationKeys.THREAD_LEVEL_IOSTATISTICS_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeys.THREAD_LEVEL_IOSTATISTICS_ENABLED_DEFAULT;

/**
 * A Utility class for IOStatisticsContext, which helps in creating and
 * getting the current active context. Static methods in this class allows to
 * get the current context to start aggregating the IOStatistics.
 *
 * Static initializer is used to work out if the feature to collect
 * thread-level IOStatistics is enabled or not and the corresponding
 * implementation class is called for it.
 *
 * Weak Reference thread map to be used to keep track of different context's
 * to avoid long-lived memory leakages as these references would be cleaned
 * up at GC.
 */
public final class IOStatisticsContextIntegration {

  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsContextIntegration.class);

  private static final boolean IS_THREAD_IOSTATS_ENABLED;

  /**
   * Active IOStatistics Context containing different worker thread's
   * statistics. Weak Reference so that it gets cleaned up during GC and we
   * avoid any memory leak issues due to long lived references.
   */
  private static final WeakReferenceThreadMap<IOStatisticsContext>
      ACTIVE_IOSTATS_CONTEXT =
      new WeakReferenceThreadMap<>(
          IOStatisticsContextIntegration::createNewInstance,
          IOStatisticsContextIntegration::referenceLostContext
      );

  static {
    // Work out if the current context has thread level IOStatistics enabled.
    final Configuration configuration = new Configuration();
    IS_THREAD_IOSTATS_ENABLED =
        configuration.getBoolean(THREAD_LEVEL_IOSTATISTICS_ENABLED,
            THREAD_LEVEL_IOSTATISTICS_ENABLED_DEFAULT);
  }

  /**
   * Private constructor for a utility class to be used in IOStatisticsContext.
   */
  private IOStatisticsContextIntegration() {}

  /**
   * Creating a new IOStatisticsContext instance for a FS to be used.
   * @param key Thread ID that represents which thread the context belongs to.
   * @return an instance of IOStatisticsContext.
   */
  private static IOStatisticsContext createNewInstance(Long key) {
    return new IOStatisticsContextImpl(key);
  }

  /**
   * In case of reference loss for IOStatisticsContext.
   * @param key ThreadID.
   */
  private static void referenceLostContext(Long key) {
    LOG.debug("Reference lost for threadID for the context: {}", key);
  }

  /**
   * Get the current thread's IOStatisticsContext instance. If no instance is
   * present for this thread ID, create one using the factory.
   * @return instance of IOStatisticsContext.
   */
  public static IOStatisticsContext getCurrentIOStatisticsContext() {
    return IS_THREAD_IOSTATS_ENABLED
        ? ACTIVE_IOSTATS_CONTEXT.getForCurrentThread()
        : EmptyIOStatisticsContextImpl.getInstance();
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread.
   */
  public static void setThreadIOStatisticsContext(
      IOStatisticsContext statisticsContext) {
    if (IS_THREAD_IOSTATS_ENABLED &&
        ACTIVE_IOSTATS_CONTEXT.getForCurrentThread() != statisticsContext) {
      ACTIVE_IOSTATS_CONTEXT.setForCurrentThread(statisticsContext);
    }
  }

  /**
   * Get thread ID specific IOStatistics values if
   * statistics are enabled and the thread ID is in the map.
   * @param testThreadId thread ID.
   * @return IOStatisticsContext if found in the map.
   */
  @VisibleForTesting
  public static IOStatisticsContext getThreadSpecificIOStatisticsContext(long testThreadId) {
    LOG.debug("IOStatsContext thread ID required: {}", testThreadId);

    if (!IS_THREAD_IOSTATS_ENABLED) {
      return null;
    }
    // lookup the weakRef IOStatisticsContext for the thread ID in the
    // ThreadMap.
    WeakReference<IOStatisticsContext> ioStatisticsSnapshotWeakReference =
        ACTIVE_IOSTATS_CONTEXT.lookup(testThreadId);
    if (ioStatisticsSnapshotWeakReference != null) {
      return ioStatisticsSnapshotWeakReference.get();
    }
    return null;
  }
}
