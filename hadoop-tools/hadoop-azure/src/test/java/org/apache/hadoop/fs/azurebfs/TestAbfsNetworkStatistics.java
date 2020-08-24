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

package org.apache.hadoop.fs.azurebfs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;

public class TestAbfsNetworkStatistics extends AbstractAbfsTestWithTimeout {

  private static final int LARGE_OPERATIONS = 1000;

  public TestAbfsNetworkStatistics() throws Exception {
  }

  /**
   * Test to check correct values of read and write throttling statistics in
   * {@code AbfsClientThrottlingAnalyzer}.
   */
  @Test
  public void testAbfsThrottlingStatistics() throws URISyntaxException {
    describe("Test to check correct values of read throttle and write "
        + "throttle statistics in Abfs");

    AbfsCounters statistics =
        new AbfsCountersImpl(new URI("www.dummyuri.com/test"));

    /*
     * Calling the throttle methods to check correct summation and values of
     * the counters.
     */
    for (int i = 0; i < LARGE_OPERATIONS; i++) {
      statistics.incrementCounter(AbfsStatistic.READ_THROTTLES, 1);
      statistics.incrementCounter(AbfsStatistic.WRITE_THROTTLES, 1);
    }

    Map<String, Long> metricMap = statistics.toMap();

    /*
     * Test to check read and write throttle statistics gave correct values for
     * 1000 calls.
     */
    assertAbfsStatistics(AbfsStatistic.READ_THROTTLES, LARGE_OPERATIONS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.WRITE_THROTTLES, LARGE_OPERATIONS,
        metricMap);
  }

  /**
   * Custom assertion for AbfsStatistics which have statistics, expected
   * value and map of statistics and value as its parameters.
   * @param statistic the AbfsStatistics which needs to be asserted.
   * @param expectedValue the expected value of the statistics.
   * @param metricMap map of (String, Long) with statistics name as key and
   *                  statistics value as map value.
   */
  private long assertAbfsStatistics(AbfsStatistic statistic,
      long expectedValue, Map<String, Long> metricMap) {
    assertEquals("Mismatch in " + statistic.getStatName(), expectedValue,
        (long) metricMap.get(statistic.getStatName()));
    return expectedValue;
  }

}
