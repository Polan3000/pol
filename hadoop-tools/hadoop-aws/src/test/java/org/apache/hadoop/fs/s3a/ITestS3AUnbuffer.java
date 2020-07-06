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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.io.IOUtils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_READ_TOTAL_BYTES;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatisticsSource;

/**
 * Integration test for calling
 * {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} on {@link S3AInputStream}.
 * Validates that the object has been closed using the
 * {@link S3AInputStream#isObjectStreamOpen()} method. Unlike the
 * {@link org.apache.hadoop.fs.contract.s3a.ITestS3AContractUnbuffer} tests,
 * these tests leverage the fact that isObjectStreamOpen exposes if the
 * underlying stream has been closed or not.
 */
public class ITestS3AUnbuffer extends AbstractS3ATestBase {

  public static final int FILE_LENGTH = 16;

  private Path dest;

  @Override
  public void setup() throws Exception {
    super.setup();
    dest = path("ITestS3AUnbuffer");
    describe("ITestS3AUnbuffer");

    byte[] data = ContractTestUtils.dataset(FILE_LENGTH, 'a', 26);
    ContractTestUtils.writeDataset(getFileSystem(), dest, data, data.length,
            16, true);
  }

  @Test
  public void testUnbuffer() throws IOException {
    describe("testUnbuffer");

    // Open file, read half the data, and then call unbuffer
    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      assertTrue(inputStream.getWrappedStream() instanceof S3AInputStream);
      readAndAssertBytesRead(inputStream, 8);
      assertTrue(isObjectStreamOpen(inputStream));
      inputStream.unbuffer();

      // Check the the wrapped stream is closed
      assertFalse(isObjectStreamOpen(inputStream));
    }
  }

  /**
   * Test that calling {@link S3AInputStream#unbuffer()} merges a stream's
   * {@code InputStreamStatistics}
   * into the {@link S3AFileSystem}'s {@link S3AInstrumentation} instance.
   */
  @Test
  public void testUnbufferStreamStatistics() throws IOException {
    describe("testUnbufferStreamStatistics");

    // Validate bytesRead is updated correctly
    S3AFileSystem fs = getFileSystem();
    S3ATestUtils.MetricDiff bytesRead = new S3ATestUtils.MetricDiff(
        fs, STREAM_READ_BYTES);
    S3ATestUtils.MetricDiff totalBytesRead = new S3ATestUtils.MetricDiff(
        fs, STREAM_READ_TOTAL_BYTES);

    // Open file, read half the data, and then call unbuffer
    FSDataInputStream inputStream = null;
    int firstBytesToRead = 8;

    int secondBytesToRead = 1;

    long expectedFinalBytesRead = firstBytesToRead + secondBytesToRead;
    Object streamStatsStr = "uninited";
    try {
      inputStream = fs.open(dest);
      streamStatsStr = demandStringifyIOStatisticsSource(inputStream);

      LOG.info("initial stream statistics {}", streamStatsStr);
      readAndAssertBytesRead(inputStream, firstBytesToRead);
      LOG.info("stream statistics after read {}", streamStatsStr);
      inputStream.unbuffer();

      // Validate that calling unbuffer updates the input stream statistics
      bytesRead.assertDiffEquals(firstBytesToRead);
      totalBytesRead.assertDiffEquals(firstBytesToRead);

      // Validate that calling unbuffer twice in a row updates the statistics
      // correctly
      readAndAssertBytesRead(inputStream, secondBytesToRead);

      inputStream.unbuffer();
      LOG.info("stream statistics after second read {}", streamStatsStr);

      bytesRead.assertDiffEquals(expectedFinalBytesRead);
      totalBytesRead.assertDiffEquals(expectedFinalBytesRead);
    } finally {
      LOG.info("Closing stream");
      IOUtils.closeStream(inputStream);
    }
    LOG.info("stream statistics after close {}", streamStatsStr);

    // Validate that closing the file does not further change the statistics
    bytesRead.assertDiffEquals(expectedFinalBytesRead);
    totalBytesRead.assertDiffEquals(FILE_LENGTH);

    // Validate that the input stream stats are correct when the file is closed
    S3AInputStreamStatistics streamStatistics = ((S3AInputStream) inputStream
        .getWrappedStream())
        .getS3AStreamStatistics();
    Assertions.assertThat(streamStatistics)
        .describedAs("Stream statistics %s", streamStatistics)
        .hasFieldOrPropertyWithValue("bytesRead", expectedFinalBytesRead)
        .hasFieldOrPropertyWithValue("totalBytesRead", (long)FILE_LENGTH)
//        .satisfies(s ->{
//          Assertions.assertThat(s.getBytesRead())
//              .describedAs("getBytesRead")
//              .isEqualTo(expectedFinalBytesRead);
//          Assertions.assertThat(s.getBytesRead())
//              .describedAs("getBytesRead")
//              .isEqualTo(expectedFinalBytesRead);
//        })
//        .matches(s -> s.getBytesRead() == expectedFinalBytesRead,
//            "bytes read")
//        .matches( s -> s.getTotalBytesRead() == FILE_LENGTH,
//            "total bytes read should be file length")

    ;
    assertEquals("S3AInputStream statistics were not updated properly in "
        + streamStatsStr,
        expectedFinalBytesRead,
            streamStatistics.getBytesRead());
  }

  private boolean isObjectStreamOpen(FSDataInputStream inputStream) {
    return ((S3AInputStream) inputStream.getWrappedStream()).isObjectStreamOpen();
  }

  /**
   * Read the specified number of bytes from the given
   * {@link FSDataInputStream} and assert that
   * {@link FSDataInputStream#read(byte[])} read the specified number of bytes.
   */
  private static void readAndAssertBytesRead(FSDataInputStream inputStream,
                                        int bytesToRead) throws IOException {
    assertEquals("S3AInputStream#read did not read the correct number of " +
                    "bytes", bytesToRead,
            inputStream.read(new byte[bytesToRead]));
  }
}
