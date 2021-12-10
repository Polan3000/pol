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

package org.apache.hadoop.fs.s3a.read;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Enhanced {@code InputStream} for reading from S3.
 *
 * This implementation provides improved read throughput by asynchronously prefetching
 * blocks of configurable size from the underlying S3 file.
 */
public class S3EInputStream
    extends FSInputStream
    implements CanSetReadahead, StreamCapabilities, IOStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(S3EInputStream.class);

  // Underlying input stream used for reading S3 file.
  private S3InputStream inputStream;

  /**
   * Initializes a new instance of the {@code S3EInputStream} class.
   *
   * @param context .
   * @param s3Attributes .
   * @param client .
   */
  public S3EInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(s3Attributes, "s3Attributes");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getBucket(), "s3Attributes.getBucket()");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getKey(), "s3Attributes.getKey()");
    Validate.checkNotNegative(s3Attributes.getLen(), "s3Attributes.getLen()");
    Validate.checkNotNull(client, "client");

    long fileSize = s3Attributes.getLen();
    if (fileSize <= context.getPrefetchBlockSize()) {
      this.inputStream = new S3InMemoryInputStream(context, s3Attributes, client);
    } else {
      this.inputStream = new S3CachingInputStream(context, s3Attributes, client);
    }
  }

  @Override
  public synchronized int available() throws IOException {
    this.throwIfClosed();
    return this.inputStream.available();
  }

  @Override
  public synchronized long getPos() throws IOException {
    this.throwIfClosed();
    return this.inputStream.getPos();
  }

  @Override
  public synchronized int read() throws IOException {
    this.throwIfClosed();
    return this.inputStream.read();
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    this.throwIfClosed();
    return this.inputStream.read(buf, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.inputStream != null) {
      this.inputStream.close();
      this.inputStream = null;
      super.close();
    }
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    this.throwIfClosed();
    this.inputStream.seek(pos);
  }

  @Override
  public synchronized void setReadahead(Long readahead) {
    if (!this.isClosed()) {
      this.inputStream.setReadahead(readahead);
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    if (!this.isClosed()) {
      return this.inputStream.hasCapability(capability);
    }

    return false;
  }

  /**
   * Access the input stream statistics.
   * This is for internal testing and may be removed without warning.
   * @return the statistics for this input stream
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    if (this.isClosed()) {
      return null;
    }
    return this.inputStream.getS3AStreamStatistics();
  }

  @Override
  public IOStatistics getIOStatistics() {
    if (this.isClosed()) {
      return null;
    }
    return this.inputStream.getIOStatistics();
  }

  protected boolean isClosed() {
    return this.inputStream == null;
  }

  protected void throwIfClosed() throws IOException {
    if (this.isClosed()) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  // Unsupported functions.

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    this.throwIfClosed();
    return false;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
