/*
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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.DataChecksum;

/**
 * A {@link Compressor} based on the popular gzip compressed file format.
 * http://www.gzip.org/
 */
@DoNotPool
public class BuiltInGzipCompressor implements Compressor {

  /**
   * Fixed ten-byte gzip header. See {@link GZIPOutputStream}'s source for
   * details.
   */
  private static final byte[] GZIP_HEADER = new byte[]{
      0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  // The trailer will be overwritten based on crc and output size.
  private static final byte[] GZIP_TRAILER = new byte[]{
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  private static final int GZIP_HEADER_LEN = GZIP_HEADER.length;
  private static final int GZIP_TRAILER_LEN = GZIP_TRAILER.length;

  private Deflater deflater;

  private int headerOff = 0;
  private int trailerOff = 0;

  private int numExtraBytesWritten = 0;

  private int currentBufLen = 0;

  private final Checksum crc = DataChecksum.newCrc32();

  private BuiltInGzipDecompressor.GzipStateLabel state;

  public BuiltInGzipCompressor(Configuration conf) {
    init(conf);
  }

  @Override
  public boolean finished() {
    // Only if the trailer is also written, it is thought as finished.
    return deflater.finished() && state == BuiltInGzipDecompressor.GzipStateLabel.FINISHED;
  }

  @Override
  public boolean needsInput() {
    if (state == BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
      return deflater.needsInput();
    }

    return false;
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    int compressedBytesWritten = 0;

    if (currentBufLen <= 0) {
      return compressedBytesWritten;
    }

    // If we are not within uncompressed data yet, output the header.
    if (state != BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM &&
            state != BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC) {
      int outputHeaderSize = writeHeader(b, off, len);
      numExtraBytesWritten += outputHeaderSize;

      compressedBytesWritten += outputHeaderSize;

      if (outputHeaderSize == len) {
        return compressedBytesWritten;
      }

      off += outputHeaderSize;
      len -= outputHeaderSize;
    }

    if (state == BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
      // now compress it into b[]
      int deflated = deflater.deflate(b, off, len);

      compressedBytesWritten += deflated;
      off += deflated;
      len -= deflated;

      // All current input are processed. Going to output trailer.
      if (deflater.finished()) {
        state = BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC;
        fillTrailer();
      } else {
        return compressedBytesWritten;
      }
    }

    int outputTrailerSize = writeTrailer(b, off, len);
    numExtraBytesWritten += outputTrailerSize;

    compressedBytesWritten += outputTrailerSize;

    return compressedBytesWritten;
  }

  @Override
  public long getBytesRead() {
    return deflater.getTotalIn();
  }

  @Override
  public long getBytesWritten() {
    return numExtraBytesWritten + deflater.getTotalOut();
  }

  @Override
  public void end() {
    deflater.end();
  }

  @Override
  public void finish() {
    deflater.finish();
  }

  private void init(Configuration conf) {
    ZlibCompressor.CompressionLevel level = ZlibFactory.getCompressionLevel(conf);
    ZlibCompressor.CompressionStrategy strategy = ZlibFactory.getCompressionStrategy(conf);

    // 'true' (nowrap) => Deflater will handle raw deflate stream only
    deflater = new Deflater(level.compressionLevel(), true);
    deflater.setStrategy(strategy.compressionStrategy());

    state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
  }

  @Override
  public void reinit(Configuration conf) {
    init(conf);
    crc.reset();
    numExtraBytesWritten = 0;
    currentBufLen = 0;
    headerOff = 0;
    trailerOff = 0;
  }

  @Override
  public void reset() {
    deflater.reset();
    state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
    crc.reset();
    numExtraBytesWritten = 0;
    currentBufLen = 0;
    headerOff = 0;
    trailerOff = 0;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    deflater.setDictionary(b, off, len);
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    deflater.reset();
    crc.reset();

    deflater.setInput(b, off, len);
    crc.update(b, off, len);  // CRC-32 is on uncompressed data
    currentBufLen = len;
  }

  private int writeHeader(byte[] b, int off, int len) {
    if (len <= 0) {
      return 0;
    }

    int n = Math.min(len, GZIP_HEADER_LEN - headerOff);
    System.arraycopy(GZIP_HEADER, headerOff, b, off, n);
    headerOff += n;

    // Completes header output.
    if (headerOff == GZIP_HEADER_LEN) {
      state = BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM;
    }

    return n;
  }

  private void fillTrailer() {
    if (state == BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC) {
      int streamCrc = (int) crc.getValue();
      GZIP_TRAILER[0] = (byte) (streamCrc & 0x000000ff);
      GZIP_TRAILER[1] = (byte) ((streamCrc & 0x0000ff00) >> 8);
      GZIP_TRAILER[2] = (byte) ((streamCrc & 0x00ff0000) >> 16);
      GZIP_TRAILER[3] = (byte) ((streamCrc & 0xff000000) >> 24);

      GZIP_TRAILER[4] = (byte) (currentBufLen & 0x000000ff);
      GZIP_TRAILER[5] = (byte) ((currentBufLen & 0x0000ff00) >> 8);
      GZIP_TRAILER[6] = (byte) ((currentBufLen & 0x00ff0000) >> 16);
      GZIP_TRAILER[7] = (byte) ((currentBufLen & 0xff000000) >> 24);
    }
  }

  private int writeTrailer(byte[] b, int off, int len) {
    if (len <= 0) {
      return 0;
    }

    int n = Math.min(len, GZIP_TRAILER_LEN - trailerOff);
    System.arraycopy(GZIP_TRAILER, trailerOff, b, off, n);
    trailerOff += n;

    if (trailerOff == GZIP_TRAILER_LEN) {
      state = BuiltInGzipDecompressor.GzipStateLabel.FINISHED;
      currentBufLen = 0;
      headerOff = 0;
      trailerOff = 0;
    }

    return n;
  }
}
