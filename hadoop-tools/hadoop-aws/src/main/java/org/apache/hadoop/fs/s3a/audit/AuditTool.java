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

package org.apache.hadoop.fs.s3a.audit;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SERVICE_UNAVAILABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;

/**
 * AuditTool is a Command Line Interface.
 * i.e, it's functionality is to parse the merged audit log file.
 * and generate avro file.
 */
public class AuditTool extends Configured implements Tool, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AuditTool.class);

  private final String entryPoint = "s3audit";

  private PrintWriter out;

  // Exit codes
  private static final int SUCCESS = EXIT_SUCCESS;
  private static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;

  /**
   * Error String when the wrong FS is used for binding: {@value}.
   **/
  @VisibleForTesting
  public static final String WRONG_FILESYSTEM = "Wrong filesystem for ";

  private final String usage = entryPoint + "  s3a://BUCKET\n";

  public AuditTool() {
  }

  /**
   * Tells us the usage of the AuditTool by commands.
   *
   * @return the string USAGE
   */
  public String getUsage() {
    return usage;
  }

  /**
   * This run method in AuditTool takes S3 bucket path.
   * which contains audit log files from command line arguments.
   * and merge the audit log files present in that path into single file in.
   * local system.
   *
   * @param args command specific arguments.
   * @return SUCCESS i.e, '0', which is an exit code.
   * @throws Exception on any failure.
   */
  @Override
  public int run(String[] args) throws Exception {
    List<String> argv = new ArrayList<>(Arrays.asList(args));
    if (argv.isEmpty()) {
      errorln(getUsage());
      throw invalidArgs("No bucket specified");
    }
    //Path of audit log files in s3 bucket
    Path s3LogsPath = new Path(argv.get(0));

    //Setting the file system
    URI fsURI = toUri(String.valueOf(s3LogsPath));
    S3AFileSystem s3AFileSystem =
        bindFilesystem(FileSystem.newInstance(fsURI, getConf()));
    RemoteIterator<LocatedFileStatus> listOfS3LogFiles =
        s3AFileSystem.listFiles(s3LogsPath, true);

    //Merging local audit files into a single file
    File s3aLogsDirectory = new File(s3LogsPath.getName());
    boolean s3aLogsDirectoryCreation = false;
    if (!s3aLogsDirectory.exists()) {
      s3aLogsDirectoryCreation = s3aLogsDirectory.mkdir();
    }
    if(s3aLogsDirectoryCreation) {
      while (listOfS3LogFiles.hasNext()) {
        Path s3LogFilePath = listOfS3LogFiles.next().getPath();
        File s3LogLocalFilePath =
            new File(s3aLogsDirectory, s3LogFilePath.getName());
        boolean localFileCreation = s3LogLocalFilePath.createNewFile();
        if (localFileCreation) {
          FileStatus fileStatus = s3AFileSystem.getFileStatus(s3LogFilePath);
          long s3LogFileLength = fileStatus.getLen();
          //Reads s3 file data into byte buffer
          byte[] s3LogDataBuffer =
              readDataset(s3AFileSystem, s3LogFilePath, (int) s3LogFileLength);
          //Writes byte array into local file
          FileUtils.writeByteArrayToFile(s3LogLocalFilePath, s3LogDataBuffer);
        }
      }
    }

    //Calls S3AAuditLogMerger for implementing merging code
    //by passing local audit log files directory which are copied from s3 bucket
    S3AAuditLogMerger s3AAuditLogMerger = new S3AAuditLogMerger();
    s3AAuditLogMerger.mergeFiles(s3aLogsDirectory.getPath());

    //Deleting the local log files
    if (s3aLogsDirectory.exists()) {
      FileUtils.forceDeleteOnExit(s3aLogsDirectory);
    }
    return SUCCESS;
  }

  /**
   * Read the file and convert to a byte dataset.
   * This implements readfully internally, so that it will read.
   * in the file without ever having to seek().
   *
   * @param s3AFileSystem   filesystem.
   * @param s3LogFilePath   path to read from.
   * @param s3LogFileLength length of data to read.
   * @return the bytes.
   * @throws IOException IO problems.
   */
  private byte[] readDataset(FileSystem s3AFileSystem, Path s3LogFilePath,
      int s3LogFileLength) throws IOException {
    byte[] s3LogDataBuffer = new byte[s3LogFileLength];
    int offset = 0;
    int nread = 0;
    try (FSDataInputStream fsDataInputStream = s3AFileSystem.open(
        s3LogFilePath)) {
      while (nread < s3LogFileLength) {
        int nbytes = fsDataInputStream.read(s3LogDataBuffer, offset + nread,
            s3LogFileLength - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }
    return s3LogDataBuffer;
  }

  /**
   * Build an exception to throw with a formatted message.
   *
   * @param exitCode exit code to use.
   * @param format   string format.
   * @param args     optional arguments for the string.
   * @return a new exception to throw.
   */
  protected static ExitUtil.ExitException exitException(
      final int exitCode,
      final String format,
      final Object... args) {
    return new ExitUtil.ExitException(exitCode,
        String.format(format, args));
  }

  /**
   * Build the exception to raise on invalid arguments.
   *
   * @param format string format.
   * @param args   optional arguments for the string.
   * @return a new exception to throw.
   */
  protected static ExitUtil.ExitException invalidArgs(
      String format, Object... args) {
    return exitException(INVALID_ARGUMENT, format, args);
  }

  /**
   * Sets the filesystem; it must be an S3A FS instance, or a FilterFS.
   * around an S3A Filesystem.
   *
   * @param bindingFS filesystem to bind to.
   * @return the bound FS.
   * @throws ExitUtil.ExitException if the FS is not an S3 FS.
   */
  protected S3AFileSystem bindFilesystem(FileSystem bindingFS) {
    FileSystem fs = bindingFS;
    while (fs instanceof FilterFileSystem) {
      fs = ((FilterFileSystem) fs).getRawFileSystem();
    }
    if (!(fs instanceof S3AFileSystem)) {
      throw new ExitUtil.ExitException(EXIT_SERVICE_UNAVAILABLE,
          WRONG_FILESYSTEM + "URI " + fs.getUri() + " : "
              + fs.getClass().getName());
    }
    S3AFileSystem filesystem = (S3AFileSystem) fs;
    return filesystem;
  }

  /**
   * Convert a path to a URI, catching any {@code URISyntaxException}.
   * and converting to an invalid args exception.
   *
   * @param s3Path path to convert to a URI.
   * @return a URI of the path.
   * @throws ExitUtil.ExitException INVALID_ARGUMENT if the URI is invalid.
   */
  protected static URI toUri(String s3Path) {
    URI uri;
    try {
      uri = new URI(s3Path);
    } catch (URISyntaxException e) {
      throw invalidArgs("Not a valid fileystem path: %s", s3Path);
    }
    return uri;
  }

  protected static void errorln(String x) {
    System.err.println(x);
  }

  /**
   * Flush all active output channels, including {@Code System.err},
   * as to stay in sync with any JRE log messages.
   */
  private void flush() {
    if (out != null) {
      out.flush();
    } else {
      System.out.flush();
    }
    System.err.flush();
  }

  /**
   * Print a line of output. This goes to any output file, or.
   * is logged at info. The output is flushed before and after, too.
   * try and stay in sync with JRE logging.
   *
   * @param format format string.
   * @param args   any arguments.
   */
  private void println(String format, Object... args) {
    flush();
    String msg = String.format(format, args);
    if (out != null) {
      out.println(msg);
    } else {
      System.out.println(msg);
    }
    flush();
  }

  /**
   * Inner entry point, with no logging or system exits.
   *
   * @param conf configuration.
   * @param argv argument list.
   * @return an exception.
   * @throws Exception Exception.
   */
  public static int exec(Configuration conf, String... argv) throws Exception {
    try (AuditTool auditTool = new AuditTool()) {
      return ToolRunner.run(conf, auditTool, argv);
    }
  }

  /**
   * Main entry point.
   *
   * @param argv args list.
   */
  public static void main(String[] argv) {
    try {
      ExitUtil.terminate(exec(new Configuration(), argv));
    } catch (ExitUtil.ExitException e) {
      LOG.error(e.toString());
      System.exit(e.status);
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      ExitUtil.halt(-1, e);
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    if (out != null) {
      out.close();
    }
  }
}
