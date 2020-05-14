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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static java.util.UUID.randomUUID;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;


/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
    AbstractAbfsIntegrationTest {

  private static final int REDUCED_RETRY_COUNT = 1;
  private static final int REDUCED_MAX_BACKOFF_INTERVALS_MS = 5000;

  public ITestAzureBlobFileSystemRename() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("testEnsureFileIsRenamed-src");
    touch(src);
    Path dest = path("testEnsureFileIsRenamed-dest");
    fs.delete(dest, true);
    assertRenameOutcome(fs, src, dest, true);

    assertIsFile(fs, dest);
    assertPathDoesNotExist(fs, "expected renamed", src);
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = new Path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = new Path("/testDst");
    assertRenameOutcome(fs, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
        filename, status.getPath().getName());
  }

  @Test
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("testDir"));
    Path test1 = new Path("testDir/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path("testDir/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          touch(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    Path source = new Path("/test");
    Path dest = new Path("/renamedDir");
    assertRenameOutcome(fs, source, dest, true);

    FileStatus[] files = fs.listStatus(dest);
    assertEquals("Wrong number of files in listing", 1000, files.length);
    assertPathDoesNotExist(fs, "rename source dir", source);
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assertRenameOutcome(fs,
        new Path("/"),
        new Path("/testRenameRoot"),
        false);
    assertRenameOutcome(fs,
        new Path(fs.getUri().toString() + "/"),
        new Path(fs.getUri().toString() + "/s"),
        false);
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.mkdirs(new Path("testDir2/test4"));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
  }

  @Test
  public void testRenameRetryFailureAsHTTP400() throws Exception {
    // Rename failed as Bad Request
    // RenameIdempotencyCheck should throw back the rename failure Op
    testRenameTimeout(HTTP_BAD_REQUEST, HTTP_BAD_REQUEST, false,
        "renameIdempotencyCheckOp should return rename BadRequest "
            + "response itself.");
  }

  @Test
  public void testRenameRetryFailureAsHTTP404() throws Exception {
    // Rename failed as FileNotFound and the destination LMT is
    // within TimespanForIdentifyingRecentOperationThroughLMT
    testRenameTimeout(HTTP_NOT_FOUND, HTTP_OK, false,
        "Rename should return success response because the destination "
            + "path is present and its LMT is within "
            + "TimespanForIdentifyingRecentOperationThroughLMT.");
  }

  @Test
  public void testRenameRetryFailureWithDestOldLMT() throws Exception {
    // Rename failed as FileNotFound and the destination LMT is
    // older than TimespanForIdentifyingRecentOperationThroughLMT
    testRenameTimeout(HTTP_NOT_FOUND, HTTP_NOT_FOUND, true,
        "Rename should return original rename failure response "
            + "because the destination path LMT is older than "
            + "TimespanForIdentifyingRecentOperationThroughLMT.");
  }

  private void testRenameTimeout(
      int renameRequestStatus,
      int renameIdempotencyCheckStatus,
      boolean isOldOp,
      String assertMessage) throws Exception {
    // Config to reduce the retry and maxBackoff time for test run
    AbfsConfiguration abfsConfig = getConfiguration();
    abfsConfig.setMaxIoRetries(REDUCED_RETRY_COUNT);
    abfsConfig.setMaxBackoffIntervalMilliseconds(
        REDUCED_MAX_BACKOFF_INTERVALS_MS);

    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient abfsClient = fs.getAbfsStore().getClient();
    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        this.getAccountName(),
        abfsConfig);

    // Create test AbfsClient
    AbfsClient testClient = new AbfsClient(
        abfsClient.getBaseUrl(),
        new SharedKeyCredentials(abfsConfig.getAccountName().substring(0,
            abfsConfig.getAccountName().indexOf(DOT)),
            abfsConfig.getStorageAccountKey()),
        abfsConfig,
        new ExponentialRetryPolicy(REDUCED_RETRY_COUNT),
        abfsConfig.getTokenProvider(),
        tracker);

    // Timespan for which LMT will be considered recent.
    assertTrue(
        "Timestamp range for LMT update should be twice of "
            + "MaxIoRetries * MaxBackoffIntervalMilliseconds",
        testClient.getTimeIntervalToTagOperationRecent(REDUCED_RETRY_COUNT)
            == (2 * REDUCED_RETRY_COUNT * REDUCED_MAX_BACKOFF_INTERVALS_MS));

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    // Set retryCount to non-zero
    when(op.getRetryCount()).thenReturn(REDUCED_RETRY_COUNT);

    // Mock instance of Http Operation response. This will return HTTP:Bad Request
    AbfsHttpOperation http400Op = mock(AbfsHttpOperation.class);
    when(http400Op.getStatusCode()).thenReturn(HTTP_BAD_REQUEST);

    // Mock instance of Http Operation response. This will return HTTP:Not Found
    AbfsHttpOperation http404Op = mock(AbfsHttpOperation.class);
    when(http404Op.getStatusCode()).thenReturn(HTTP_NOT_FOUND);

    Path destinationPath = fs.makeQualified(
        new Path("destination" + randomUUID().toString()));

    if (renameRequestStatus == HTTP_BAD_REQUEST) {
      when(op.getResult()).thenReturn(http400Op);
    } else if (renameRequestStatus == HTTP_NOT_FOUND) {
      // Create the file new.
      fs.create(destinationPath);

      when(op.getResult()).thenReturn(http404Op);

      if (isOldOp) {
        // sleep to cross the threshold for old LMT.
        Thread.sleep(
            testClient.getTimeIntervalToTagOperationRecent(
                REDUCED_RETRY_COUNT));
      }

    }

    assertEquals(
        assertMessage,
        renameIdempotencyCheckStatus,
        testClient.renameIdempotencyCheckOp(op,
            destinationPath.toUri().getPath())
            .getResult()
            .getStatusCode());

  }
}
