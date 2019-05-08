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

package org.apache.hadoop.fs.s3a;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Mode;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.S3_SELECT_CAPABILITY;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.SELECT_SQL;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.apache.http.HttpStatus.SC_PRECONDITION_FAILED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Test S3A remote file change detection.
 */
@RunWith(Parameterized.class)
public class ITestS3ARemoteFileChanged extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ARemoteFileChanged.class);

  private static final String TEST_DATA = "Some test data";
  private static final int TEST_MAX_RETRIES = 5;
  private static final String TEST_RETRY_INTERVAL = "100ms";
  private static final String QUOTED_TEST_DATA =
      "\"" + TEST_DATA + "\"";

  private enum InteractionType {
    READ,
    READ_AFTER_DELETE,
    EVENTUALLY_CONSISTENT_READ,
    COPY,
    EVENTUALLY_CONSISTENT_COPY,
    SELECT
  }

  private final String changeDetectionSource;
  private final String changeDetectionMode;
  private final Collection<InteractionType> expectedExceptionInteractions;
  private S3AFileSystem fs;

  @Parameterized.Parameters(name = "{index}: source={0}; mode={1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        // make sure it works with invalid config
        {"bogus", "bogus",
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.SELECT)},

        // test with etag
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_SERVER,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.SELECT)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_CLIENT,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.COPY,
                // not InteractionType.EVENTUALLY_CONSISTENT_COPY as copy change
                // detection can't really occur client-side.  The eTag of
                // the new object can't be expected to match.
                InteractionType.SELECT)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_WARN,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_NONE,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},

        // test with versionId
        // when using server-side versionId, the normal read exceptions
        // shouldn't happen since the previous version will still be available,
        // but they will still happen on rename and select since we always do a
        // client-side check against the current version
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_SERVER,
            Arrays.asList(
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.SELECT)},

        // with client-side versionId it will behave similar to client-side eTag
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_CLIENT,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                // not InteractionType.EVENTUALLY_CONSISTENT_COPY as copy change
                // detection can't really occur client-side.  The versionId of
                // the new object can't be expected to match.
                InteractionType.SELECT)},

        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_WARN,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_NONE,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)}
    });
  }

  public ITestS3ARemoteFileChanged(String changeDetectionSource,
      String changeDetectionMode,
      Collection<InteractionType> expectedExceptionInteractions) {
    this.changeDetectionSource = changeDetectionSource;
    this.changeDetectionMode = changeDetectionMode;
    this.expectedExceptionInteractions = expectedExceptionInteractions;
  }

  @Before
  public void setUp() {
    fs = getFileSystem();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        CHANGE_DETECT_SOURCE,
        CHANGE_DETECT_MODE,
        RETRY_LIMIT,
        RETRY_INTERVAL);
    conf.set(CHANGE_DETECT_SOURCE, changeDetectionSource);
    conf.set(CHANGE_DETECT_MODE, changeDetectionMode);

    // reduce retry limit so FileNotFoundException cases timeout faster,
    // speeding up the tests
    conf.setInt(RETRY_LIMIT, TEST_MAX_RETRIES);
    conf.set(RETRY_INTERVAL, TEST_RETRY_INTERVAL);

    if (conf.getClass(S3_METADATA_STORE_IMPL, MetadataStore.class) ==
        NullMetadataStore.class) {
      // favor LocalMetadataStore over NullMetadataStore
      conf.setClass(S3_METADATA_STORE_IMPL,
          LocalMetadataStore.class, MetadataStore.class);
    }
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * Tests reading a file that is changed while the reader's InputStream is
   * open.
   */
  @Test
  public void testReadFileChangedStreamOpen() throws Throwable {
    describe("Tests reading a file that is changed while the reader's"
        + "InputStream is open.");
    final int originalLength = 8192;
    final byte[] originalDataset = dataset(originalLength, 'a', 32);
    final int newLength = originalLength + 1;
    final byte[] newDataset = dataset(newLength, 'A', 32);
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, originalDataset, originalDataset.length,
        1024, false);

    skipIfVersionPolicyAndNoVersionId(testpath);

    try(FSDataInputStream instream = fs.open(testpath)) {
      // seek forward and read successfully
      instream.seek(1024);
      assertTrue("no data to read", instream.read() >= 0);

      // overwrite
      writeDataset(fs, testpath, newDataset, newDataset.length, 1024, true);
      // here the new file length is larger. Probe the file to see if this is
      // true, with a spin and wait
      eventually(30 * 1000, 1000,
          () -> {
            assertEquals(newLength, fs.getFileStatus(testpath).getLen());
          });

      // With the new file version in place, any subsequent S3 read by
      // eTag/versionId will fail.  A new read by eTag/versionId will occur in
      // reopen() on read after a seek() backwards.  We verify seek backwards
      // results in the expected exception and seek() forward works without
      // issue.

      // first check seek forward
      instream.seek(2048);
      assertTrue("no data to read", instream.read() >= 0);

      // now check seek backward
      instream.seek(instream.getPos() - 100);

      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read());
      } else {
        instream.read();
      }

      byte[] buf = new byte[256];

      // seek backward
      instream.seek(0);

      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read(buf));
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read(0, buf, 0, buf.length));
        intercept(RemoteFileChangedException.class,  "", "readfully",
            () -> instream.readFully(0, buf));
      } else {
        instream.read(buf);
        instream.read(0, buf, 0, buf.length);
        instream.readFully(0, buf);
      }

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      // seek backward
      instream.seek(0);

      if (expectedExceptionInteractions.contains(
          InteractionType.READ_AFTER_DELETE)) {
        intercept(FileNotFoundException.class, "", "read()",
            () -> instream.read());
        intercept(FileNotFoundException.class, "", "readfully",
            () -> instream.readFully(2048, buf));
      } else {
        instream.read();
        instream.readFully(2048, buf);
      }
    }
  }

  /**
   * Tests reading a file where the version visible in S3 does not match the
   * version tracked in the metadata store.
   */
  @Test
  public void testReadFileChangedOutOfSyncMetadata() throws Throwable {
    final Path testpath = writeOutOfSyncFileVersion("fileChangedOutOfSync.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    try (final FSDataInputStream instream = fs.open(testpath)) {
      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        intercept(RemoteFileChangedException.class, "", "read()",
            () -> {
              instream.read();
            });
      } else {
        instream.read();
      }
    }
  }

  /**
   * Ensures a file can be read when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testReadWithNoVersionMetadata() throws Throwable {
    final Path testpath = writeFileWithNoVersionMetadata("readnoversion.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    assertEquals("Contents of " + testpath,
        TEST_DATA,
        ContractTestUtils.readUTF8(fs, testpath, TEST_DATA.length()));
  }

  /**
   * Tests using S3 Select on a file where the version visible in S3 does not
   * match the version tracked in the metadata store.
   */
  @Test
  public void testSelectChangedFile() throws Throwable {
    requireS3Select();
    final Path testpath = writeOutOfSyncFileVersion("select.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    if (expectedExceptionInteractions.contains(InteractionType.SELECT)) {
      interceptFuture(RemoteFileChangedException.class, "select",
          fs.openFile(testpath)
              .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build());
    } else {
      fs.openFile(testpath)
          .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
          .build()
          .get()
          .close();
    }
  }

  /**
   * Tests using S3 Select on a file where the version visible in S3 does not
   * initially match the version tracked in the metadata store, but eventually
   * (after retries) does.
   */
  @Test
  public void testSelectEventuallyConsistentFile() throws Throwable {
    describe("Eventually Consistent S3 Select");
    requireS3Guard();
    requireS3Select();
    AmazonS3 s3ClientSpy = Mockito.spy(fs.getAmazonS3Client());
    fs.setAmazonS3Client(s3ClientSpy);

    final Path testpath1 = writeEventuallyConsistentFileVersion(
        "select1.dat", s3ClientSpy, 0, TEST_MAX_RETRIES, 0);

    skipIfVersionPolicyAndNoVersionId(testpath1);

    // should succeed since the inconsistency doesn't last longer than the
    // configured retry limit
    fs.openFile(testpath1)
        .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
        .build()
        .get()
        .close();

    // TEST_MAX_RETRIES + 2 is required to before failure since there is
    // one getObjectMetadata() call due to getFileStatus() before the
    // change detection and retries in select()
    final Path testpath2 = writeEventuallyConsistentFileVersion(
        "select2.dat", s3ClientSpy, 0, TEST_MAX_RETRIES + 2, 0);

    if (expectedExceptionInteractions.contains(InteractionType.SELECT)) {
      // should fail since the inconsistency lasts longer than the configured
      // retry limit
      interceptFuture(RemoteFileChangedException.class, "select",
          fs.openFile(testpath2)
              .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build());
    } else {
      fs.openFile(testpath2)
          .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
          .build()
          .get()
          .close();
    }
  }

  /**
   * Ensures a file can be read via S3 Select when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testSelectWithNoVersionMetadata() throws Throwable {
    requireS3Select();
    final Path testpath =
        writeFileWithNoVersionMetadata("selectnoversion.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    try (FSDataInputStream instream = fs.openFile(testpath)
        .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build().get()) {
      assertEquals(QUOTED_TEST_DATA,
          IOUtils.toString(instream, Charset.forName("UTF-8")).trim());
    }
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version tracked in the metadata store.
   * @throws Throwable
   */
  @Test
  public void testRenameChangedFile() throws Throwable {
    final Path testpath = writeOutOfSyncFileVersion("rename.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    final Path dest = path("dest.dat");
    if (expectedExceptionInteractions.contains(InteractionType.COPY)) {
      intercept(RemoteFileChangedException.class, "", "copy()",
          () -> {
            fs.rename(testpath, dest);
          });
    } else {
      fs.rename(testpath, dest);
    }
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version in the metadata store until a certain number of retries
   * has been met.
   */
  @Test
  public void testRenameEventuallyConsistentFile() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = Mockito.spy(fs.getAmazonS3Client());
    fs.setAmazonS3Client(s3ClientSpy);

    // Total inconsistent response count across getObjectMetadata() and
    // copyObject() must be TEST_MAX_RETRIES + 2 to generate a failure
    // since there is one getObjectMetadata() call from innerRename() before
    // copyObject().
    // The split of inconsistent responses between getObjectMetadata() and
    // copyObject() is arbitrary.
    int maxInconsistenciesBeforeFailure = TEST_MAX_RETRIES + 1;
    int metadataInconsistencyCount = 3;
    final Path testpath1 =
        writeEventuallyConsistentFileVersion("rename-eventually1.dat",
            s3ClientSpy,
            0,
            metadataInconsistencyCount,
            maxInconsistenciesBeforeFailure - metadataInconsistencyCount);

    skipIfVersionPolicyAndNoVersionId(testpath1);

    final Path dest1 = path("dest1.dat");
    // shouldn't fail since the inconsistency doesn't last through the
    // configured retry limit
    fs.rename(testpath1, dest1);

    final Path testpath2 =
        writeEventuallyConsistentFileVersion("rename-eventually2.dat",
            s3ClientSpy,
            0,
            metadataInconsistencyCount,
            maxInconsistenciesBeforeFailure - metadataInconsistencyCount + 1);
    final Path dest2 = path("dest2.dat");
    if (expectedExceptionInteractions.contains(
        InteractionType.EVENTUALLY_CONSISTENT_COPY)) {
      // should fail since the inconsistency is set up to persist longer than
      // the configured retry limit
      intercept(RemoteFileChangedException.class, "", "copy()",
          () -> {
            fs.rename(testpath2, dest2);
          });
    } else {
      fs.rename(testpath2, dest2);
    }
  }

  /**
   * Ensures a file can be renamed when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testRenameWithNoVersionMetadata() throws Throwable {
    final Path testpath =
        writeFileWithNoVersionMetadata("renamenoversion.dat");

    skipIfVersionPolicyAndNoVersionId(testpath);

    final Path dest = path("noversiondest.dat");
    fs.rename(testpath, dest);
    FSDataInputStream inputStream = fs.open(dest);
    assertEquals(TEST_DATA,
        IOUtils.toString(inputStream, Charset.forName("UTF-8")).trim());
  }

  /**
   * Ensures S3Guard and retries allow an eventually consistent read.
   */
  @Test
  public void testReadAfterEventuallyConsistentWrite() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = Mockito.spy(fs.getAmazonS3Client());
    fs.setAmazonS3Client(s3ClientSpy);
    final Path testpath1 =
        writeEventuallyConsistentFileVersion("eventually1.dat",
            s3ClientSpy, TEST_MAX_RETRIES, 0 , 0);

    skipIfVersionPolicyAndNoVersionId(testpath1);

    try (FSDataInputStream instream1 = fs.open(testpath1)) {
      // succeeds on the last retry
      instream1.read();
    }

    final Path testpath2 =
        writeEventuallyConsistentFileVersion("eventually2.dat",
            s3ClientSpy, TEST_MAX_RETRIES + 1, 0, 0);
    try (FSDataInputStream instream2 = fs.open(testpath2)) {
      if (expectedExceptionInteractions.contains(
          InteractionType.EVENTUALLY_CONSISTENT_READ)) {
        // keeps retrying and eventually gives up with RemoteFileChangedException
        intercept(RemoteFileChangedException.class, "", "read()",
            () -> {
              instream2.read();
            });
      } else {
        instream2.read();
      }
    }
  }

  /**
   * Ensures read on re-open (after seek backwards) when S3 does not return the
   * version of the file tracked in the metadata store fails immediately.  No
   * retries should happen since a retry is not expected to recover.
   */
  @Test
  public void testEventuallyConsistentReadOnReopen() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = Mockito.spy(fs.getAmazonS3Client());
    fs.setAmazonS3Client(s3ClientSpy);
    String filename = "eventually-reopen.dat";
    final Path testpath =
        writeEventuallyConsistentFileVersion(filename,
            s3ClientSpy, 0, 0, 0);

    skipIfVersionPolicyAndNoVersionId(testpath);

    try (FSDataInputStream instream = fs.open(testpath)) {
      instream.read();
      // overwrite the file, returning inconsistent version for
      // (effectively) infinite retries
      writeEventuallyConsistentFileVersion(filename, s3ClientSpy,
          Integer.MAX_VALUE, 0, 0);
      instream.seek(0);
      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        // if it retries at all, it will retry forever, which should fail
        // the test.  The expected behavior is immediate
        // RemoteFileChangedException.
        intercept(RemoteFileChangedException.class, "", "read()",
            () -> {
              instream.read();
            });
      } else {
        instream.read();
      }
    }
  }

  /**
   * Writes a file with old ETag and versionId in the metadata store such
   * that the metadata is out of sync with S3.  Attempts to read such a file
   * should result in {@link RemoteFileChangedException}.
   */
  private Path writeOutOfSyncFileVersion(String filename) throws IOException {
    final Path testpath = path(filename);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(out);
    printWriter.println(TEST_DATA);
    printWriter.close();
    final byte[] dataset = out.toByteArray();
    writeDataset(fs, testpath, dataset, dataset.length,
        1024, false);
    S3AFileStatus originalStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    // overwrite with half the content
    writeDataset(fs, testpath, dataset, dataset.length / 2,
        1024, true);

    S3AFileStatus newStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    // put back the original etag, versionId
    S3AFileStatus forgedStatus =
        S3AFileStatus.fromFileStatus(newStatus, Tristate.FALSE,
            originalStatus.getETag(), originalStatus.getVersionId());
    fs.getMetadataStore().put(
        new PathMetadata(forgedStatus, Tristate.FALSE, false));

    return testpath;
  }

  /**
   * Writes a file where the file will be inconsistent in S3 for some duration.
   * The duration of the inconsistency is controlled by the
   * getObjectInconsistencyCount, getMetdataInconsistencyCount, and
   * copyInconistentCallCount parameters.  The inconsistency manifests in
   * AmazonS3#getObject, AmazonS3#getObjectMetadata, and AmazonS3#copyObject.
   * This method sets up the provided s3ClientSpy to return a response to each
   * of these methods indicating an inconsistency where the requested object
   * version (eTag or versionId) is not available until a certain retry
   * threshold is met.  Providing inconsistent call count values above or
   * below the overall retry limit allows a test to simulate a condition that
   * either should or should not result in an overall failure from retry
   * exhaustion.
   */
  private Path writeEventuallyConsistentFileVersion(String filename,
      AmazonS3 s3ClientSpy,
      int getObjectInconsistencyCount,
      int getMetdataInconsistencyCount,
      int copyInconsistencyCount)
      throws IOException {
    final Path testpath = path(filename);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(out);
    printWriter.println(TEST_DATA);
    printWriter.close();
    final byte[] dataset = out.toByteArray();
    writeDataset(fs, testpath, dataset, dataset.length,
        1024, true);
    S3AFileStatus originalStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    // overwrite with half the content
    writeDataset(fs, testpath, dataset, dataset.length / 2,
        1024, true);

    S3AFileStatus newStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    stubTemporaryUnavailable(s3ClientSpy, getObjectInconsistencyCount,
        testpath, newStatus);

    stubTemporaryWrongVersion(s3ClientSpy, getObjectInconsistencyCount,
        testpath, originalStatus);

    if (fs.getChangeDetectionPolicy().getMode() == Mode.Server) {
      // only stub inconsistency when mode is server since no constraints that
      // should trigger inconsistency are passed in any other mode
      stubTemporaryCopyInconsistency(s3ClientSpy, testpath, newStatus,
          copyInconsistencyCount);
    }

    stubTemporaryMetadataInconsistency(s3ClientSpy, testpath, originalStatus,
        newStatus, getMetdataInconsistencyCount);

    return testpath;
  }

  /**
   * Stubs {@link AmazonS3#getObject(GetObjectRequest)}
   * within s3ClientSpy to return null until inconsistentCallCount calls have
   * been made.  The null response simulates what occurs when an object
   * matching the specified ETag or versionId is not available.
   * @param s3ClientSpy the spy to stub
   * @param inconsistentCallCount the number of calls that should return the
   * null response
   * @param testpath the path of the object the stub should apply to
   */
  private void stubTemporaryUnavailable(AmazonS3 s3ClientSpy,
      int inconsistentCallCount, Path testpath,
      S3AFileStatus newStatus) {
    Answer<S3Object> temporarilyUnavailableAnswer = new Answer<S3Object>() {
      private int callCount = 0;

      @Override
      public S3Object answer(InvocationOnMock invocation) throws Throwable {
        // simulates ETag or versionId constraint not met until
        // inconsistentCallCount surpassed
        callCount++;
        if (callCount <= inconsistentCallCount) {
          return null;
        }
        return (S3Object) invocation.callRealMethod();
      }
    };

    // match the requests that would be made in either server-side change
    // detection mode
    doAnswer(temporarilyUnavailableAnswer).when(s3ClientSpy)
        .getObject(
            matchingGetObjectRequest(
                testpath, newStatus.getETag(), null));
    doAnswer(temporarilyUnavailableAnswer).when(s3ClientSpy)
        .getObject(
            matchingGetObjectRequest(
                testpath, null, newStatus.getVersionId()));
  }

  /**
   * Stubs {@link AmazonS3#getObject(GetObjectRequest)}
   * within s3ClientSpy to return an object modified to contain metadata
   * from originalStatus until inconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param inconsistentCallCount the number of calls that should return the
   * null response
   * @param originalStatus the status metadata to inject into the
   * inconsistentCallCount responses
   */
  private void stubTemporaryWrongVersion(AmazonS3 s3ClientSpy,
      int inconsistentCallCount, Path testpath,
      S3AFileStatus originalStatus) {
    Answer<S3Object> temporarilyWrongVersionAnswer = new Answer<S3Object>() {
      private int callCount = 0;

      @Override
      public S3Object answer(InvocationOnMock invocation) throws Throwable {
        // simulates old ETag or versionId until inconsistentCallCount surpassed
        callCount++;
        S3Object s3Object = (S3Object) invocation.callRealMethod();
        if (callCount <= inconsistentCallCount) {
          S3Object objectSpy = Mockito.spy(s3Object);
          ObjectMetadata metadataSpy =
              Mockito.spy(s3Object.getObjectMetadata());
          when(objectSpy.getObjectMetadata()).thenReturn(metadataSpy);
          when(metadataSpy.getETag()).thenReturn(originalStatus.getETag());
          when(metadataSpy.getVersionId())
              .thenReturn(originalStatus.getVersionId());
          return objectSpy;
        }
        return s3Object;
      }
    };

    // match requests that would be made in client-side change detection
    doAnswer(temporarilyWrongVersionAnswer).when(s3ClientSpy).getObject(
        matchingGetObjectRequest(testpath, null, null));
  }

  /**
   * Stubs {@link AmazonS3#copyObject(CopyObjectRequest)}
   * within s3ClientSpy to throw precondition failed error until
   * copyInconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param newStatus the status metadata containing the ETag and versionId
   * that should be matched in order for the stub to apply
   * @param copyInconsistentCallCount how many times to return the
   * precondition failed error
   */
  private void stubTemporaryCopyInconsistency(AmazonS3 s3ClientSpy,
      Path testpath, S3AFileStatus newStatus,
      int copyInconsistentCallCount) {
    Answer<CopyObjectResult> temporarilyPreconditionsNotMetAnswer =
        new Answer<CopyObjectResult>() {
      private int callCount = 0;

      @Override
      public CopyObjectResult answer(InvocationOnMock invocation)
          throws Throwable {
        callCount++;
        if (callCount <= copyInconsistentCallCount) {
          AmazonServiceException exception =
              new AmazonServiceException("preconditions not met");
          exception.setStatusCode(SC_PRECONDITION_FAILED);
          throw exception;
        }
        return (CopyObjectResult) invocation.callRealMethod();
      }
    };

    // match requests made during copy
    doAnswer(temporarilyPreconditionsNotMetAnswer).when(s3ClientSpy).copyObject(
        matchingCopyObjectRequest(testpath, newStatus.getETag(), null));
    doAnswer(temporarilyPreconditionsNotMetAnswer).when(s3ClientSpy).copyObject(
        matchingCopyObjectRequest(testpath, null, newStatus.getVersionId()));
  }

  /**
   * Stubs {@link AmazonS3#getObjectMetadata(GetObjectMetadataRequest)}
   * within s3ClientSpy to return metadata from originalStatus until
   * metadataInconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param originalStatus the inconsistent status metadata to return
   * @param newStatus the status metadata to return after
   * metadataInconsistentCallCount is met
   * @param metadataInconsistentCallCount how many times to return the
   * inconsistent metadata
   */
  private void stubTemporaryMetadataInconsistency(AmazonS3 s3ClientSpy,
      Path testpath, S3AFileStatus originalStatus,
      S3AFileStatus newStatus, int metadataInconsistentCallCount) {
    Answer<ObjectMetadata> temporarilyOldMetadataAnswer =
        new Answer<ObjectMetadata>() {
      private int callCount = 0;

      @Override
      public ObjectMetadata answer(InvocationOnMock invocation)
          throws Throwable {
        ObjectMetadata objectMetadata =
            (ObjectMetadata) invocation.callRealMethod();
        callCount++;
        if (callCount <= metadataInconsistentCallCount) {
          ObjectMetadata metadataSpy =
              Mockito.spy(objectMetadata);
          when(metadataSpy.getETag()).thenReturn(originalStatus.getETag());
          when(metadataSpy.getVersionId())
              .thenReturn(originalStatus.getVersionId());
          return metadataSpy;
        }
        return objectMetadata;
      }
    };

    // match requests made during select
    doAnswer(temporarilyOldMetadataAnswer).when(s3ClientSpy).getObjectMetadata(
        matchingMetadataRequest(testpath, null));
    doAnswer(temporarilyOldMetadataAnswer).when(s3ClientSpy).getObjectMetadata(
        matchingMetadataRequest(testpath, newStatus.getVersionId()));
  }

  /**
   * Writes a file with null ETag and versionId in the metadata store.
   */
  private Path writeFileWithNoVersionMetadata(String filename)
      throws IOException {
    final Path testpath = path(filename);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(out);
    printWriter.println(TEST_DATA);
    printWriter.close();
    final byte[] dataset = out.toByteArray();
    writeDataset(fs, testpath, dataset, dataset.length,
        1024, false);
    S3AFileStatus originalStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    // remove ETag and versionId
    S3AFileStatus newStatus = S3AFileStatus.fromFileStatus(originalStatus,
        Tristate.FALSE, null, null);
    fs.getMetadataStore().put(new PathMetadata(newStatus, Tristate.FALSE,
        false));

    return testpath;
  }

  /**
   * The test is invalid if the policy uses versionId but the bucket doesn't
   * have versioning enabled.
   */
  private void skipIfVersionPolicyAndNoVersionId(Path testpath)
      throws IOException {
    if (fs.getChangeDetectionPolicy().getSource() == Source.VersionId) {
      // skip versionId tests if the bucket doesn't have object versioning
      // enabled
      Assume.assumeTrue(
          "Target filesystem does not support versioning",
          fs.getObjectMetadata(fs.pathToKey(testpath)).getVersionId() != null);
    }
  }

  private GetObjectRequest matchingGetObjectRequest(Path path, String eTag,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getBucketName().equals(fs.getBucket())
          && request.getKey().equals(fs.pathToKey(path))) {
        if (eTag == null && !request.getMatchingETagConstraints().isEmpty()) {
          return false;
        }
        if (eTag != null &&
            !request.getMatchingETagConstraints().contains(eTag)) {
          return false;
        }
        if (versionId == null && request.getVersionId() != null) {
          return false;
        }
        if (versionId != null && !versionId.equals(request.getVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  private CopyObjectRequest matchingCopyObjectRequest(Path path, String eTag,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getSourceBucketName().equals(fs.getBucket())
          && request.getSourceKey().equals(fs.pathToKey(path))) {
        if (eTag == null && !request.getMatchingETagConstraints().isEmpty()) {
          return false;
        }
        if (eTag != null &&
            !request.getMatchingETagConstraints().contains(eTag)) {
          return false;
        }
        if (versionId == null && request.getSourceVersionId() != null) {
          return false;
        }
        if (versionId != null &&
            !versionId.equals(request.getSourceVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  private GetObjectMetadataRequest matchingMetadataRequest(Path path,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getBucketName().equals(fs.getBucket())
          && request.getKey().equals(fs.pathToKey(path))) {
        if (versionId == null && request.getVersionId() != null) {
          return false;
        }
        if (versionId != null &&
            !versionId.equals(request.getVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  /**
   * Skip a test case if it needs S3Guard and the filesystem does
   * not have it.
   */
  private void requireS3Guard() {
    Assume.assumeTrue("S3Guard must be enabled", fs.hasMetadataStore());
  }

  /**
   * Skip a test case if S3 Select is not supported on this store.
   */
  private void requireS3Select() {
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
  }
}
