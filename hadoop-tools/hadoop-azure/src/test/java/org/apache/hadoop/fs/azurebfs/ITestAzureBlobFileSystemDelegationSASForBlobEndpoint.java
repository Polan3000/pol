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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

public class ITestAzureBlobFileSystemDelegationSASForBlobEndpoint extends  AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemDelegationSASForBlobEndpoint()
      throws Exception {
    // These tests rely on specific settings in azure-auth-keys.xml:
    String sasProvider = getRawConfiguration().get(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    Assume.assumeTrue(MockDelegationSASTokenProvider.class.getCanonicalName().equals(sasProvider));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_ID));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID));
    // The test uses shared key to create a random filesystem and then creates another
    // instance of this filesystem using SAS authorization.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    createFilesystemForSASTests();
    super.setup();
    Assume.assumeTrue(getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
  }

  @Before
  public void before() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
  }

  @Test
  public void testPosixRenameDirectoryWhereDirectoryAlreadyThereOnDestination() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.mkdirs(new Path("testDir2/test4/test3"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    Assert.assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if(getIsNamespaceEnabled(fs) || fs.getAbfsStore().getAbfsConfiguration().get("fs.azure.abfs.account.name").contains(".dfs.core") ||fs.getAbfsStore().getAbfsConfiguration().get("fs.azure.abfs.account.name").contains(".blob.core")) {
      assertFalse(fs.exists(new Path("testDir2/test4/test3/file")));
      assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    } else {
      assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
      assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    }
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
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    fileSystem.setAbfsStore(store);
    Mockito.doReturn(COPY_STATUS_PENDING).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    fileSystem.create(new Path("/test1/file"));
    fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class), Mockito.any(
            TracingContext.class), Mockito.any(String.class));
  }
}
