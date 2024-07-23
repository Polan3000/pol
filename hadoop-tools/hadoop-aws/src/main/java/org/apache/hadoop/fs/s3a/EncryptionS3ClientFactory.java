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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.encryption.CSEMaterials;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.functional.LazyAtomicReference;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.encryption.s3.S3AsyncEncryptionClient;
import software.amazon.encryption.s3.S3EncryptionClient;
import software.amazon.encryption.s3.materials.CryptographicMaterialsManager;
import software.amazon.encryption.s3.materials.DefaultCryptoMaterialsManager;
import software.amazon.encryption.s3.materials.Keyring;

import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unavailable;

/**
 * Factory class to create encrypted s3 client and encrypted async s3 client.
 */
public class EncryptionS3ClientFactory extends DefaultS3ClientFactory {

  private static final String ENCRYPTION_CLIENT_CLASSNAME =
      "software.amazon.encryption.s3.S3EncryptionClient";

  /**
   * Encryption client availability.
   */
  private static final LazyAtomicReference<Boolean> ENCRYPTION_CLIENT_AVAILABLE =
      LazyAtomicReference.lazyAtomicReferenceFromSupplier(
          EncryptionS3ClientFactory::checkForEncryptionClient
      );


  /**
   * S3Client to be wrapped by encryption client.
   */
  private S3Client s3Client;

  /**
   * S3AsyncClient to be wrapped by encryption client.
   */
  private S3AsyncClient s3AsyncClient;

  private static boolean checkForEncryptionClient() {
    try {
      ClassLoader cl = EncryptionS3ClientFactory.class.getClassLoader();
      cl.loadClass(ENCRYPTION_CLIENT_CLASSNAME);
      LOG.debug("encryption client class {} found", ENCRYPTION_CLIENT_CLASSNAME);
      return true;
    } catch (Exception e) {
      LOG.debug("encryption client class {} not found", ENCRYPTION_CLIENT_CLASSNAME, e);
      return false;
    }
  }

  /**
   * Is the Encryption client available?
   * @return true if it was found in the classloader
   */
  private static synchronized boolean isEncryptionClientAvailable() {
    return ENCRYPTION_CLIENT_AVAILABLE.get();
  }

  /**
   * Creates both synchronous and asynchronous encrypted s3 clients.
   * Synchronous client is wrapped by encryption client first and then
   * Asynchronous client is wrapped by encryption client.
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return encrypted s3 client
   * @throws IOException IO failures
   */
  @Override
  public S3Client createS3Client(URI uri, S3ClientCreationParameters parameters)
      throws IOException {
    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null,
          "No encryption client available");
    }

    s3Client = super.createS3Client(uri, parameters);
    s3AsyncClient = super.createS3AsyncClient(uri, parameters);

    return createS3EncryptionClient(parameters.getClientSideEncryptionMaterials());
  }

  /**
   * Create async encrypted s3 client.
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return async encrypted s3 client
   * @throws IOException IO failures
   */
  @Override
  public S3AsyncClient createS3AsyncClient(URI uri, S3ClientCreationParameters parameters)
      throws IOException {
    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null,
          "No encryption client available");
    }
    return createS3AsyncEncryptionClient(parameters.getClientSideEncryptionMaterials());
  }

  /**
   * Create encrypted s3 client.
   * @param cseMaterials
   * @return encrypted s3 client
   */
  private S3Client createS3EncryptionClient(final CSEMaterials cseMaterials) {
    Preconditions.checkArgument(s3AsyncClient !=null,
        "S3 async client not initialized");
    Preconditions.checkArgument(s3Client !=null,
        "S3 client not initialized");
    S3EncryptionClient.Builder s3EncryptionClientBuilder =
        S3EncryptionClient.builder().wrappedAsyncClient(s3AsyncClient).wrappedClient(s3Client)
            // this is required for doing S3 ranged GET calls
            .enableLegacyUnauthenticatedModes(true)
            // this is required for backward compatibility with older encryption clients
            .enableLegacyWrappingAlgorithms(true);

    switch (cseMaterials.getCseKeyType()) {
    case KMS:
      s3EncryptionClientBuilder.kmsKeyId(cseMaterials.getKmsKeyId());
      break;
    case CUSTOM:
      Keyring keyring = getKeyringProvider(cseMaterials.getCustomKeyringClassName(),
          cseMaterials.getConf());
      CryptographicMaterialsManager cmm =  DefaultCryptoMaterialsManager.builder()
          .keyring(keyring)
          .build();
      s3EncryptionClientBuilder.cryptoMaterialsManager(cmm);
      break;
    default:
      break;
    }

    return s3EncryptionClientBuilder.build();
  }

  /**
   * Create async encrypted s3 client.
   * @param cseMaterials
   * @return encrypted async s3 client
   */
  private S3AsyncClient createS3AsyncEncryptionClient(final CSEMaterials cseMaterials) {
    Preconditions.checkArgument(s3AsyncClient !=null,
        "S3 async client not initialized");
    S3AsyncEncryptionClient.Builder s3EncryptionAsyncClientBuilder =
        S3AsyncEncryptionClient.builder().wrappedClient(s3AsyncClient)
            // this is required for doing S3 ranged GET calls
            .enableLegacyUnauthenticatedModes(true)
            // this is required for backward compatibility with older encryption clients
            .enableLegacyWrappingAlgorithms(true);

    switch (cseMaterials.getCseKeyType()) {
    case KMS:
      s3EncryptionAsyncClientBuilder.kmsKeyId(cseMaterials.getKmsKeyId());
      break;
    case CUSTOM:
      Keyring keyring = getKeyringProvider(cseMaterials.getCustomKeyringClassName(),
          cseMaterials.getConf());
      CryptographicMaterialsManager cmm =  DefaultCryptoMaterialsManager.builder()
          .keyring(keyring)
          .build();
      s3EncryptionAsyncClientBuilder.cryptoMaterialsManager(cmm);
      break;
    default:
      break;
    }

    return s3EncryptionAsyncClientBuilder.build();
  }

  /**
   * Get the custom Keyring class.
   * @param className
   * @param conf
   * @return custom keyring class
   */
  private Keyring getKeyringProvider(String className,
      Configuration conf) {
    try {
      return ReflectionUtils.newInstance(getCustomKeyringProviderClass(className), conf);
    } catch (Exception e) {
      // this is for testing purpose to support CustomKeyring.java
      return ReflectionUtils.newInstance(getCustomKeyringProviderClass(className), conf,
          new Class[] {Configuration.class}, conf);
    }
  }

  private Class<? extends Keyring> getCustomKeyringProviderClass(String className) {
    Preconditions.checkArgument(className !=null && !className.isEmpty(),
        "Custom Keyring class name is null or empty");
    try {
      return Class.forName(className).asSubclass(Keyring.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Custom CryptographicMaterialsManager class " + className + "not found", e);
    }
  }
}