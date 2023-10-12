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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.SignerFactory;
import org.apache.hadoop.util.VersionInfo;
import org.apache.http.client.utils.URIBuilder;

import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_STS;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_TTL;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_TTL;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_DOMAIN;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_HOST;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_PASSWORD;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_PORT;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_SECURED;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_USERNAME;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_WORKSTATION;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_S3;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_STS;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;

/**
 * Methods for configuring the S3 client.
 * These methods are used when creating and configuring
 * {@link software.amazon.awssdk.services.s3.S3Client} which communicates with the S3 service.
 */
public final class AWSClientConfig {
  private static final Logger LOG = LoggerFactory.getLogger(AWSClientConfig.class);

  private AWSClientConfig() {
  }

  public static ClientOverrideConfiguration.Builder createClientConfigBuilder(Configuration conf,
      String awsServiceIdentifier) throws IOException {
    ClientOverrideConfiguration.Builder overrideConfigBuilder =
        ClientOverrideConfiguration.builder();

    initRequestTimeout(conf, overrideConfigBuilder);

    initUserAgent(conf, overrideConfigBuilder);

    String signer = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if (!signer.isEmpty()) {
      LOG.debug("Signer override = {}", signer);
      overrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.SIGNER,
          SignerFactory.createSigner(signer, SIGNING_ALGORITHM));
    }

    initSigner(conf, overrideConfigBuilder, awsServiceIdentifier);

    return overrideConfigBuilder;
  }

  /**
   * Configures the http client.
   *
   * @param conf The Hadoop configuration
   * @return Http client builder
   * @throws IOException on any problem
   */
  public static ApacheHttpClient.Builder createHttpClientBuilder(Configuration conf)
      throws IOException {
    ApacheHttpClient.Builder httpClientBuilder =
        ApacheHttpClient.builder();

    httpClientBuilder.maxConnections(S3AUtils.intOption(conf, MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS, 1));

    Duration connectionEstablishTimeout = getDuration(conf, ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT, TimeUnit.MILLISECONDS);

    Duration socketTimeout = getDuration(conf, SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    // set the connection TTL irrespective of whether the connection is in use or not.
    // this can balance requests over different S3 servers, and avoid failed
    // connections. See HADOOP-18845.
    Duration ttl = getDuration(conf, CONNECTION_TTL, (int)DEFAULT_CONNECTION_TTL, TimeUnit.SECONDS);

    httpClientBuilder.connectionTimeout(connectionEstablishTimeout);
    httpClientBuilder.socketTimeout(socketTimeout);
    httpClientBuilder.connectionTimeToLive(ttl);

    NetworkBinding.bindSSLChannelMode(conf, httpClientBuilder);

    return httpClientBuilder;
  }

  /**
   * Configures the async http client.
   *
   * @param conf The Hadoop configuration
   * @return Http client builder
   */
  public static NettyNioAsyncHttpClient.Builder createAsyncHttpClientBuilder(Configuration conf) {
    NettyNioAsyncHttpClient.Builder httpClientBuilder =
        NettyNioAsyncHttpClient.builder();

    httpClientBuilder.maxConcurrency(S3AUtils.intOption(conf, MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS, 1));
    Duration connectionEstablishTimeout = getDuration(conf, ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT, TimeUnit.MILLISECONDS);

    Duration socketTimeout = getDuration(conf, SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    // set the connection TTL irrespective of whether the connection is in use or not.
    // this can balance requests over different S3 servers, and avoid failed
    // connections. See HADOOP-18845.
    Duration ttl = getDuration(conf, CONNECTION_TTL, (int)DEFAULT_CONNECTION_TTL, TimeUnit.MILLISECONDS);

    httpClientBuilder.connectionTimeout(connectionEstablishTimeout);
    httpClientBuilder.readTimeout(socketTimeout);
    httpClientBuilder.writeTimeout(socketTimeout);
    httpClientBuilder.connectionTimeToLive(ttl);

    // TODO: Don't think you can set a socket factory for the netty client.
    //  NetworkBinding.bindSSLChannelMode(conf, awsConf);

    return httpClientBuilder;
  }

  /**
   * Configures the retry policy.
   * Retry policy is {@code RetryMode.ADAPTIVE}, which
   * "dynamically limits the rate of AWS requests to maximize success rate",
   * possibly at the expense of latency.
   * Based on the ABFS experience, it is better to limit the rate requests are
   * made rather than have to resort to exponential backoff after failures come
   * in -especially as that backoff is per http connection.
   *
   * @param conf The Hadoop configuration
   * @return Retry policy builder
   */
  public static RetryPolicy.Builder createRetryPolicyBuilder(Configuration conf) {

    RetryPolicy.Builder retryPolicyBuilder = RetryPolicy.builder(RetryMode.ADAPTIVE);

    retryPolicyBuilder.numRetries(S3AUtils.intOption(conf, MAX_ERROR_RETRIES,
        DEFAULT_MAX_ERROR_RETRIES, 0));

    return retryPolicyBuilder;
  }

  /**
   * Configures the proxy.
   *
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return Proxy configuration
   * @throws IOException on any IO problem
   */
  public static ProxyConfiguration createProxyConfiguration(Configuration conf,
      String bucket) throws IOException {

    ProxyConfiguration.Builder proxyConfigBuilder = ProxyConfiguration.builder();

    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);

    if (!proxyHost.isEmpty()) {
      if (proxyPort >= 0) {
        String scheme = conf.getBoolean(PROXY_SECURED, false) ? "https" : "http";
        proxyConfigBuilder.endpoint(buildURI(scheme, proxyHost, proxyPort));
      } else {
        if (conf.getBoolean(PROXY_SECURED, false)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          proxyConfigBuilder.endpoint(buildURI("https", proxyHost, 443));
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          proxyConfigBuilder.endpoint(buildURI("http", proxyHost, 80));
        }
      }
      final String proxyUsername = S3AUtils.lookupPassword(bucket, conf, PROXY_USERNAME,
          null, null);
      final String proxyPassword = S3AUtils.lookupPassword(bucket, conf, PROXY_PASSWORD,
          null, null);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      proxyConfigBuilder.username(proxyUsername);
      proxyConfigBuilder.password(proxyPassword);
      proxyConfigBuilder.ntlmDomain(conf.getTrimmed(PROXY_DOMAIN));
      proxyConfigBuilder.ntlmWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on "
                + "domain {} as workstation {}", proxyHost, proxyPort, proxyUsername, proxyPassword,
            PROXY_DOMAIN, PROXY_WORKSTATION);
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    return proxyConfigBuilder.build();

  }

  /**
   * Configures the proxy for the async http client.
   *
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return Proxy configuration
   * @throws IOException on any IO problem
   */
  public static software.amazon.awssdk.http.nio.netty.ProxyConfiguration
      createAsyncProxyConfiguration(Configuration conf,
          String bucket) throws IOException {

    software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigBuilder =
        software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder();

    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);

    if (!proxyHost.isEmpty()) {
      if (proxyPort >= 0) {
        String scheme = conf.getBoolean(PROXY_SECURED, false) ? "https" : "http";
        proxyConfigBuilder.host(proxyHost);
        proxyConfigBuilder.port(proxyPort);
        proxyConfigBuilder.scheme(scheme);
      } else {
        if (conf.getBoolean(PROXY_SECURED, false)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          proxyConfigBuilder.host(proxyHost);
          proxyConfigBuilder.port(443);
          proxyConfigBuilder.scheme("https");
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          proxyConfigBuilder.host(proxyHost);
          proxyConfigBuilder.port(80);
          proxyConfigBuilder.scheme("http");
        }
      }
      final String proxyUsername = S3AUtils.lookupPassword(bucket, conf, PROXY_USERNAME,
          null, null);
      final String proxyPassword = S3AUtils.lookupPassword(bucket, conf, PROXY_PASSWORD,
          null, null);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      proxyConfigBuilder.username(proxyUsername);
      proxyConfigBuilder.password(proxyPassword);
      // TODO: check NTLM support
      // proxyConfigBuilder.ntlmDomain(conf.getTrimmed(PROXY_DOMAIN));
      // proxyConfigBuilder.ntlmWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on "
                + "domain {} as workstation {}", proxyHost, proxyPort, proxyUsername, proxyPassword,
            PROXY_DOMAIN, PROXY_WORKSTATION);
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    } else {
      return null;
    }

    return proxyConfigBuilder.build();
  }

  /***
   * Builds a URI, throws an IllegalArgumentException in case of errors.
   *
   * @param host proxy host
   * @param port proxy port
   * @return uri with host and port
   */
  private static URI buildURI(String scheme, String host, int port) {
    try {
      return new URIBuilder().setScheme(scheme).setHost(host).setPort(port).build();
    } catch (URISyntaxException e) {
      String msg =
          "Proxy error: incorrect " + PROXY_HOST + " or " + PROXY_PORT;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Initializes the User-Agent header to send in HTTP requests to AWS
   * services.  We always include the Hadoop version number.  The user also
   * may set an optional custom prefix to put in front of the Hadoop version
   * number.  The AWS SDK internally appends its own information, which seems
   * to include the AWS SDK version, OS and JVM version.
   *
   * @param conf Hadoop configuration
   * @param clientConfig AWS SDK configuration to update
   */
  private static void initUserAgent(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    String userAgentPrefix = conf.getTrimmed(USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }
    LOG.debug("Using User-Agent: {}", userAgent);
    clientConfig.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent);
  }

  private static void initSigner(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig, String awsServiceIdentifier)
      throws IOException {
    String configKey = null;
    switch (awsServiceIdentifier) {
    case AWS_SERVICE_IDENTIFIER_S3:
      configKey = SIGNING_ALGORITHM_S3;
      break;
    case AWS_SERVICE_IDENTIFIER_STS:
      configKey = SIGNING_ALGORITHM_STS;
      break;
    default:
      // Nothing to do. The original signer override is already setup
    }
    if (configKey != null) {
      String signerOverride = conf.getTrimmed(configKey, "");
      if (!signerOverride.isEmpty()) {
        LOG.debug("Signer override for {} = {}", awsServiceIdentifier, signerOverride);
        clientConfig.putAdvancedOption(SdkAdvancedClientOption.SIGNER,
            SignerFactory.createSigner(signerOverride, configKey));
      }
    }
  }

  /**
   * Configures request timeout.
   *
   * @param conf Hadoop configuration
   * @param clientConfig AWS SDK configuration to update
   */
  private static void initRequestTimeout(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig) {
    final Duration requestTimeoutMillis = getDuration(conf, REQUEST_TIMEOUT,
        DEFAULT_REQUEST_TIMEOUT, TimeUnit.SECONDS);

    if (requestTimeoutMillis.toMillis() > 0) {
      clientConfig.apiCallAttemptTimeout(requestTimeoutMillis);
    }
  }

  /**
   * Get duration. This may be negative; callers must check.
   * If the config option is greater than {@code Integer.MAX_VALUE} milliseconds,
   * it is set to that max.
   * Logs the value for diagnostics.
   * @param conf config
   * @param name option name
   * @param defVal default value
   * @param defaultUnit unit of default value
   * @return duration. may be negative.
   */
  private static Duration getDuration(final Configuration conf,
      final String name,
      final int defVal,
      final TimeUnit defaultUnit) {
    long timeMillis = conf.getTimeDuration(name,
        defVal, defaultUnit, TimeUnit.MILLISECONDS);

    if (timeMillis > Integer.MAX_VALUE) {
      LOG.debug("Request timeout is too high({} ms). Setting to {} ms instead",
          timeMillis, Integer.MAX_VALUE);
      timeMillis = Integer.MAX_VALUE;
    }
    final Duration duration = Duration.ofMillis(timeMillis);
    LOG.debug("Duration of {} = {}", name, duration);
    return duration;
  }
}
