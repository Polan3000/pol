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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import java.util.Map;
import org.apache.hadoop.fs.azurebfs.AbfsBackoffMetrics;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.util.Time.now;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * The AbfsRestOperation for Rest AbfsClient.
 */
public class AbfsRestOperation {
  // The type of the REST operation (Append, ReadFile, etc)
  private final AbfsRestOperationType operationType;
  // Blob FS client, which has the credentials, retry policy, and logs.
  private final AbfsClient client;
  // Return intercept instance
  private final AbfsThrottlingIntercept intercept;
  // the HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE)
  private final String method;
  // full URL including query parameters
  private final URL url;
  // all the custom HTTP request headers provided by the caller
  private final List<AbfsHttpHeader> requestHeaders;

  // This is a simple operation class, where all the upload methods have a
  // request body and all the download methods have a response body.
  private final boolean hasRequestBody;

  // Used only by AbfsInputStream/AbfsOutputStream to reuse SAS tokens.
  private final String sasToken;

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  private static final Logger LOG1 = LoggerFactory.getLogger(AbfsRestOperation.class);
  // For uploads, this is the request entity body.  For downloads,
  // this will hold the response entity body.
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private int retryCount = 0;
  private boolean isThrottledRequest = false;
  private long maxRetryCount = 0L;
  private final int maxIoRetries;
  private AbfsHttpOperation result;
  private final AbfsCounters abfsCounters;
  private AbfsBackoffMetrics abfsBackoffMetrics;
  private Map<String, AbfsBackoffMetrics> metricsMap;
  /**
   * This variable contains the reason of last API call within the same
   * AbfsRestOperation object.
   */
  private String failureReason;
  private AbfsRetryPolicy retryPolicy;

  /**
   * This variable stores the tracing context used for last Rest Operation.
   */
  private TracingContext lastUsedTracingContext;

  /**
   * Checks if there is non-null HTTP response.
   * @return true if there is a non-null HTTP response from the ABFS call.
   */
  public boolean hasResult() {
    return result != null;
  }

  public AbfsHttpOperation getResult() {
    return result;
  }

  public void hardSetResult(int httpStatus) {
    result = AbfsHttpOperation.getAbfsHttpOperationWithFixedResult(this.url,
        this.method, httpStatus);
  }

  public URL getUrl() {
    return url;
  }

  public List<AbfsHttpHeader> getRequestHeaders() {
    return requestHeaders;
  }

  public boolean isARetriedRequest() {
    return (retryCount > 0);
  }

  String getSasToken() {
    return sasToken;
  }

  private static final int MIN_FIRST_RANGE = 1;
  private static final int MAX_FIRST_RANGE = 5;
  private static final int MAX_SECOND_RANGE = 15;
  private static final int MAX_THIRD_RANGE = 25;

  /**
   * Initializes a new REST operation.
   *
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   */
  AbfsRestOperation(final AbfsRestOperationType operationType,
                    final AbfsClient client,
                    final String method,
                    final URL url,
                    final List<AbfsHttpHeader> requestHeaders) {
    this(operationType, client, method, url, requestHeaders, null);
  }

  /**
   * Initializes a new REST operation.
   *
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   * @param sasToken A sasToken for optional re-use by AbfsInputStream/AbfsOutputStream.
   */
  AbfsRestOperation(final AbfsRestOperationType operationType,
                    final AbfsClient client,
                    final String method,
                    final URL url,
                    final List<AbfsHttpHeader> requestHeaders,
                    final String sasToken) {
    this.operationType = operationType;
    this.client = client;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.hasRequestBody = (AbfsHttpConstants.HTTP_METHOD_PUT.equals(method)
            || AbfsHttpConstants.HTTP_METHOD_POST.equals(method)
            || AbfsHttpConstants.HTTP_METHOD_PATCH.equals(method));
    this.sasToken = sasToken;
    this.abfsCounters = client.getAbfsCounters();
    if (abfsCounters != null) {
      this.abfsBackoffMetrics = abfsCounters.getAbfsBackoffMetrics();
    }
    if (abfsBackoffMetrics != null) {
      this.metricsMap = abfsBackoffMetrics.getMetricsMap();
    }
    this.maxIoRetries = client.getAbfsConfiguration().getMaxIoRetries();
    this.intercept = client.getIntercept();
    this.retryPolicy = client.getExponentialRetryPolicy();
  }

  /**
   * Initializes a new REST operation.
   *
   * @param operationType The type of the REST operation (Append, ReadFile, etc).
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   * @param buffer For uploads, this is the request entity body.  For downloads,
   *               this will hold the response entity body.
   * @param bufferOffset An offset into the buffer where the data beings.
   * @param bufferLength The length of the data in the buffer.
   * @param sasToken A sasToken for optional re-use by AbfsInputStream/AbfsOutputStream.
   */
  AbfsRestOperation(AbfsRestOperationType operationType,
                    AbfsClient client,
                    String method,
                    URL url,
                    List<AbfsHttpHeader> requestHeaders,
                    byte[] buffer,
                    int bufferOffset,
                    int bufferLength,
                    String sasToken) {
    this(operationType, client, method, url, requestHeaders, sasToken);
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
  }

  /**
   * Execute a AbfsRestOperation. Track the Duration of a request if
   * abfsCounters isn't null.
   * @param tracingContext TracingContext instance to track correlation IDs
   */
  public void execute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // Since this might be a sub-sequential or parallel rest operation
    // triggered by a single file system call, using a new tracing context.
    lastUsedTracingContext = createNewTracingContext(tracingContext);
    try {
      abfsCounters.getLastExecutionTime().set(now());
      client.timerOrchestrator(TimerFunctionality.RESUME, null);
      IOStatisticsBinding.trackDurationOfInvocation(abfsCounters,
          AbfsStatistic.getStatNameFromHttpCall(method),
          () -> completeExecute(lastUsedTracingContext));
    } catch (AzureBlobFileSystemException aze) {
      throw aze;
    } catch (IOException e) {
      throw new UncheckedIOException("Error while tracking Duration of an "
          + "AbfsRestOperation call", e);
    }
  }

  /**
   * Executes the REST operation with retry, by issuing one or more
   * HTTP operations.
   * @param tracingContext TracingContext instance to track correlation IDs
   */
  void completeExecute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // see if we have latency reports from the previous requests
    String latencyHeader = getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    // By Default Exponential Retry Policy Will be used
    retryCount = 0;
    retryPolicy = client.getExponentialRetryPolicy();
    LOG.debug("First execution of REST operation - {}", operationType);
    long sleepDuration = 0L;
    if (abfsBackoffMetrics != null) {
      synchronized (this) {
        abfsBackoffMetrics.incrementTotalRequests();
      }
    }
    while (!executeHttpOperation(retryCount, tracingContext)) {
      try {
        ++retryCount;
        tracingContext.setRetryCount(retryCount);
        long retryInterval = retryPolicy.getRetryInterval(retryCount);
        LOG.debug("Rest operation {} failed with failureReason: {}. Retrying with retryCount = {}, retryPolicy: {} and sleepInterval: {}",
            operationType, failureReason, retryCount, retryPolicy.getAbbreviation(), retryInterval);
        if (abfsBackoffMetrics != null) {
          updateBackoffTimeMetrics(retryCount, sleepDuration);
        }
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    if (abfsBackoffMetrics != null) {
      updateBackoffMetrics(retryCount, result.getStatusCode());
    }
    int status = result.getStatusCode();
    /*
      If even after exhausting all retries, the http status code has an
      invalid value it qualifies for InvalidAbfsRestOperationException.
      All http status code less than 1xx range are considered as invalid
      status codes.
     */
    if (status < HTTP_CONTINUE) {
      throw new InvalidAbfsRestOperationException(null, retryCount);
    }

    if (status >= HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new AbfsRestOperationException(result.getStatusCode(), result.getStorageErrorCode(),
          result.getStorageErrorMessage(), null, result);
    }
    LOG.trace("{} REST operation complete", operationType);
  }

  private void updateBackoffMetrics(int retryCount, int statusCode) {
    if (statusCode < HttpURLConnection.HTTP_OK
            || statusCode >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
      synchronized (this) {
        if (retryCount >= maxIoRetries) {
          abfsBackoffMetrics.incrementNumberOfRequestsFailed();
        }
      }
    } else {
      synchronized (this) {
        if (retryCount > ZERO && retryCount <= maxIoRetries) {
          maxRetryCount = Math.max(abfsBackoffMetrics.getMaxRetryCount(), retryCount);
          abfsBackoffMetrics.setMaxRetryCount(maxRetryCount);
          updateCount(retryCount);
        } else {
          abfsBackoffMetrics.incrementNumberOfRequestsSucceededWithoutRetrying();
        }
      }
    }
  }

  @VisibleForTesting
  String getClientLatency() {
    return client.getAbfsPerfTracker().getClientLatency();
  }

  /**
   * Executes a single HTTP operation to complete the REST operation.  If it
   * fails, there may be a retry.  The retryCount is incremented with each
   * attempt.
   */
  private boolean executeHttpOperation(final int retryCount,
    TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsHttpOperation httpOperation;
    boolean wasIOExceptionThrown = false;

    try {
      // initialize the HTTP request and open the connection
      httpOperation = createHttpOperation();
      incrementCounter(AbfsStatistic.CONNECTIONS_MADE, 1);
      tracingContext.constructHeader(httpOperation, failureReason, retryPolicy.getAbbreviation());

      signRequest(httpOperation, hasRequestBody ? bufferLength : 0);

    } catch (IOException e) {
      LOG.debug("Auth failure: {}, {}", method, url);
      throw new AbfsRestOperationException(-1, null,
          "Auth failure: " + e.getMessage(), e);
    }

    try {
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          httpOperation.getConnection().getRequestProperties());
      intercept.sendingRequest(operationType, abfsCounters);
      if (hasRequestBody) {
        // HttpUrlConnection requires
        httpOperation.sendRequest(buffer, bufferOffset, bufferLength);
        incrementCounter(AbfsStatistic.SEND_REQUESTS, 1);
        incrementCounter(AbfsStatistic.BYTES_SENT, bufferLength);
      }

      httpOperation.processResponse(buffer, bufferOffset, bufferLength);
      if (!isThrottledRequest && httpOperation.getStatusCode()
          >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
        isThrottledRequest = true;
        AzureServiceErrorCode serviceErrorCode =
            AzureServiceErrorCode.getAzureServiceCode(
                httpOperation.getStatusCode(),
                httpOperation.getStorageErrorCode(),
                httpOperation.getStorageErrorMessage());
        LOG1.trace("Service code is " + serviceErrorCode + " status code is "
            + httpOperation.getStatusCode() + " error code is "
            + httpOperation.getStorageErrorCode()
            + " error message is " + httpOperation.getStorageErrorMessage());
        if (abfsBackoffMetrics != null) {
          synchronized (this) {
            if (serviceErrorCode.equals(
                    AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT)
                    || serviceErrorCode.equals(
                    AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT)) {
              abfsBackoffMetrics.incrementNumberOfBandwidthThrottledRequests();
            } else if (serviceErrorCode.equals(
                    AzureServiceErrorCode.REQUEST_OVER_ACCOUNT_LIMIT)) {
              abfsBackoffMetrics.incrementNumberOfIOPSThrottledRequests();
            } else {
              abfsBackoffMetrics.incrementNumberOfOtherThrottledRequests();
            }
          }
        }
      }
        incrementCounter(AbfsStatistic.GET_RESPONSES, 1);
      //Only increment bytesReceived counter when the status code is 2XX.
      if (httpOperation.getStatusCode() >= HttpURLConnection.HTTP_OK
          && httpOperation.getStatusCode() <= HttpURLConnection.HTTP_PARTIAL) {
        incrementCounter(AbfsStatistic.BYTES_RECEIVED,
            httpOperation.getBytesReceived());
      } else if (httpOperation.getStatusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
        incrementCounter(AbfsStatistic.SERVER_UNAVAILABLE, 1);
      }
    } catch (UnknownHostException ex) {
      String hostname = null;
      hostname = httpOperation.getHost();
      failureReason = RetryReason.getAbbreviation(ex, null, null);
      retryPolicy = client.getRetryPolicy(failureReason);
      LOG.warn("Unknown host name: {}. Retrying to resolve the host name...",
          hostname);
      if (abfsBackoffMetrics != null) {
        synchronized (this) {
          abfsBackoffMetrics.incrementNumberOfNetworkFailedRequests();
        }
      }
      if (!retryPolicy.shouldRetry(retryCount, -1)) {
        updateBackoffMetrics(retryCount, httpOperation.getStatusCode());
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }
      return false;
    } catch (IOException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("HttpRequestFailure: {}, {}", httpOperation, ex);
      }
      if (abfsBackoffMetrics != null) {
        synchronized (this) {
          abfsBackoffMetrics.incrementNumberOfNetworkFailedRequests();
        }
      }
      failureReason = RetryReason.getAbbreviation(ex, -1, "");
      retryPolicy = client.getRetryPolicy(failureReason);
      wasIOExceptionThrown = true;
      if (!retryPolicy.shouldRetry(retryCount, -1)) {
        updateBackoffMetrics(retryCount, httpOperation.getStatusCode());
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }
      return false;
    } finally {
      int status = httpOperation.getStatusCode();
      /*
       A status less than 300 (2xx range) or greater than or equal
       to 500 (5xx range) should contribute to throttling metrics being updated.
       Less than 200 or greater than or equal to 500 show failed operations. 2xx
       range contributes to successful operations. 3xx range is for redirects
       and 4xx range is for user errors. These should not be a part of
       throttling backoff computation.
       */
      boolean updateMetricsResponseCode = (status < HttpURLConnection.HTTP_MULT_CHOICE
              || status >= HttpURLConnection.HTTP_INTERNAL_ERROR);

      /*
       Connection Timeout failures should not contribute to throttling
       In case the current request fails with Connection Timeout we will have
       ioExceptionThrown true and failure reason as CT
       In case the current request failed with 5xx, failure reason will be
       updated after finally block but wasIOExceptionThrown will be false;
       */
      boolean isCTFailure = CONNECTION_TIMEOUT_ABBREVIATION.equals(failureReason) && wasIOExceptionThrown;

      if (updateMetricsResponseCode && !isCTFailure) {
        intercept.updateMetrics(operationType, httpOperation);
      }
    }

    LOG.debug("HttpRequest: {}: {}", operationType, httpOperation);

    int status = httpOperation.getStatusCode();
    failureReason = RetryReason.getAbbreviation(null, status, httpOperation.getStorageErrorMessage());
    retryPolicy = client.getRetryPolicy(failureReason);

    if (retryPolicy.shouldRetry(retryCount, httpOperation.getStatusCode())) {
      return false;
    }

    result = httpOperation;

    return true;
  }

  /**
   * Sign an operation.
   * @param httpOperation operation to sign
   * @param bytesToSign how many bytes to sign for shared key auth.
   * @throws IOException failure
   */
  @VisibleForTesting
  public void signRequest(final AbfsHttpOperation httpOperation, int bytesToSign) throws IOException {
    switch(client.getAuthType()) {
      case Custom:
      case OAuth:
        LOG.debug("Authenticating request with OAuth2 access token");
        httpOperation.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
            client.getAccessToken());
        break;
      case SAS:
        // do nothing; the SAS token should already be appended to the query string
        httpOperation.setMaskForSAS(); //mask sig/oid from url for logs
        break;
      case SharedKey:
      default:
        // sign the HTTP request
        LOG.debug("Signing request with shared key");
        // sign the HTTP request
        client.getSharedKeyCredentials().signRequest(
            httpOperation.getConnection(),
            bytesToSign);
        break;
    }
  }

  /**
   * Creates new object of {@link AbfsHttpOperation} with the url, method, requestHeader fields and
   * timeout values as set in configuration of the AbfsRestOperation object.
   *
   * @return {@link AbfsHttpOperation} to be used for sending requests
   */
  @VisibleForTesting
  AbfsHttpOperation createHttpOperation() throws IOException {
    return new AbfsHttpOperation(url, method, requestHeaders,
            client.getAbfsConfiguration().getHttpConnectionTimeout(),
            client.getAbfsConfiguration().getHttpReadTimeout());
  }

  /**
   * Incrementing Abfs counters with a long value.
   *
   * @param statistic the Abfs statistic that needs to be incremented.f
   * @param value     the value to be incremented by.
   */
  private void incrementCounter(AbfsStatistic statistic, long value) {
    if (abfsCounters != null) {
      abfsCounters.incrementCounter(statistic, value);
    }
  }

  /**
   * Updates the count metrics based on the provided retry count.
   * @param retryCount The retry count used to determine the metrics category.
   *
   * This method increments the number of succeeded requests for the specified retry count.
   */
  private void updateCount(int retryCount){
      String retryCounter = getKey(retryCount);
      metricsMap.get(retryCounter).incrementNumberOfRequestsSucceeded();
  }

  /**
   * Updates backoff time metrics based on the provided retry count and sleep duration.
   * @param retryCount    The retry count used to determine the metrics category.
   * @param sleepDuration The duration of sleep during backoff.
   *
   * This method calculates and updates various backoff time metrics, including minimum, maximum,
   * and total backoff time, as well as the total number of requests for the specified retry count.
   */
  private void updateBackoffTimeMetrics(int retryCount, long sleepDuration) {
    synchronized (this) {
      String retryCounter = getKey(retryCount);
      AbfsBackoffMetrics abfsBackoffMetrics = metricsMap.get(retryCounter);
      long minBackoffTime = Math.min(abfsBackoffMetrics.getMinBackoff(), sleepDuration);
      long maxBackoffForTime = Math.max(abfsBackoffMetrics.getMaxBackoff(), sleepDuration);
      long totalBackoffTime = abfsBackoffMetrics.getTotalBackoff() + sleepDuration;
      abfsBackoffMetrics.incrementTotalRequests();
      abfsBackoffMetrics.setMinBackoff(minBackoffTime);
      abfsBackoffMetrics.setMaxBackoff(maxBackoffForTime);
      abfsBackoffMetrics.setTotalBackoff(totalBackoffTime);
      metricsMap.put(retryCounter, abfsBackoffMetrics);
    }
  }

  /**
   * Generates a key based on the provided retry count to categorize metrics.
   *
   * @param retryCount The retry count used to determine the key.
   * @return A string key representing the metrics category for the given retry count.
   *
   * This method categorizes retry counts into different ranges and assigns a corresponding key.
   */
  private String getKey(int retryCount) {
    if (retryCount >= MIN_FIRST_RANGE && retryCount < MAX_FIRST_RANGE) {
      return Integer.toString(retryCount);
    } else if (retryCount >= MAX_FIRST_RANGE && retryCount < MAX_SECOND_RANGE) {
      return "5_15";
    } else if (retryCount >= MAX_SECOND_RANGE && retryCount < MAX_THIRD_RANGE) {
      return "15_25";
    } else {
      return "25AndAbove";
    }
  }

  /**
   * Creates a new Tracing context before entering the retry loop of a rest operation.
   * This will ensure all rest operations have unique
   * tracing context that will be used for all the retries.
   * @param tracingContext original tracingContext.
   * @return tracingContext new tracingContext object created from original one.
   */
  @VisibleForTesting
  public TracingContext createNewTracingContext(final TracingContext tracingContext) {
    return new TracingContext(tracingContext);
  }

  /**
   * Returns the tracing contest used for last rest operation made.
   * @return tracingContext lasUserTracingContext.
   */
  @VisibleForTesting
  public final TracingContext getLastTracingContext() {
    return lastUsedTracingContext;
  }
}
