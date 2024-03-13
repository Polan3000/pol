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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Callbacks for the bulk delete operation.
 */
public class BulkDeleteOperationCallbacksImpl implements
    BulkDeleteOperation.BulkDeleteOperationCallbacks {

  private final String path;

  private final int pageSize;

  /** span for operations. */
  private final AuditSpan span;

  private final S3AStore store;


  public BulkDeleteOperationCallbacksImpl(final S3AStore store,
      String path, int pageSize, AuditSpan span) {
    this.span = span;
    this.pageSize = pageSize;
    this.path = path;
    this.store = store;
  }

  @Override
  @Retries.RetryTranslated
  public List<String> bulkDelete(final List<ObjectIdentifier> keysToDelete)
      throws MultiObjectDeleteException, IOException, IllegalArgumentException {
    span.activate();
    final int size = keysToDelete.size();
    checkArgument(size <= pageSize,
        "Too many paths to delete in one operation: %s", size);
    if (size == 0) {
      return Collections.emptyList();
    }
    if (size == 1) {
      store.deleteObject(store.getRequestFactory()
          .newDeleteObjectRequestBuilder(keysToDelete.get(0).key())
          .build());
      // no failures, so return an empty list
      return Collections.emptyList();
    }

    final DeleteObjectsResponse response = once("bulkDelete", path, () ->
        store.deleteObjects(store.getRequestFactory()
            .newBulkDeleteRequestBuilder(keysToDelete)
            .build())).getValue();
    final List<S3Error> errors = response.errors();
    if (errors.isEmpty()) {
      // all good.
      return Collections.emptyList();
    } else {
      return errors.stream()
          .map(S3Error::key)
          .collect(Collectors.toList());
    }
  }
}
