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

package org.apache.hadoop.fs.store;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.StreamCapabilities;

import static org.apache.hadoop.fs.FSExceptionMessages.*;
import static org.apache.hadoop.fs.StreamCapabilities.HFLUSH;
import static org.apache.hadoop.fs.StreamCapabilities.HSYNC;

/**
 * Utility classes to help implementing filesystems and streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StoreImplementationUtils {

  private StoreImplementationUtils() {
  }

  /**
   * Check the supplied capabilities for being those required for full
   * {@code Syncable.hsync()} and {@code Syncable.hflush()} functionality.
   * @param capability capability string.
   * @return true if either refers to one of the Syncable operations.
   */
  public static boolean supportsSyncable(String capability) {
    return capability.equalsIgnoreCase(HSYNC) ||
        capability.equalsIgnoreCase((HFLUSH));
  }

  /**
   * Probe for an object having a capability; returns true
   * iff the stream implements {@link StreamCapabilities} and its
   * {@code hasCapabilities()} method returns true for the capability.
   * This is a private method intended to provided a common implementation
   * for input, output streams; only the stronger typed
   * @param obj output stream
   * @param capability capability to probe for
   * @return true iff the object declares that it supports the capability.
   */
  private static boolean objectHasCapability(Object obj, String capability) {
    if (obj instanceof StreamCapabilities) {
      return ((StreamCapabilities) obj).hasCapability(capability);
    }
    return false;
  }

  /**
   * Probe for an autput stream having a capability; returns true
   * iff the stream implements {@link StreamCapabilities} and its
   * {@code hasCapabilities()} method returns true for the capability.
   * @param out output stream
   * @param capability capability to probe for
   * @return true iff the stream declares that it supports the capability.
   */
  public static boolean hasCapability(OutputStream out, String capability) {
    return objectHasCapability(out, capability);
  }

  /**
   * Class to manage {@code close()} logic.
   * A simple wrapper around an atomic boolean to guard against
   * calling operations when closed; {@link #checkOpen()}
   * will throw an exception when closed ... it should be
   * used in methods which require the stream/filesystem to be
   * open.
   *
   * The {@link #enterClose()} call can be used to ensure that
   * a stream is closed at most once.
   * It should be the first operation in the {@code close} method,
   * with the caller exiting immediately if the stream is already closed.
   * <pre>
   * public void close() throws IOException {
   *   if (!closed.enterClose()) {
   *     return;
   *   }
   *   ... close operations
   * }
   * </pre>
   */
  public static class CloseChecker {

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Path; if not empty then a {@link PathIOException} will be raised
     * containing this path.
     */
    private final String path;

    /**
     * Constructor.
     * @param path path to use in exception text.
     */
    public CloseChecker(String path) {
      this.path = path;
    }

    /**
     * Instantiate.
     * @param path path to use in exception messages.
     */
    public CloseChecker(Path path) {
      this.path = path.toUri().toString();
    }

    /**
     * Constructor without a path.
     */
    public CloseChecker() {
      this("");
    }

    /**
     * Enter the close call, non-reentrantly
     * @return true if the close() call can continue; false
     * if the state has been reached.
     */
    public boolean enterClose() {
      return closed.compareAndSet(false, true);
    }

    /**
     * Check for the stream being open, throwing an
     * exception if it is not.
     * @throws IOException if the stream is closed.
     * @throws PathIOException if the stream is closed and this checker
     * was constructed with a path.
     */
    public void checkOpen() throws IOException {
      if (isClosed()) {
        if (StringUtils.isNotEmpty(path)) {
          throw new PathIOException(path, STREAM_IS_CLOSED);
        } else {
          throw new IOException(STREAM_IS_CLOSED);
        }
      }
    }

    /**
     * Is the stream closed?
     * @return true if the stream is closed.
     */
    public boolean isClosed() {
      return closed.get();
    }

  }


  /**
   * Stream has three states: open, error, close,
   */
  public static class StreamState {

    public enum State {Open, Error, Close}

    /**
     * Path; if not empty then a {@link PathIOException} will be raised
     * containing this path.
     */
    private final String path;

    /** Lock. Not considering an InstrumentedWriteLock, but it is an option. */
    private final Lock lock = new ReentrantLock();

    /** Initial state: open. */
    private State state = State.Open;

    /** Any exception to raise on the next checkOpen call. */
    private IOException exception;

    public StreamState(final Path path) {
      this.path = path.toString();
    }

    public StreamState(final String path) {
      this.path = path;
    }

    /**
     * Get the current state.
     * Not synchronized; lock if you want consistency across calls.
     * @return the current state.
     */
    public State getState() {
      return state;
    }

    // Change state to close
    // @return - true iff state transitions from open or error to close
    public synchronized boolean enterClosedState() {
      if (state == State.Open) {
        state = State.Close;
        return true;
      } else {
        return false;
      }

    }

    /**
     * Change state to error and stores first error so it can be re-thrown.
     * If already in error: return previous exception.
     * @param ex the exception to record
     * @return the exception set when the error state was entered.
     */
    public synchronized IOException enterErrorState(final IOException ex) {
      Preconditions.checkArgument(ex != null, "Null exception");
      switch (state) {
        // a stream can go into the error state when open or closed
      case Open:
      case Close:
        exception = ex;
        state = State.Error;
        break;
      case Error:
        // already in this state; retain the previous exception.
        break;
      }
      return exception;
    }

    /**
     * Check a stream is open.
     * If in an error state: rethrow that exception. If closed,
     * throw an exception about that.
     * @throws IOException if the stream is not open.
     */
    public synchronized void checkOpen() throws IOException {
      switch (state) {
      case Open:
        return;

      case Error:
        throw exception;

      case Close:
        if (StringUtils.isNotEmpty(path)) {
          throw new PathIOException(path, STREAM_IS_CLOSED);
        } else {
          throw new IOException(STREAM_IS_CLOSED);
        }
      }
    }

    /**
     * Acquire an exclusive lock.
     * @param checkOpen must the stream be open?
     * @throws IOException if the stream is in error state or checkOpen==true
     * and the stream is closed.
     */
    public void acquireLock(boolean checkOpen) throws IOException {
      // fail fast if the stream is required to be open and it is not
      if (checkOpen) {
        checkOpen();
      }

      // acquire the lock; this may suspend the thread
      lock.lock();

      // now verify that the stream is still open.
      if (checkOpen) {
        checkOpen();
      }
    }

    /**
     * Release the lock.
     */
    public void releaseLock() {
      lock.unlock();
    }

    /**
     * Check for a stream being in a specific state.
     * The check is synchronized, but not locked; if the caller does
     * not hold a lock then the state may change before any subsequent
     * operation.
     * @param expected expected state
     * @return return true iff the steam was in the state at the time
     * of checking.
     */
    public synchronized boolean isInState(State expected) {
      return state.equals(expected);
    }

  }

}
