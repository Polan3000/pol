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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.async.ApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncCatchFunction;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import  org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor.CONCURRENT;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApplyUseExecutor;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncCatch;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncCurrent;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncFinally;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncForEach;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncThrowException;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncTry;

public class RouterAsyncRpcClient extends RouterRpcClient{
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncRpcClient.class);
  /** Router using this RPC client. */
  private final Router router;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;
  /** Optional perf monitor. */
  private final RouterRpcMonitor rpcMonitor;

  /**
   * Create a router async RPC client to manage remote procedure calls to NNs.
   *
   * @param conf Hdfs Configuration.
   * @param router A router using this RPC client.
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   * @param routerStateIdContext the router state context object to hold the state ids for all
   * namespaces.
   */
  public RouterAsyncRpcClient(
      Configuration conf, Router router, ActiveNamenodeResolver resolver,
      RouterRpcMonitor monitor, RouterStateIdContext routerStateIdContext) {
    super(conf, router, resolver, monitor, routerStateIdContext);
    this.router = router;
    this.namenodeResolver = resolver;
    this.rpcMonitor = monitor;
  }

  @Override
  public <T extends RemoteLocationContext> boolean invokeAll(
      final Collection<T> locations, final RemoteMethod method)
      throws IOException {
    invokeConcurrent(locations, method, false, false,
        Boolean.class);
    asyncApply((ApplyFunction<Map<T, Boolean>, Object>)
        results -> results.containsValue(true));
    return asyncReturn(boolean.class);
  }

  @Override
  public Object invokeMethod(
      UserGroupInformation ugi,
      List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver, Class<?> protocol,
      Method method, Object... params) throws IOException {
    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }
    // transfer threadLocalContext to worker threads of executor.
    ThreadLocalContext threadLocalContext = new ThreadLocalContext();
    asyncComplete(null);
    asyncApplyUseExecutor((AsyncApplyFunction<Object, Object>) o -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Async invoke method : {}, {}, {}, {}", method.getName(), useObserver,
            namenodes.toString(), params);
      }
      threadLocalContext.transfer();
      invokeMethodAsync(ugi, (List<FederationNamenodeContext>) namenodes,
          useObserver, protocol, method, params);
    }, RouterRpcServer.getAsyncRouterHandler());
    return null;
  }

  private void invokeMethodAsync(
      final UserGroupInformation ugi,
      final List<FederationNamenodeContext> namenodes,
      boolean useObserver,
      final Class<?> protocol, final Method method, final Object... params) {

    addClientInfoToCallerContext(ugi);
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    final ExecutionStatus status = new ExecutionStatus(false, useObserver);
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();
    asyncForEach(namenodes.iterator(),
        (foreach, namenode) -> {
          if (!status.isShouldUseObserver()
              && (namenode.getState() == FederationNamenodeServiceState.OBSERVER)) {
            asyncComplete(null);
            return;
          }
          String nsId = namenode.getNameserviceId();
          String rpcAddress = namenode.getRpcAddress();
          try {
            ConnectionContext connection = getConnection(ugi, nsId, rpcAddress, protocol);
            NameNodeProxiesClient.ProxyAndInfo<?> client = connection.getClient();
            invokeAsync(nsId, namenode, status.isShouldUseObserver(), 0, method,
                client.getProxy(), params);
            asyncApply(res -> {
              status.setComplete(true);
              if (status.isFailOver() &&
                  FederationNamenodeServiceState.OBSERVER != namenode.getState()) {
                // Success on alternate server, update
                InetSocketAddress address = client.getAddress();
                namenodeResolver.updateActiveNamenode(nsId, address);
              }
              if (rpcMonitor != null) {
                rpcMonitor.proxyOpComplete(true, nsId, namenode.getState());
              }
              if (router.getRouterClientMetrics() != null) {
                router.getRouterClientMetrics().incInvokedMethod(method);
              }
              foreach.breakNow();
              return res;
            });
            asyncCatch((res, ioe) -> {
              ioes.put(namenode, ioe);
              if (ioe instanceof ObserverRetryOnActiveException) {
                LOG.info("Encountered ObserverRetryOnActiveException from {}."
                    + " Retry active namenode directly.", namenode);
                status.setShouldUseObserver(false);
              } else if (ioe instanceof StandbyException) {
                // Fail over indicated by retry policy and/or NN
                if (rpcMonitor != null) {
                  rpcMonitor.proxyOpFailureStandby(nsId);
                }
                status.setShouldUseObserver(true);
              } else if (isUnavailableException(ioe)) {
                if (rpcMonitor != null) {
                  rpcMonitor.proxyOpFailureCommunicate(nsId);
                }
                if (FederationNamenodeServiceState.OBSERVER == namenode.getState()) {
                  namenodeResolver.updateUnavailableNamenode(nsId,
                      NetUtils.createSocketAddr(namenode.getRpcAddress()));
                } else {
                  status.setShouldUseObserver(true);
                }
              } else if (ioe instanceof RemoteException) {
                if (this.rpcMonitor != null) {
                  rpcMonitor.proxyOpComplete(true, nsId,
                      namenode.getState());
                }
                RemoteException re = (RemoteException) ioe;
                ioe = re.unwrapRemoteException();
                ioe = getCleanException(ioe);
                // RemoteException returned by NN
                throw ioe;
              } else if (ioe instanceof NoNamenodesAvailableException) {
                IOException cause = (IOException) ioe.getCause();
                if (rpcMonitor != null) {
                  rpcMonitor.proxyOpNoNamenodes(nsId);
                }
                LOG.error("Cannot get available namenode for {} {} error: {}",
                    nsId, rpcAddress, ioe.getMessage());
                // Rotate cache so that client can retry the next namenode in the cache
                if (shouldRotateCache(cause)) {
                  this.namenodeResolver.rotateCache(nsId, namenode, useObserver);
                }
                // Throw RetriableException so that client can retry
                throw new RetriableException(ioe);
              } else {
                // Other communication error, this is a failure
                // Communication retries are handled by the retry policy
                if (rpcMonitor != null) {
                  rpcMonitor.proxyOpComplete(false, nsId,
                      namenode.getState());
                }
                throw ioe;
              }
              return res;
            }, IOException.class);
            asyncFinally(res -> {
              connection.release();
              return res;
            });
          } catch (ConnectionNullException ioe) {
            if (rpcMonitor != null) {
              rpcMonitor.proxyOpFailureCommunicate(nsId);
            }
            LOG.error("Get connection for {} {} error: {}", nsId, rpcAddress,
                ioe.getMessage());
            // Throw StandbyException so that client can retry
            StandbyException se = new StandbyException(ioe.getMessage());
            se.initCause(ioe);
            throw se;
          }
        });

    asyncApply(res -> {
      if (status.isComplete()) {
        return res;
      }
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpComplete(false, null, null);
      }
      // All namenodes were unavailable or in standby
      String msg = "No namenode available to invoke " + method.getName() + " " +
          Arrays.deepToString(params) + " in " + namenodes + " from " +
          router.getRouterId();
      LOG.error(msg);
      int exConnect = 0;
      for (Map.Entry<FederationNamenodeContext, IOException> entry :
          ioes.entrySet()) {
        FederationNamenodeContext namenode = entry.getKey();
        String nnKey = namenode.getNamenodeKey();
        String addr = namenode.getRpcAddress();
        IOException ioe = entry.getValue();
        if (ioe instanceof StandbyException) {
          LOG.error("{} at {} is in Standby: {}",
              nnKey, addr, ioe.getMessage());
        } else if (isUnavailableException(ioe)) {
          exConnect++;
          LOG.error("{} at {} cannot be reached: {}",
              nnKey, addr, ioe.getMessage());
        } else {
          LOG.error("{} at {} error: \"{}\"", nnKey, addr, ioe.getMessage());
        }
      }
      if (exConnect == ioes.size()) {
        throw new ConnectException(msg);
      } else {
        throw new StandbyException(msg);
      }
    });
  }

  private void invokeAsync(
      String nsId, FederationNamenodeContext namenode,
      Boolean listObserverFirst,
      int retryCount, final Method method,
      final Object obj, final Object... params) {
    try {
      Client.setAsynchronousMode(true);
      method.invoke(obj, params);
      Client.setAsynchronousMode(false);
      asyncCatch((AsyncCatchFunction<Object, Throwable>) (o, e) -> {
        if (e instanceof IOException) {
          IOException ioe = (IOException) e;

          // Check if we should retry.
          RetryDecision decision = shouldRetry(ioe, retryCount, nsId,
              namenode, listObserverFirst);
          if (decision == RetryDecision.RETRY) {
            if (rpcMonitor != null) {
              rpcMonitor.proxyOpRetries();
            }

            // retry
            invokeAsync(nsId, namenode, listObserverFirst, retryCount + 1,
                method, obj, params);
          } else if (decision == RetryDecision.FAILOVER_AND_RETRY) {
            // failover, invoker looks for standby exceptions for failover.
            if (ioe instanceof StandbyException) {
              throw ioe;
            } else if (isUnavailableException(ioe)) {
              throw ioe;
            } else {
              throw new StandbyException(ioe.getMessage());
            }
          } else {
            throw ioe;
          }
        } else {
          throw new IOException(e);
        }
      }, Throwable.class);
    } catch (InvocationTargetException e) {
      asyncThrowException(e.getCause());
    } catch (IllegalAccessException | IllegalArgumentException e) {
      LOG.error("Unexpected exception while proxying API", e);
      asyncThrowException(e);
    }
  }

  @Override
  public <T> T invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<T> expectedResultClass,
      Object expectedResultValue) throws IOException {
    invokeSequential(remoteMethod, locations, expectedResultClass, expectedResultValue);
    asyncApply((ApplyFunction<RemoteResult, Object>) result -> result.getResult());
    return asyncReturn(expectedResultClass);
  }

  @Override
  public <R extends RemoteLocationContext, T> RemoteResult invokeSequential(
      final RemoteMethod remoteMethod, final List<R> locations,
      Class<T> expectedResultClass, Object expectedResultValue)
      throws IOException {

    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    List<IOException> thrownExceptions = new ArrayList<>();
    final Object[] firstResult = {null};
    final ExecutionStatus status = new ExecutionStatus();
    Iterator<RemoteLocationContext> locationIterator =
        (Iterator<RemoteLocationContext>) locations.iterator();
    // Invoke in priority order
    asyncForEach(locationIterator,
        (foreach, loc) -> {
          String ns = loc.getNameserviceId();
          boolean isObserverRead = isObserverReadEligible(ns, m);
          List<? extends FederationNamenodeContext> namenodes =
              getOrderedNamenodes(ns, isObserverRead);
          acquirePermit(ns, ugi, remoteMethod, controller);
          asyncTry(() -> {
            Class<?> proto = remoteMethod.getProtocol();
            Object[] params = remoteMethod.getParams(loc);
            invokeMethod(ugi, namenodes, isObserverRead, proto, m, params);
            asyncApply(result -> {
              // Check if the result is what we expected
              if (isExpectedClass(expectedResultClass, result) &&
                  isExpectedValue(expectedResultValue, result)) {
                // Valid result, stop here
                @SuppressWarnings("unchecked") R location = (R) loc;
                @SuppressWarnings("unchecked") T ret = (T) result;
                foreach.breakNow();
                status.setComplete(true);
                return new RemoteResult<>(location, ret);
              }
              if (firstResult[0] == null) {
                firstResult[0] = result;
              }
              return null;
            });
          });
          asyncCatch((ret, e) -> {
            if (e instanceof IOException) {
              IOException ioe = (IOException) e;
              // Localize the exception
              ioe = processException(ioe, loc);
              // Record it and move on
              thrownExceptions.add(ioe);
            } else {
              // Unusual error, ClientProtocol calls always use IOException (or
              // RemoteException). Re-wrap in IOException for compatibility with
              // ClientProtocol.
              LOG.error("Unexpected exception {} proxying {} to {}",
                  e.getClass(), m.getName(), ns, e);
              IOException ioe = new IOException(
                  "Unexpected exception proxying API " + e.getMessage(), e);
              thrownExceptions.add(ioe);
            }
            return ret;
          }, Exception.class);
          asyncFinally(ret -> {
            releasePermit(ns, ugi, remoteMethod, controller);
            return ret;
          });
        });
    asyncApply(result -> {
      if (status.isComplete()) {
        return result;
      }
      if (!thrownExceptions.isEmpty()) {
        // An unavailable subcluster may be the actual cause
        // We cannot surface other exceptions (e.g., FileNotFoundException)
        for (int i = 0; i < thrownExceptions.size(); i++) {
          IOException ioe = thrownExceptions.get(i);
          if (isUnavailableException(ioe)) {
            throw ioe;
          }
        }
        // re-throw the first exception thrown for compatibility
        throw thrownExceptions.get(0);
      }
      // Return the first result, whether it is the value or not
      @SuppressWarnings("unchecked") T ret = (T) firstResult[0];
      return new RemoteResult<>(locations.get(0), ret);
    });
    return asyncReturn(RemoteResult.class);
  }

  @Override
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, long timeOutMs, Class<R> clazz)
      throws IOException {
    invokeConcurrent(locations, method, standby, timeOutMs, clazz);
    asyncApply((ApplyFunction<List<RemoteResult<T, R>>, Object>) results -> {
      // Go over the results and exceptions
      final Map<T, R> ret = new TreeMap<>();
      final List<IOException> thrownExceptions = new ArrayList<>();
      IOException firstUnavailableException = null;
      for (final RemoteResult<T, R> result : results) {
        if (result.hasException()) {
          IOException ioe = result.getException();
          thrownExceptions.add(ioe);
          // Track unavailable exceptions to throw them first
          if (isUnavailableException(ioe)) {
            firstUnavailableException = ioe;
          }
        }
        if (result.hasResult()) {
          ret.put(result.getLocation(), result.getResult());
        }
      }
      // Throw exceptions if needed
      if (!thrownExceptions.isEmpty()) {
        // Throw if response from all servers required or no results
        if (requireResponse || ret.isEmpty()) {
          // Throw unavailable exceptions first
          if (firstUnavailableException != null) {
            throw firstUnavailableException;
          } else {
            throw thrownExceptions.get(0);
          }
        }
      }
      return ret;
    });
    return asyncReturn(Map.class);
  }

  @Override
  public <T extends RemoteLocationContext, R> List<RemoteResult<T, R>>
      invokeConcurrent(final Collection<T> locations,
                   final RemoteMethod method, boolean standby, long timeOutMs,
                   Class<R> clazz) throws IOException {
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();

    if (locations.isEmpty()) {
      throw new IOException("No remote locations available");
    } else if (locations.size() == 1 && timeOutMs <= 0) {
      // Shortcut, just one call
      T location = locations.iterator().next();
      String ns = location.getNameserviceId();
      boolean isObserverRead = isObserverReadEligible(ns, m);
      final List<? extends FederationNamenodeContext> namenodes =
          getOrderedNamenodes(ns, isObserverRead);
      RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
      acquirePermit(ns, ugi, method, controller);
      asyncTry(() -> {
        Class<?> proto = method.getProtocol();
        Object[] paramList = method.getParams(location);
        invokeMethod(ugi, namenodes, isObserverRead, proto, m, paramList);
        asyncApply((ApplyFunction<R, Object>) result -> {
          RemoteResult<T, R> remoteResult = new RemoteResult<>(location, result);
          return Collections.singletonList(remoteResult);
        });
      });
      asyncCatch((o, ioe) -> {
        throw processException(ioe, location);
      }, IOException.class);
      asyncFinally(o -> {
        releasePermit(ns, ugi, method, controller);
        return o;
      });
      return asyncReturn(List.class);
    }

    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    if (this.router.getRouterClientMetrics() != null) {
      this.router.getRouterClientMetrics().incInvokedConcurrent(m);
    }
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(CONCURRENT_NS, ugi, method, controller);
    List<T> orderedLocations = new ArrayList<>();
    asyncCurrent(locations,
        location -> {
          String nsId = location.getNameserviceId();
          boolean isObserverRead = isObserverReadEligible(nsId, m);
          final List<? extends FederationNamenodeContext> namenodes =
              getOrderedNamenodes(nsId, isObserverRead);
          final Class<?> proto = method.getProtocol();
          final Object[] paramList = method.getParams(location);
          if (standby) {
            // Call the objectGetter to all NNs (including standby)
            for (final FederationNamenodeContext nn : namenodes) {
              final List<FederationNamenodeContext> nnList =
                  Collections.singletonList(nn);
              String nnId = nn.getNamenodeId();
              T nnLocation = location;
              if (location instanceof RemoteLocation) {
                nnLocation = (T) new RemoteLocation(nsId, nnId, location.getDest());
              }
              orderedLocations.add(nnLocation);
              invokeMethod(ugi, nnList, isObserverRead, proto, m, paramList);
            }
          } else {
            // Call the objectGetter in order of nameservices in the NS list
            orderedLocations.add(location);
            invokeMethod(ugi, namenodes, isObserverRead, proto, m, paramList);
          }
        },
        futures -> {
          List<RemoteResult<T, R>> results = new ArrayList<>();
          for (int i = 0; i < futures.length; i++) {
            T location = orderedLocations.get(i);
            Future<Object> resultFuture = futures[i];
            Object result = null;
            try {
              result = resultFuture.get();
              results.add((RemoteResult<T, R>) new RemoteResult<>(location, result));
            } catch (InterruptedException ignored) {
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              IOException ioe = null;
              if (cause instanceof CancellationException) {
                T loc = orderedLocations.get(i);
                String msg = "Invocation to \"" + loc + "\" for \""
                    + method.getMethodName() + "\" timed out";
                LOG.error(msg);
                ioe = new SubClusterTimeoutException(msg);
              } else if (cause instanceof IOException) {
                LOG.debug("Cannot execute {} in {}: {}",
                    m.getName(), location, cause.getMessage());
                ioe = (IOException) cause;
              } else {
                ioe = new IOException("Unhandled exception while proxying API " +
                    m.getName() + ": " + cause.getMessage(), cause);
              }
              // Store the exceptions
              results.add(new RemoteResult<>(location, ioe));
            }
          }
          if (rpcMonitor != null) {
            rpcMonitor.proxyOpComplete(true, CONCURRENT, null);
          }
          releasePermit(CONCURRENT_NS, ugi, method, controller);
          return results;
        });
    return asyncReturn(List.class);
  }

  @Override
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(nsId, ugi, method, controller);
    asyncTry(() -> {
      boolean isObserverRead = isObserverReadEligible(nsId, method.getMethod());
      List<? extends FederationNamenodeContext> nns = getOrderedNamenodes(nsId, isObserverRead);
      RemoteLocationContext loc = new RemoteLocation(nsId, "/", "/");
      Class<?> proto = method.getProtocol();
      Method m = method.getMethod();
      Object[] params = method.getParams(loc);
      invokeMethod(ugi, nns, isObserverRead, proto, m, params);
    });
    asyncFinally(o -> {
      releasePermit(nsId, ugi, method, controller);
      return o;
    });
    return null;
  }

  static class ExecutionStatus {
    private byte flags;
    private static final byte FAIL_OVER_BIT = 1;
    private static final byte SHOULD_USE_OBSERVER_BIT = 2;
    private static final byte COMPLETE_BIT = 4;

    ExecutionStatus() {
      this(false, false);
    }

    ExecutionStatus(boolean failOver, boolean shouldUseObserver) {
      this.flags = 0;
      setFailOver(failOver);
      setShouldUseObserver(shouldUseObserver);
      setComplete(false);
    }

    private void setFailOver(boolean failOver) {
      flags = (byte) (failOver ? (flags | FAIL_OVER_BIT) : (flags & ~FAIL_OVER_BIT));
    }

    private void setShouldUseObserver(boolean shouldUseObserver) {
      flags = (byte) (shouldUseObserver ?
          (flags | SHOULD_USE_OBSERVER_BIT) : (flags & ~SHOULD_USE_OBSERVER_BIT));
    }

    private void setComplete(boolean complete) {
      flags = (byte) (complete ? (flags | COMPLETE_BIT) : (flags & ~COMPLETE_BIT));
    }

    public boolean isFailOver() {
      return (flags & FAIL_OVER_BIT) != 0;
    }

    public boolean isShouldUseObserver() {
      return (flags &  SHOULD_USE_OBSERVER_BIT) != 0;
    }

    public boolean isComplete() {
      return (flags & COMPLETE_BIT) != 0;
    }
  }
}
