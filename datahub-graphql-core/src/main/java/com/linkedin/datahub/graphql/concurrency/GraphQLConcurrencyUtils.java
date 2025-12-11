/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.concurrency;

import io.opentelemetry.context.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class GraphQLConcurrencyUtils {
  private GraphQLConcurrencyUtils() {}

  private static ExecutorService graphQLExecutorService = null;

  public static ExecutorService getExecutorService() {
    return GraphQLConcurrencyUtils.graphQLExecutorService;
  }

  public static ExecutorService setExecutorService(ExecutorService executorService) {
    GraphQLConcurrencyUtils.graphQLExecutorService = Context.taskWrapping(executorService);
    return graphQLExecutorService;
  }

  public static <T> CompletableFuture<T> supplyAsync(
      Supplier<T> supplier, String caller, String task) {
    if (GraphQLConcurrencyUtils.graphQLExecutorService == null) {
      // Hack around to force context wrapping for base executor
      return CompletableFuture.supplyAsync(
          supplier, Context.taskWrapping(new CompletableFuture().defaultExecutor()));
    } else {
      return CompletableFuture.supplyAsync(
          supplier, GraphQLConcurrencyUtils.graphQLExecutorService);
    }
  }
}
