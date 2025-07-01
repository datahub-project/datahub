package com.linkedin.datahub.graphql.concurrency;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
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

  public static void setExecutorService(ExecutorService executorService) {
    GraphQLConcurrencyUtils.graphQLExecutorService = Context.taskWrapping(executorService);
  }

  public static <T> CompletableFuture<T> supplyAsync(
      Supplier<T> supplier, String caller, String task) {
    MetricUtils.counter(
            MetricRegistry.name(
                GraphQLConcurrencyUtils.class.getSimpleName(), "supplyAsync", caller, task))
        .inc();
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
