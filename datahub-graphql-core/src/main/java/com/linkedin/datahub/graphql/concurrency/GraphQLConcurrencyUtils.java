package com.linkedin.datahub.graphql.concurrency;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.context.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

  /**
   * Takes in a list of futures and returns a composed future that is dependent on all futures in
   * the list being done. This allows further composition on the final resulting list of all
   * completed futures.
   *
   * @param futures the list of futures to be composed
   * @return a composed CompleteableFuture that returns the list of all final results
   * @param <T> type of results in the list
   */
  public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
    CompletableFuture<Void> allDone =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    return allDone.thenApply(
        v -> futures.stream().map(future -> future.getNow(null)).collect(Collectors.toList()));
  }
}
