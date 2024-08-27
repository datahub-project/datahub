package com.linkedin.metadata.utils;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConcurrencyUtils {
  private ConcurrencyUtils() {}

  public static <O, T> List<T> transformAndCollectAsync(
      List<O> originalList, Function<O, T> transformer) {
    return transformAndCollectAsync(originalList, transformer, Collectors.toList());
  }

  /**
   * Transforms original list into the final list using the function transformer in an asynchronous
   * fashion i.e. each element transform is run as a separate CompleteableFuture and then joined at
   * the end
   */
  public static <O, T, OUTPUT> OUTPUT transformAndCollectAsync(
      Collection<O> originalCollection,
      Function<O, T> transformer,
      Collector<T, ?, OUTPUT> collector) {
    return originalCollection.stream()
        .map(element -> CompletableFuture.supplyAsync(() -> transformer.apply(element)))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(),
                completableFutureList ->
                    completableFutureList.stream().map(CompletableFuture::join)))
        .collect(collector);
  }

  /**
   * Transforms original list into the final list using the function transformer in an asynchronous
   * fashion with exceptions handled by the input exceptionHandler i.e. each element transform is
   * run as a separate CompleteableFuture and then joined at the end
   */
  public static <O, T> List<T> transformAndCollectAsync(
      List<O> originalList,
      Function<O, T> transformer,
      BiFunction<O, Throwable, ? extends T> exceptionHandler) {
    return transformAndCollectAsync(
        originalList, transformer, exceptionHandler, Collectors.toList());
  }

  /**
   * Transforms original list into the final list using the function transformer in an asynchronous
   * fashion with exceptions handled by the input exceptionHandler i.e. each element transform is
   * run as a separate CompleteableFuture and then joined at the end
   */
  public static <O, T, OUTPUT> OUTPUT transformAndCollectAsync(
      Collection<O> originalCollection,
      Function<O, T> transformer,
      BiFunction<O, Throwable, ? extends T> exceptionHandler,
      Collector<T, ?, OUTPUT> collector) {
    return originalCollection.stream()
        .map(
            element ->
                CompletableFuture.supplyAsync(() -> transformer.apply(element))
                    .exceptionally(e -> exceptionHandler.apply(element, e)))
        .filter(Objects::nonNull)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(),
                completableFutureList ->
                    completableFutureList.stream().map(CompletableFuture::join)))
        .collect(collector);
  }

  /**
   * Wait for a list of futures to end with a timeout and only return results that were returned
   * before the timeout expired
   */
  public static <T> List<T> getAllCompleted(
      List<CompletableFuture<T>> futuresList, long timeout, TimeUnit unit) {
    CompletableFuture<Void> allFuturesResult =
        CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[0]));
    try {
      allFuturesResult.get(timeout, unit);
    } catch (Exception e) {
      log.info("Timed out while waiting for futures to complete");
    }

    return futuresList.stream()
        .filter(future -> future.isDone() && !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .collect(Collectors.<T>toList());
  }
}
