package com.linkedin.metadata.utils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ConcurrencyUtils {
  private ConcurrencyUtils() {
  }

  /**
   * Transforms original list into the final list using the function transformer in an asynchronous fashion
   * i.e. each element transform is run as a separate CompleteableFuture and then joined at the end
   */
  public static <O, T> List<T> transformAndCollectAsync(List<O> originalList, Function<O, T> transformer) {
    return originalList.stream()
        .map(element -> CompletableFuture.supplyAsync(() -> transformer.apply(element)))
        .collect(Collectors.collectingAndThen(Collectors.toList(),
            completableFutureList -> completableFutureList.stream().map(CompletableFuture::join)))
        .collect(Collectors.toList());
  }

  /**
   * Transforms original list into the final list using the function transformer in an asynchronous fashion
   * with exceptions handled by the input exceptionHandler
   * i.e. each element transform is run as a separate CompleteableFuture and then joined at the end
   */
  public static <O, T> List<T> transformAndCollectAsync(List<O> originalList, Function<O, T> transformer,
      BiFunction<O, Throwable, ? extends T> exceptionHandler) {
    return originalList.stream()
        .map(element -> CompletableFuture.supplyAsync(() -> transformer.apply(element))
            .exceptionally(e -> exceptionHandler.apply(element, e)))
        .filter(Objects::nonNull)
        .collect(Collectors.collectingAndThen(Collectors.toList(),
            completableFutureList -> completableFutureList.stream().map(CompletableFuture::join)))
        .collect(Collectors.toList());
  }
}
