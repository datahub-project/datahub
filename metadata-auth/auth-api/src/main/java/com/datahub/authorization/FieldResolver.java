package com.datahub.authorization;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Helper class for lazy resolution of fields Input resolveField function that is given as input
 * will only be called when getFieldValuesFuture is called
 */
@RequiredArgsConstructor
public class FieldResolver {
  private final Supplier<CompletableFuture<FieldValue>> resolveField;

  @Getter(lazy = true)
  private final CompletableFuture<FieldValue> fieldValuesFuture = resolveField.get();

  private static final FieldValue EMPTY = new FieldValue(Collections.emptySet());

  /** Helper function that returns FieldResolver for precomputed values */
  public static FieldResolver getResolverFromValues(Set<String> values) {
    return new FieldResolver(
        () -> CompletableFuture.completedFuture(FieldValue.builder().values(values).build()));
  }

  /** Helper function that returns FieldResolver given a fetchFieldValue function */
  public static FieldResolver getResolverFromFunction(
      EntitySpec entitySpec, Function<EntitySpec, FieldValue> fetchFieldValue) {
    return new FieldResolver(
        () -> CompletableFuture.supplyAsync(() -> fetchFieldValue.apply(entitySpec)));
  }

  public static FieldValue emptyFieldValue() {
    return EMPTY;
  }

  /**
   * Container for storing the field value, in case we need to extend this to have more types of
   * field values
   */
  @Value
  @Builder
  public static class FieldValue {
    Set<String> values;
  }
}
