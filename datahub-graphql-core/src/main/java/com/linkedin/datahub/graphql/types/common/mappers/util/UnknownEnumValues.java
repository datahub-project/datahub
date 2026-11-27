package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.metadata.utils.UnknownDataGuard;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps stored enum values that may have been written by a newer version (read after a rollback) to
 * GraphQL enums. Mappers use this instead of {@link PdlEnumMapper} directly wherever a value only a
 * newer version knows must not fail the request, so that behavior has one switch.
 *
 * <p>When unknown enum values aren't tolerated, every method is a plain {@link Enum#valueOf} and
 * throws for a value this version doesn't know, as the mappers did before.
 */
public final class UnknownEnumValues {

  // During a rollback every read of an affected object hits these, so the fallback and null cases
  // log at most once a minute per GraphQL enum rather than once per value.
  private static final UnknownDataGuard UNKNOWN_VALUES =
      UnknownDataGuard.forSite(UnknownEnumValues.class, "enum value");

  private UnknownEnumValues() {}

  /** The matching GraphQL value, or {@code fallback} for a value this version doesn't know. */
  @Nonnull
  public static <T extends Enum<T>> T map(
      @Nonnull final Class<T> graphqlEnum,
      @Nonnull final Enum<?> pdlValue,
      @Nonnull final T fallback) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    }
    final T value = valueOrNull(graphqlEnum, pdlValue);
    if (value != null) {
      return value;
    }
    UNKNOWN_VALUES.invalid(
        graphqlEnum.getSimpleName(),
        graphqlEnum.getSimpleName() + " has no such value; shown as " + fallback,
        pdlValue);
    return fallback;
  }

  /** The matching GraphQL value, or null for a value this version doesn't know. */
  @Nullable
  public static <T extends Enum<T>> T mapOrNull(
      @Nonnull final Class<T> graphqlEnum, @Nonnull final Enum<?> pdlValue) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    }
    final T value = valueOrNull(graphqlEnum, pdlValue);
    if (value == null) {
      UNKNOWN_VALUES.invalid(
          graphqlEnum.getSimpleName(),
          graphqlEnum.getSimpleName() + " has no such value; shown as null",
          pdlValue);
    }
    return value;
  }

  /**
   * The matching GraphQL value. For a value this version doesn't know it throws {@link
   * UnrepresentableValueException}, so the caller can leave out the object that holds it; a guess
   * could be saved back and change what the object means.
   */
  @Nonnull
  public static <T extends Enum<T>> T require(
      @Nonnull final Class<T> graphqlEnum, @Nonnull final Enum<?> pdlValue) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    }
    // Not logged here: the caller that leaves the object out logs the skip.
    final T value = valueOrNull(graphqlEnum, pdlValue);
    if (value == null) {
      throw new UnrepresentableValueException(graphqlEnum, pdlValue);
    }
    return value;
  }

  @Nullable
  private static <T extends Enum<T>> T valueOrNull(
      @Nonnull final Class<T> graphqlEnum, @Nonnull final Enum<?> pdlValue) {
    try {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The result of {@code mapping}, or null when it hits a value only a newer version defines in a
   * required field ({@link #require}). For an object held in an optional field: the object is left
   * out, logged through {@code guard} under {@code what}, instead of failing its parent.
   *
   * <p>When unknown enum values aren't tolerated, {@link #require} throws a plain {@link
   * IllegalArgumentException}, which this doesn't catch, so the request fails as before.
   */
  @Nullable
  public static <T> T leaveOutIfUnrepresentable(
      @Nonnull final UnknownDataGuard guard,
      @Nonnull final String what,
      @Nonnull final Supplier<T> mapping) {
    try {
      return mapping.get();
    } catch (UnrepresentableValueException e) {
      guard.skippedBecause(Optional.empty(), what, e.getMessage(), what);
      return null;
    }
  }

  /**
   * Each value mapped, in order, leaving out the ones whose mapping hits a value only a newer
   * version defines in a required field, as {@link #leaveOutIfUnrepresentable} does. For list
   * elements that can't be null.
   */
  @Nonnull
  public static <S, T> List<T> mapRepresentable(
      @Nonnull final UnknownDataGuard guard,
      @Nonnull final String what,
      @Nonnull final Collection<S> values,
      @Nonnull final Function<S, T> mapping) {
    final List<T> result = new ArrayList<>(values.size());
    for (S value : values) {
      try {
        result.add(mapping.apply(value));
      } catch (UnrepresentableValueException e) {
        guard.skippedBecause(Optional.empty(), what, e.getMessage(), what);
      }
    }
    return result;
  }

  /** As {@link #mapOrNull}, for an enum stored as a free-form string. */
  @Nullable
  public static <T extends Enum<T>> T fromStringOrNull(
      @Nonnull final Class<T> graphqlEnum, @Nullable final String value) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, value);
    }
    if (value == null) {
      return null;
    }
    try {
      return Enum.valueOf(graphqlEnum, value);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** As {@link #map}, for an enum stored as a free-form string. */
  @Nonnull
  public static <T extends Enum<T>> T fromString(
      @Nonnull final Class<T> graphqlEnum,
      @Nullable final String value,
      @Nonnull final T fallback) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, value);
    }
    try {
      return Enum.valueOf(graphqlEnum, value);
    } catch (IllegalArgumentException | NullPointerException e) {
      UNKNOWN_VALUES.invalid(
          graphqlEnum.getSimpleName(),
          graphqlEnum.getSimpleName() + " has no such value; shown as " + fallback,
          value);
      return fallback;
    }
  }
}
