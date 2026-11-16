package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
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
    return PdlEnumMapper.map(graphqlEnum, pdlValue, fallback);
  }

  /** The matching GraphQL value, or null for a value this version doesn't know. */
  @Nullable
  public static <T extends Enum<T>> T mapOrNull(
      @Nonnull final Class<T> graphqlEnum, @Nonnull final Enum<?> pdlValue) {
    if (!handling().toleratesUnknownEnum()) {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    }
    return PdlEnumMapper.mapDefaultNull(graphqlEnum, pdlValue);
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
    return PolicyInfoPolicyMapper.safeValueOf(graphqlEnum, value, fallback);
  }
}
