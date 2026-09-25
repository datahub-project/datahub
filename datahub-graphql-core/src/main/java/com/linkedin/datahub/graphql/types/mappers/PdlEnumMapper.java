package com.linkedin.datahub.graphql.types.mappers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Safe mapping from PDL enum values to GraphQL-generated Java enums.
 *
 * <p>PDL enums use {@code $UNKNOWN} as a sentinel for forwards compatibility: when an older schema
 * reads an enum value added in a newer version, Pegasus deserializes it as {@code $UNKNOWN}. The
 * GraphQL-generated Java enums do not include {@code $UNKNOWN}, so a raw {@link Enum#valueOf} call
 * will throw {@link IllegalArgumentException}. Always use this mapper instead.
 */
@Slf4j
public final class PdlEnumMapper {

  private PdlEnumMapper() {}

  /**
   * Maps a PDL enum value to the corresponding GraphQL enum constant, falling back to {@code
   * defaultValue} for any unrecognized value (including {@code $UNKNOWN}).
   */
  @Nonnull
  public static <T extends Enum<T>> T map(
      @Nonnull Class<T> graphqlEnum, @Nonnull Enum<?> pdlValue, @Nonnull T defaultValue) {
    try {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    } catch (IllegalArgumentException e) {
      log.warn(
          "Unmapped PDL enum value '{}' for {}; falling back to {}",
          pdlValue,
          graphqlEnum.getSimpleName(),
          defaultValue);
      return defaultValue;
    }
  }

  /** Maps a PDL enum value to the corresponding GraphQL enum constant, falling back to null. */
  @Nullable
  public static <T extends Enum<T>> T mapDefaultNull(
      @Nonnull Class<T> graphqlEnum, @Nonnull Enum<?> pdlValue) {
    try {
      return Enum.valueOf(graphqlEnum, pdlValue.name());
    } catch (IllegalArgumentException e) {
      log.warn(
          "Unmapped PDL enum value '{}' for {}; falling back to null",
          pdlValue,
          graphqlEnum.getSimpleName());
      return null;
    }
  }
}
