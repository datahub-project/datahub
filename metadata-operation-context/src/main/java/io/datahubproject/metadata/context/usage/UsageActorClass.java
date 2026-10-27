package io.datahubproject.metadata.context.usage;

import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Who is acting on a request — used for usage aggregation and legacy JMX request counters. */
public enum UsageActorClass {
  REGULAR,
  SYSTEM,
  SUPPORT;

  @Nonnull
  public String dimensionValue() {
    return name().toLowerCase();
  }

  /** Label segment for legacy JMX {@code requestContext_*} counters. */
  @Nonnull
  public String toLegacyUserCategoryTag() {
    return switch (this) {
      case SYSTEM -> "system";
      case SUPPORT -> "support";
      case REGULAR -> "regular";
    };
  }

  /**
   * URN-only classification for legacy {@code requestContext_*} metrics. Does not inspect corp-user
   * flags (support users remain {@link #REGULAR} on this path).
   */
  @Nonnull
  public static UsageActorClass fromActorUrn(@Nullable String actorUrn) {
    if (Constants.SYSTEM_ACTOR.equals(actorUrn)) {
      return SYSTEM;
    }
    return REGULAR;
  }
}
