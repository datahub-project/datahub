package com.linkedin.datahub.graphql.resolvers.auth;

import com.datahub.authentication.AccessTokenConfiguration;
import com.datahub.authentication.token.IsoDurationParser;
import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves create-token duration inputs against {@link AccessTokenConfiguration}: XOR of enum vs
 * ISO-8601, millisecond allowlist membership, and never-expire gating.
 */
public final class AccessTokenDurationPolicy {

  private AccessTokenDurationPolicy() {}

  /**
   * Resolve expires-in milliseconds from create/get token input fields.
   *
   * @return empty Optional means never-expire (only when allowed)
   */
  @Nonnull
  public static Optional<Long> resolveExpiresInMs(
      @Nonnull final AccessTokenConfiguration policy,
      @Nullable final AccessTokenDuration duration,
      @Nullable final String durationIso) {
    final boolean hasEnum = duration != null;
    final boolean hasIso = durationIso != null;

    if (hasEnum == hasIso) {
      throw new IllegalArgumentException(
          "Exactly one of duration or durationIso must be provided when creating an access token");
    }

    if (hasIso) {
      final long millis = IsoDurationParser.parseToMillis(durationIso);
      if (!policy.isDurationMillisAllowed(millis)) {
        throw new IllegalArgumentException(
            String.format(
                "Access token duration '%s' is not allowed. Choose a value from"
                    + " ACCESS_TOKEN_ALLOWED_DURATIONS (currently: %s).",
                durationIso, policy.getAllowedDurations()));
      }
      return Optional.of(millis);
    }

    if (duration == AccessTokenDuration.NO_EXPIRY) {
      if (!policy.isAllowNoExpiry()) {
        throw new IllegalArgumentException(
            String.format(
                "Never-expiring access tokens (NO_EXPIRY) cannot be created because"
                    + " ACCESS_TOKEN_ALLOW_NO_EXPIRY is false. Use a finite duration from"
                    + " ACCESS_TOKEN_ALLOWED_DURATIONS (currently: %s), or set"
                    + " ACCESS_TOKEN_ALLOW_NO_EXPIRY=true to allow NO_EXPIRY. Existing"
                    + " never-expire tokens continue to work.",
                policy.getAllowedDurations()));
      }
      return Optional.empty();
    }

    final Optional<Long> expiresInMs = AccessTokenUtil.mapDurationToMs(duration);
    final long millis =
        expiresInMs.orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Access token duration %s could not be mapped to milliseconds", duration)));
    if (!policy.isDurationMillisAllowed(millis)) {
      throw new IllegalArgumentException(
          String.format(
              "Access token duration %s is not allowed. Choose a value from"
                  + " ACCESS_TOKEN_ALLOWED_DURATIONS (currently: %s).",
              duration, policy.getAllowedDurations()));
    }
    return Optional.of(millis);
  }
}
