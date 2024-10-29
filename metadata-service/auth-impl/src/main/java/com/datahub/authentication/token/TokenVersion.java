package com.datahub.authentication.token;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Represents a type of JWT access token granted by the {@link StatelessTokenService}. */
public enum TokenVersion {
  /** The first version of the DataHub access token. */
  ONE(1),

  /**
   * The second version of the DataHub access token (latest). Used to represent tokens that are
   * stateful and are stored within DataHub.
   */
  TWO(2);

  public final int numericValue;

  TokenVersion(final int numericValue) {
    this.numericValue = numericValue;
  }

  /** Returns the numeric representation of the version */
  public int getNumericValue() {
    return this.numericValue;
  }

  /** Returns a {@link TokenVersion} provided a numeric token version. */
  public static TokenVersion fromNumericValue(int num) {
    Optional<TokenVersion> maybeVersion =
        Arrays.stream(TokenVersion.values())
            .filter(version -> num == version.getNumericValue())
            .findFirst();
    if (maybeVersion.isPresent()) {
      return maybeVersion.get();
    }
    throw new IllegalArgumentException(
        String.format("Failed to find DataHubAccessTokenVersion %s", num));
  }

  /** Returns a {@link TokenVersion} provided a stringified numeric token version. */
  public static TokenVersion fromNumericStringValue(String num) {
    Objects.requireNonNull(num);
    Optional<TokenVersion> maybeVersion =
        Arrays.stream(TokenVersion.values())
            .filter(version -> Integer.parseInt(num) == version.getNumericValue())
            .findFirst();
    if (maybeVersion.isPresent()) {
      return maybeVersion.get();
    }
    throw new IllegalArgumentException(
        String.format("Failed to find DataHubAccessTokenVersion %s", num));
  }
}
