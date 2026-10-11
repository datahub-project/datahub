package com.datahub.authentication;

import com.datahub.authentication.token.IsoDurationParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** POJO for {@code authentication.accessTokens} in application.yaml. */
@Getter
@ToString
@EqualsAndHashCode
public class AccessTokenConfiguration {

  public static final String DEFAULT_ALLOWED_DURATIONS = "PT1H,P1D,P7D,P30D,P90D,P180D,P365D";

  private boolean allowNoExpiry = false;

  /** Configured ISO-8601 duration strings in display order. */
  private List<String> allowedDurations =
      Collections.unmodifiableList(parseDurationList(DEFAULT_ALLOWED_DURATIONS));

  /** Normalized millisecond lengths corresponding to {@link #allowedDurations}. */
  private Set<Long> allowedDurationMillis = toMillisSet(allowedDurations);

  public void setAllowNoExpiry(final boolean allowNoExpiry) {
    this.allowNoExpiry = allowNoExpiry;
  }

  /**
   * Spring binds the comma-separated env/YAML value as a single string. Validates and fails fast on
   * empty or unparsable input.
   */
  public void setAllowedDurations(final String allowedDurationsCsv) {
    final List<String> parsed = parseDurationList(allowedDurationsCsv);
    if (parsed.isEmpty()) {
      throw new IllegalArgumentException(
          "authentication.accessTokens.allowedDurations must not be empty");
    }
    this.allowedDurations = Collections.unmodifiableList(parsed);
    this.allowedDurationMillis = toMillisSet(parsed);
  }

  /** Whether the given TTL in milliseconds is permitted by the allowlist. */
  public boolean isDurationMillisAllowed(final long durationMillis) {
    return allowedDurationMillis.contains(durationMillis);
  }

  private static List<String> parseDurationList(final String csv) {
    if (csv == null || csv.trim().isEmpty()) {
      return Collections.emptyList();
    }
    final List<String> result = new ArrayList<>();
    for (final String part : csv.split(",", -1)) {
      final String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        throw new IllegalArgumentException(
            "authentication.accessTokens.allowedDurations contains an empty entry; "
                + "remove trailing commas and blank CSV items");
      }
      // Fail fast on invalid entries
      IsoDurationParser.parseToMillis(trimmed);
      result.add(trimmed);
    }
    return result;
  }

  private static Set<Long> toMillisSet(final List<String> durations) {
    final Set<Long> millis =
        durations.stream()
            .map(IsoDurationParser::parseToMillis)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return Collections.unmodifiableSet(millis);
  }

  /** Convenience for tests / defaults without going through Spring. */
  public static AccessTokenConfiguration defaults() {
    final AccessTokenConfiguration config = new AccessTokenConfiguration();
    config.setAllowedDurations(DEFAULT_ALLOWED_DURATIONS);
    config.setAllowNoExpiry(false);
    return config;
  }

  static {
    // Ensure DEFAULT_ALLOWED_DURATIONS stays valid
    Arrays.stream(DEFAULT_ALLOWED_DURATIONS.split(",")).forEach(IsoDurationParser::parseToMillis);
  }
}
