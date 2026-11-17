package com.linkedin.metadata.config.productupdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A numeric release version, e.g. {@code v1.7.0}. {@link #parse} accepts the dot/underscore
 * spellings DataHub uses for release ids and note file names ({@code v1.7.0}, {@code v_2_1_0}); the
 * hyphenated form used in docs anchors and blog slugs ({@code 1-7-0}) is matched only when
 * searching within text via {@link #appearsIn}.
 */
public final class ReleaseVersion implements Comparable<ReleaseVersion> {

  private static final Pattern SEPARATORS = Pattern.compile("[._]");
  private static final Pattern DIGITS = Pattern.compile("\\d+");
  private static final List<String> SEPARATOR_SPELLINGS = List.of(".", "-", "_");

  /** Matches a multi-component version anywhere in free text, e.g. "Explore version v1.7.0". */
  private static final Pattern VERSION_TOKEN = Pattern.compile("v?\\d+(?:[._]\\d+)+");

  private final List<Integer> components;

  private ReleaseVersion(@Nonnull List<Integer> components) {
    this.components = List.copyOf(components);
  }

  @Nonnull
  public static Optional<ReleaseVersion> parse(@Nullable String raw) {
    if (raw == null) {
      return Optional.empty();
    }
    String normalized = raw.trim();
    if (normalized.startsWith("v") || normalized.startsWith("V")) {
      normalized = normalized.substring(1);
    }
    normalized = normalized.replaceAll("^[._]+", "").replaceAll("[._]+$", "");
    if (normalized.isEmpty()) {
      return Optional.empty();
    }

    List<Integer> parsed = new ArrayList<>();
    for (String part : SEPARATORS.split(normalized)) {
      if (!DIGITS.matcher(part).matches()) {
        return Optional.empty();
      }
      try {
        parsed.add(Integer.parseInt(part));
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
    }
    return Optional.of(new ReleaseVersion(parsed));
  }

  /** Component at {@code index}, defaulting to 0 so 2.1 and 2.1.0 compare as equal. */
  public int component(int index) {
    return index < components.size() ? components.get(index) : 0;
  }

  /**
   * Whether both versions belong to the same MAJOR.MINOR series.
   *
   * <p>Product update toasts are written per minor release, and the two flavors declare different
   * levels of precision for the same release (core uses {@code v1.7.0}, cloud uses {@code v2.1} for
   * what the release notes call {@code v2.1.0}). Comparing on MAJOR.MINOR reconciles those, and
   * keeps patch releases and hotfix rollups — which don't get their own toast — from failing.
   */
  public boolean sameMinorSeries(@Nonnull ReleaseVersion other) {
    return component(0) == other.component(0) && component(1) == other.component(1);
  }

  /** Whether this version is referenced in {@code text} under any of its separator spellings. */
  public boolean appearsIn(@Nonnull String text) {
    for (String separator : SEPARATOR_SPELLINGS) {
      // Digit boundaries keep v2.1 from matching the v2.10 in ".../datahub-cloud-2-10".
      Pattern reference =
          Pattern.compile("(?<!\\d)" + Pattern.quote(joinWith(separator)) + "(?!\\d)");
      if (reference.matcher(text).find()) {
        return true;
      }
    }
    return false;
  }

  /** The highest version referenced in {@code text}, if any. */
  @Nonnull
  public static Optional<ReleaseVersion> highestIn(@Nonnull String text) {
    Matcher matcher = VERSION_TOKEN.matcher(text);
    Optional<ReleaseVersion> highest = Optional.empty();
    while (matcher.find()) {
      Optional<ReleaseVersion> candidate = parse(matcher.group());
      if (candidate.isPresent()
          && (highest.isEmpty() || candidate.get().compareTo(highest.get()) > 0)) {
        highest = candidate;
      }
    }
    return highest;
  }

  @Nonnull
  private String joinWith(@Nonnull String separator) {
    return components.stream().map(String::valueOf).collect(Collectors.joining(separator));
  }

  @Override
  public int compareTo(@Nonnull ReleaseVersion other) {
    int length = Math.max(components.size(), other.components.size());
    for (int i = 0; i < length; i++) {
      int comparison = Integer.compare(component(i), other.component(i));
      if (comparison != 0) {
        return comparison;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ReleaseVersion && compareTo((ReleaseVersion) other) == 0;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for (int i = 0; i < components.size(); i++) {
      // Trailing zeros are insignificant (2.1 equals 2.1.0), so they must not affect the hash.
      if (component(i) != 0) {
        hash = 31 * hash + Integer.hashCode(component(i)) + i;
      }
    }
    return hash;
  }

  @Override
  public String toString() {
    return "v" + joinWith(".");
  }
}
