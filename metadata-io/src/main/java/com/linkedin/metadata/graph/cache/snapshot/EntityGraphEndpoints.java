package com.linkedin.metadata.graph.cache.snapshot;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses optional relationship endpoints for entity graph snapshots. Missing, JSON-null, blank, or
 * unparsable values are absence — not vertices.
 */
@Slf4j
public final class EntityGraphEndpoints {

  private EntityGraphEndpoints() {}

  /**
   * Returns a canonical URN string when {@code raw} parses as a URN; {@code null} when the value is
   * missing, JSON-null, blank, or unparsable.
   */
  @Nullable
  public static String parse(@Nullable String raw) {
    Urn urn = toUrn(raw);
    return urn == null ? null : urn.toString();
  }

  /**
   * Returns a {@link Urn} when {@code raw} parses; {@code null} when the value is missing,
   * JSON-null, blank, or unparsable. Does not throw.
   */
  @Nullable
  public static Urn toUrn(@Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String stripped = stripQuotes(raw.trim());
    if (stripped.isEmpty() || "null".equalsIgnoreCase(stripped)) {
      return null;
    }
    try {
      return Urn.createFromString(stripped);
    } catch (URISyntaxException e) {
      log.debug("Skipping unparsable entity graph endpoint {}", stripped, e);
      return null;
    }
  }

  public static boolean isValidEdge(@Nullable String source, @Nullable String dest) {
    return parse(source) != null && parse(dest) != null;
  }

  @Nonnull
  public static List<Urn> toUrnList(@Nullable Collection<String> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    List<Urn> urns = new ArrayList<>();
    for (String value : values) {
      Urn urn = toUrn(value);
      if (urn != null) {
        urns.add(urn);
      }
    }
    return urns;
  }

  @Nonnull
  public static Set<Urn> toUrnSet(@Nullable Collection<String> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptySet();
    }
    LinkedHashSet<Urn> urns = new LinkedHashSet<>();
    for (String value : values) {
      Urn urn = toUrn(value);
      if (urn != null) {
        urns.add(urn);
      }
    }
    return urns;
  }

  static String stripQuotes(@Nonnull String value) {
    String trimmed = value.trim();
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed;
  }
}
