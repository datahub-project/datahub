package com.linkedin.metadata.utils.elasticsearch;

import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entity search index generation, identified by the {@code index_vN} token in an index name.
 *
 * <p>{@link #matches} accepts the alias/convention name ({@code datasetindex_v3}) and the physical
 * backing names an incremental reindex produces ({@code datasetindex_v3_0_13_1_0_1704067200000}, or
 * a bare timestamp suffix {@code datasetindex_v3_1704067200000}). Bulk responses and search hits
 * carry the physical name, so callers classifying those must not assume the token is terminal.
 * {@link #isAliasName} accepts only the alias form, for callers that enumerate or address the
 * convention name itself. Semantic indices ({@code datasetindex_v2_semantic...}) are not entity
 * indices and never match either way.
 */
public enum EntityIndexVersion {
  V2("index_v2"),
  V3("index_v3");

  private static final String SEMANTIC_SUFFIX = "_semantic";
  private static final Pattern PHYSICAL_SUFFIX = Pattern.compile("_(?:\\d+|.+_\\d+)");

  private final String token;

  EntityIndexVersion(@Nonnull final String token) {
    this.token = token;
  }

  @Nonnull
  public String token() {
    return token;
  }

  public boolean matches(@Nullable final String indexName) {
    return fromIndexName(indexName).filter(this::equals).isPresent();
  }

  /** True only for the alias/convention form: the version token is terminal. */
  public boolean isAliasName(@Nullable final String indexName) {
    return matches(indexName) && indexName.endsWith(token);
  }

  /**
   * Everything before the version token, prefix included, when {@code indexName} is this version.
   */
  @Nonnull
  public Optional<String> keyOf(@Nullable final String indexName) {
    if (!matches(indexName)) {
      return Optional.empty();
    }
    return Optional.of(indexName.substring(0, indexName.lastIndexOf(token)));
  }

  @Nonnull
  public static Optional<EntityIndexVersion> fromIndexName(@Nullable final String indexName) {
    if (indexName == null) {
      return Optional.empty();
    }
    EntityIndexVersion latest = null;
    int latestStart = -1;
    for (EntityIndexVersion version : values()) {
      final int start = indexName.lastIndexOf(version.token);
      if (start > latestStart) {
        latest = version;
        latestStart = start;
      }
    }
    // A token at position 0 has no key in front of it and is not an entity index.
    if (latest == null || latestStart <= 0) {
      return Optional.empty();
    }
    final String rest = indexName.substring(latestStart + latest.token.length());
    if (rest.isEmpty()) {
      return Optional.of(latest);
    }
    if (rest.startsWith(SEMANTIC_SUFFIX)) {
      return Optional.empty();
    }
    return PHYSICAL_SUFFIX.matcher(rest).matches() ? Optional.of(latest) : Optional.empty();
  }
}
