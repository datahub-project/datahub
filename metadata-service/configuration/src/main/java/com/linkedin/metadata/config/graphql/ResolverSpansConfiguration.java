package com.linkedin.metadata.config.graphql;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * Configuration for custom, filtered GraphQL resolver-level OpenTelemetry spans.
 *
 * <p>These spans are emitted by a custom graphql-java {@code Instrumentation} (not the
 * all-or-nothing OTel {@code GraphQLTelemetry}) so that selected resolver names appear in traces
 * without causing span explosion on large list responses.
 */
@Data
public class ResolverSpansConfiguration {
  /** Master switch — when false, no resolver instrumentation is registered. */
  private boolean enabled = false;

  /**
   * When false (default), data fetchers reported as trivial (e.g. simple property/map getters) are
   * never traced regardless of other settings.
   */
  private boolean includeTrivialDataFetchers = false;

  /**
   * When true (default), only top-level {@code Query} / {@code Mutation} fields are traced. Nested
   * fields are traced only if explicitly listed in {@link #allowedPaths}.
   */
  private boolean topLevelOnly = true;

  /**
   * Explicit allow-list of {@code "ParentType.field"} entries to trace (e.g. {@code
   * "Query.searchAcrossEntities"}, {@code "Entity.ownership"}). Defaults to empty.
   */
  private List<String> allowedPaths = new ArrayList<>();
}
