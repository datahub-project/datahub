package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * A template resolved and rendered for one query. Nothing is sent to the search engine.
 *
 * <p>There is no GraphQL, Rest.li, or OpenAPI route for this yet. Template choice has no shared
 * privilege, and the rendered body can contain caller-specific values, so an endpoint has to wait
 * for that privilege rather than grow an API-specific check.
 */
public record SearchTemplateDryRender(
    @Nonnull String resolvedUrn,
    @Nonnull List<String> parentChain,
    @Nonnull List<String> routeChain,
    @Nonnull String engineSlot,
    @Nonnull String queryDsl,
    @Nonnull List<String> queriedFields,
    int clauseCount,
    int estimatedClauseCost,
    @Nonnull List<CapabilityDecision> capabilities,
    @Nonnull List<String> ladderRungs) {

  public SearchTemplateDryRender {
    parentChain = List.copyOf(parentChain);
    routeChain = List.copyOf(routeChain);
    queriedFields = List.copyOf(queriedFields);
    capabilities = List.copyOf(capabilities);
    ladderRungs = List.copyOf(ladderRungs);
  }

  /** A declared capability or the section check. {@code detail} is a fixed code. */
  public record CapabilityDecision(
      @Nonnull String name, boolean accepted, @Nonnull String detail) {}
}
