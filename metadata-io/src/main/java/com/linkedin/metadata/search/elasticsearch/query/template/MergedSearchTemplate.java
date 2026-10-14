package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.StructuredPropertyQuery;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Parent chain merged into one template. A null section is absent. */
@Value
@Builder
public class MergedSearchTemplate {
  @Nonnull String urn;
  @Nonnull String dialect;
  @Nullable String query;

  /** Hole name to the JSON spliced into that hole. Empty when the template fills none. */
  @Nonnull Map<String, String> queryFragments;

  @Nullable String clauses;
  @Nullable String functionScore;
  @Nullable String rescore;
  @Nullable String knn;
  @Nullable SearchTemplateRanking ranking;
  @Nullable SearchTemplateRetrieval retrieval;
  @Nullable SearchTemplateLadder ladder;
  @Nullable String aggregations;
  @Nullable String sort;
  @Nullable SearchFieldOverrides fields;
  @Nullable StructuredPropertyQuery structuredPropertyQuery;
  @Nullable SearchTemplateBoostArray boosts;
  @Nullable HighlightFieldOverrides highlightFields;
  @Nonnull Map<String, String> parameters;
  long lastModified;

  /** Compiled route patterns, in order. Empty when this template is pinned. */
  @Builder.Default @Nonnull List<SearchTemplateRoutes.Compiled> routes = List.of();

  /** Template used when no pattern matches. Null stays on this template. */
  @Nullable String defaultRoute;

  /** URNs from the requested template through its parents, used to drop the cache on a write. */
  @Nonnull List<String> chain;

  /**
   * The requested template came from the shipped file, or this process always reads that file.
   * Stored lookups such as structured properties are empty in that case.
   */
  @Builder.Default boolean classpathBridge = false;
}
