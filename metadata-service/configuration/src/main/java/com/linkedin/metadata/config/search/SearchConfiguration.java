package com.linkedin.metadata.config.search;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
@Slf4j
public class SearchConfiguration {
  private int maxTermBucketSize;

  /**
   * Default number of buckets returned per facet aggregation (e.g. the Type and Platform filter
   * dropdowns) when a caller doesn't set searchFlags.maxAggValues. Read only by {@code
   * AggregationQueryBuilder}, the sole consumer of the flag, so it applies to every entrypoint
   * (GraphQL, OpenAPI, Rest.li, the Python SDK). Matches {@code SearchFlags.pdl}'s default (20), so
   * leaving it unset changes nothing. Still capped by {@link #maxTermBucketSize}.
   */
  @Builder.Default private int defaultMaxAggValues = 20;

  private boolean pointInTimeCreationEnabled;
  private ExactMatchConfiguration exactMatch;
  private PartialConfiguration partial;
  private CustomConfiguration custom;
  private GraphQueryConfiguration graph;
  private WordGramConfiguration wordGram;
  private SearchValidationConfiguration validation;

  /**
   * Configurable entity-type lists (value/add/remove). Production defaults live in {@code
   * application.yaml}. Env vars: {@code SEARCH_*_ENTITY_TYPES}, {@code SEARCH_*_ENTITY_TYPES_ADD},
   * {@code SEARCH_*_ENTITY_TYPES_REMOVE}.
   */
  private EntityTypeListConfig defaultEntityTypes;

  private EntityTypeListConfig autocompleteEntityTypes;

  private EntityTypeListConfig browseEntityTypes;

  private EntityTypeListConfig prioritizedSourceEntityTypes;

  private EntityTypeListConfig prioritizedDatahubEntityTypes;

  /** Warns when defaultMaxAggValues would be silently clamped to maxTermBucketSize. */
  @PostConstruct
  public void validate() {
    if (isDefaultMaxAggValuesAboveMaxTermBucketSize()) {
      log.warn(
          "elasticsearch.search.defaultMaxAggValues ({}) exceeds elasticsearch.search.maxTermBucketSize"
              + " ({}); requests that don't set searchFlags.maxAggValues will be clamped to {}.",
          defaultMaxAggValues,
          maxTermBucketSize,
          maxTermBucketSize);
    }
  }

  /** True when {@link #defaultMaxAggValues} would be clamped to {@link #maxTermBucketSize}. */
  public boolean isDefaultMaxAggValuesAboveMaxTermBucketSize() {
    return defaultMaxAggValues > maxTermBucketSize;
  }
}
