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
   * Default number of buckets returned per facet aggregation (e.g. the Type/Platform filter
   * dropdowns) when a caller doesn't explicitly set searchFlags.maxAggValues. Read only by {@code
   * AggregationQueryBuilder.facetToAggregationBuilder}, the sole consumer of the flag, so it
   * applies uniformly to every entrypoint (GraphQL, OpenAPI, Rest.li, the Python SDK) rather than
   * only the callers that happen to echo a value back. Matches {@code SearchFlags.pdl}'s own
   * default (20), so leaving this unset changes nothing. Still subject to the {@link
   * #maxTermBucketSize} ceiling -- see {@link #isDefaultMaxAggValuesAboveMaxTermBucketSize()}.
   */
  @Builder.Default private int defaultMaxAggValues = 20;

  private boolean pointInTimeCreationEnabled;
  private ExactMatchConfiguration exactMatch;
  private PartialConfiguration partial;
  private CustomConfiguration custom;
  private GraphQueryConfiguration graph;
  private WordGramConfiguration wordGram;
  private SearchValidationConfiguration validation;
  private StructuredPropertyQueryConfiguration structuredProperties;

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

  /** Validate configuration parameters on bean creation */
  @PostConstruct
  public void validate() {
    if (isDefaultMaxAggValuesAboveMaxTermBucketSize()) {
      log.warn(
          "elasticsearch.search.defaultMaxAggValues ({}) exceeds elasticsearch.search.maxTermBucketSize"
              + " ({}); requests that don't set searchFlags.maxAggValues will be silently clamped to"
              + " {} instead of the configured default.",
          defaultMaxAggValues,
          maxTermBucketSize,
          maxTermBucketSize);
    }
  }

  /**
   * True when {@link #defaultMaxAggValues} would be silently clamped down to {@link
   * #maxTermBucketSize} rather than actually taking effect.
   */
  public boolean isDefaultMaxAggValuesAboveMaxTermBucketSize() {
    return defaultMaxAggValues > maxTermBucketSize;
  }

  /** Search V3 query templates ({@code elasticsearch.search.v3.templates}). */
  @Builder.Default private SearchV3Configuration v3 = new SearchV3Configuration();

  /**
   * Missing score sections are filled on a copy. Callers keep the original bean, so reading this
   * configuration from Spring does not depend on which builder was constructed first.
   */
  public static SearchConfiguration withSectionDefaults(SearchConfiguration searchConfiguration) {
    ExactMatchConfiguration exactMatch = searchConfiguration.getExactMatch();
    if (exactMatch == null) {
      exactMatch = new ExactMatchConfiguration();
      exactMatch.setExclusive(false);
      exactMatch.setWithPrefix(true);
      exactMatch.setExactFactor(16f);
      exactMatch.setPrefixFactor(1.1f);
      exactMatch.setCaseSensitivityFactor(0f);
      exactMatch.setEnableStructured(true);
    }
    PartialConfiguration partial = searchConfiguration.getPartial();
    if (partial == null) {
      partial = new PartialConfiguration();
      partial.setUrnFactor(0.5f);
      partial.setFactor(0.4f);
    }
    WordGramConfiguration wordGram = searchConfiguration.getWordGram();
    if (wordGram == null) {
      wordGram = new WordGramConfiguration();
      wordGram.setTwoGramFactor(1.2f);
      wordGram.setThreeGramFactor(1.5f);
      wordGram.setFourGramFactor(1.8f);
    }
    StructuredPropertyQueryConfiguration structuredProperties =
        searchConfiguration.getStructuredProperties();
    if (structuredProperties == null) {
      structuredProperties =
          StructuredPropertyQueryConfiguration.builder()
              .cacheTtlSeconds(300)
              .defaultTextWeight(0.8f)
              .stemmedFactor(0.5f)
              .pageSize(500)
              .maxProperties(1000)
              .build();
    }
    return searchConfiguration.toBuilder()
        .exactMatch(exactMatch)
        .partial(partial)
        .wordGram(wordGram)
        .structuredProperties(structuredProperties)
        .build();
  }

  private QueryCanonicalizationConfiguration canonicalization;
}
