package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class SearchConfiguration {
  private int maxTermBucketSize;
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
