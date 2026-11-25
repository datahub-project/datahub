package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Search V3 full-text catalog of structured properties. Defaults live in {@code application.yaml}
 * under {@code elasticsearch.search.structuredProperties}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StructuredPropertyQueryConfiguration {
  /** How long a resolved property list is reused. Matches the property-definition aspect cache. */
  private int cacheTtlSeconds;

  /** Weight of {@code .text} when a definition does not set {@code boostScore}. */
  private float defaultTextWeight;

  /**
   * Stemmed weight relative to {@code .text} when {@code subfieldBoostScores.stemmed} is absent.
   */
  private float stemmedFactor;

  /** Page size when scrolling structured-property entities. */
  private int pageSize;

  /**
   * How many property definitions the catalog keeps. Defaults to {@code
   * elasticsearch.index.maxObjectKeys}.
   */
  private int maxProperties;
}
