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
}
