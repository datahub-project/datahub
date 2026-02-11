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
  private RescoreConfiguration rescore;
  private RescoreFormulaConfig rescoreFormula;
  private GraphQueryConfiguration graph;
  private WordGramConfiguration wordGram;
  private SearchValidationConfiguration validation;

  /**
   * Enable Search V2.5 features. When false: Uses V2 logic (current main branch). When true: Uses
   * V2.5 logic (fuzzy matching, OR operator, improved ranking). Default: false (for backwards
   * compatibility)
   */
  private boolean enableSearchV2_5 = false;

  /**
   * Enable fuzzy matching in V2.5 queries. Only applies when enableSearchV2_5 is true. When false:
   * V2.5 uses exact term matching (no typo tolerance). When true: V2.5 uses fuzzy matching with
   * ~1/~2 edit distance for typo tolerance. Default: false (fuzzy disabled by default)
   */
  private boolean enableFuzzyMatching = false;
}
