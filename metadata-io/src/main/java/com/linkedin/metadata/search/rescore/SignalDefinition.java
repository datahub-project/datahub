package com.linkedin.metadata.search.rescore;

import lombok.Builder;
import lombok.Value;

/**
 * Definition of a signal used in rescoring formula.
 *
 * <p>Each signal maps a document field to a normalized variable that can be used in the exp4j
 * formula.
 */
@Value
@Builder
public class SignalDefinition {
  /** Human-readable name of the signal (e.g., "viewCount", "hasDescription") */
  String name;

  /** Variable name used in the exp4j formula (e.g., "norm_views", "hasDesc") */
  String normalizedName;

  /** Path to extract the value from ES document (e.g., "viewCount", "_score") */
  String fieldPath;

  /** Type of the signal (determines extraction logic) */
  @Builder.Default SignalType type = SignalType.NUMERIC;

  /** Normalization configuration (how to transform raw value) */
  @Builder.Default NormalizationConfig normalization = NormalizationConfig.none();

  /** Exponent/boost to apply in power-law formula */
  @Builder.Default double boost = 1.0;
}
