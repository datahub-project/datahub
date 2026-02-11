package com.linkedin.metadata.search.rescore;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

/**
 * Value of a signal for a specific document, with full explanation.
 *
 * <p>This captures the entire transformation pipeline: raw → normalized → contribution.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SignalValue {
  /** Name of the signal (matches SignalDefinition.name) */
  String name;

  /** Raw value extracted from the document (may be null if field missing) */
  Object rawValue;

  /** Numeric representation of raw value */
  double numericValue;

  /** Value after normalization applied */
  double normalizedValue;

  /** Normalization type that was applied */
  NormalizationType normalizationType;

  /** Boost/exponent used in formula */
  double boost;

  /** Final contribution to score: pow(normalizedValue, boost) */
  double contribution;
}
