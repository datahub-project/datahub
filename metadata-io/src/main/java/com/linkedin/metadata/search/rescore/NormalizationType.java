package com.linkedin.metadata.search.rescore;

/** Types of normalization that can be applied to signals. */
public enum NormalizationType {
  /** Sigmoid normalization: maps value to bounded range using sigmoid curve */
  SIGMOID,
  /** Log-sigmoid: applies log10 before sigmoid for large value ranges (e.g., 0 to millions) */
  LOG_SIGMOID,
  /** Linear decay: linear interpolation based on age/distance */
  LINEAR_DECAY,
  /** Boolean: maps true/false to configured values */
  BOOLEAN,
  /** No normalization: pass through raw value */
  NONE
}
