package com.linkedin.metadata.aspect.plugins.validation;

public enum ValidationSubType {
  // A validation exception is thrown
  VALIDATION,
  // A failed precondition is thrown if the header constraints are not met
  PRECONDITION,
  // Exclude from processing further
  FILTER
}
