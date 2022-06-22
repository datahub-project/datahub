package com.linkedin.metadata.test.definition;

import java.util.Collections;
import java.util.List;
import lombok.Value;


@Value
public class ValidationResult {
  boolean isValid;
  List<String> messages;

  private static final ValidationResult VALID = new ValidationResult(true, Collections.emptyList());

  public static ValidationResult validResult() {
    return VALID;
  }
}
