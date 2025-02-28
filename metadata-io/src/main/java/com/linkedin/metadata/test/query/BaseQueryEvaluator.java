package com.linkedin.metadata.test.query;

import com.linkedin.metadata.test.definition.ValidationResult;
import java.util.Collections;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public abstract class BaseQueryEvaluator implements QueryEvaluator {
  @Getter @Setter QueryEngine queryEngine;

  protected ValidationResult invalidResultWithMessage(String message) {
    return new ValidationResult(false, Collections.singletonList(message));
  }
}
