package com.linkedin.metadata.test.definition.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import lombok.Value;


@Value
public class PredicateParam implements OperationParam {
  TestPredicate predicate;

  @Override
  public Type getType() {
    return Type.PREDICATE;
  }
}
