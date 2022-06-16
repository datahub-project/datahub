package com.linkedin.metadata.test.definition.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import java.util.List;
import lombok.Value;


/**
 * Parameter with a list of predicates that are resolved to return either true/false
 */
@Value
public class PredicateParam implements OperationParam {
  List<TestPredicate> predicates;

  @Override
  public Type getType() {
    return Type.PREDICATE;
  }
}
