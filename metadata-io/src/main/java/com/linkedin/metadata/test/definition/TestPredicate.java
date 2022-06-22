package com.linkedin.metadata.test.definition;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import java.util.Map;
import lombok.Value;


/**
 * DataHub Test Rule that returns a true or false when applied to an entity
 */
@Value
public class TestPredicate {
  /**
   * Operation to evaluate the rule e.g. equals
   */
  String operation;

  /**
   * Parameters to pass onto the operation
   */
  OperationParams params;

  /**
   * Whether or not to negate the test rule
   */
  boolean negated;

  public TestPredicate(String operation, Map<String, OperationParam> params, boolean negated) {
    this.operation = operation;
    this.params = new OperationParams(params);
    this.negated = negated;
  }

  public TestPredicate(String operation, Map<String, OperationParam> params) {
    this(operation, params, false);
  }
}
