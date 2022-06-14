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
   * Query to fetch the fields used to apply operation: e.g. dataPlatformInstance.platform
   */
  TestQuery query;

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

  public TestPredicate(String query, String operation, Map<String, OperationParam> params, boolean negated) {
    this.query = new TestQuery(query);
    this.operation = operation;
    this.params = new OperationParams(params);
    this.negated = negated;
  }

  public TestPredicate(String query, String operation, Map<String, OperationParam> params) {
    this(query, operation, params, false);
  }

  public TestPredicate(String operation, Map<String, OperationParam> params, boolean negated) {
    this("", operation, params, negated);
  }

  public TestPredicate(String operation, Map<String, OperationParam> params) {
    this("", operation, params, false);
  }
}
