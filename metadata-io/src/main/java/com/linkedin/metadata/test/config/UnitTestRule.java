package com.linkedin.metadata.test.config;

import java.util.Map;
import lombok.Value;


/**
 * DataHub Test Rule that returns a true or false when applied to an entity
 */
@Value
public class UnitTestRule implements TestRule {
  /**
   * Query to fetch the fields used to apply operation: e.g. dataPlatformInstance.platform
   */
  String query;

  /**
   * Operation to evaluate the rule e.g. equals
   */
  String operation;

  /**
   * Parameters to pass onto the operation
   */
  Map<String, Object> params;

  /**
   * Whether or not to negate the test rule
   */
  boolean negated;

  public UnitTestRule(String query, String operation, Map<String, Object> params) {
    this(query, operation, params, false);
  }

  public UnitTestRule(String query, String operation, Map<String, Object> params, boolean negated) {
    this.query = query;
    this.operation = operation;
    this.params = params;
    this.negated = negated;
  }
}
