package com.linkedin.metadata.test.definition;

import com.linkedin.common.urn.Urn;
import lombok.Data;


/**
 * Definition of a DataHub Test that includes a targeting rule and the main test rule
 * i.e. targeting rule defines whether or not to apply the test to a certain entity,
 * and the main test rule defines whether the entity passed the test or not
 */
@Data
public class TestDefinition {
  /**
   * Urn of the test
   */
  private final Urn testUrn;

  /**
   * Targeting rule that defines which entities are eligible for this test
   */
  private final TestTargetingRule target;

  /**
   * Main test rule that returns true or false given an entity
   */
  private final TestPredicate rule;
}
