package io.datahub.test.definition;

import com.linkedin.common.urn.Urn;
import io.datahub.test.definition.operator.Predicate;
import lombok.Data;
import lombok.ToString;


/**
 * Definition of a DataHub Test that includes a targeting rule and the main test rule
 * i.e. targeting rule defines whether or not to apply the test to a certain entity,
 * and the main test rule defines whether the entity passed the test or not
 */
@Data
@ToString
public class TestDefinition {
  /**
   * Urn of the test
   */
  private final Urn urn;

  /**
   * Targeting rule that defines which entities are eligible for this test
   */
  private final TestMatch on;

  /**
   * Main test rule that returns true or false given an entity
   */
  private final Predicate rules;

  /**
   * Actions to run on failure / success of a particular test.
   */
  private final TestActions actions;
}
