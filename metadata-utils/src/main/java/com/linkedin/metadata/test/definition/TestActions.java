package com.linkedin.metadata.test.definition;

import java.util.List;
import lombok.Data;
import lombok.ToString;

/**
 * Definition of a DataHub Test actions which determines what to do on success / failure of a
 * Metadata Test.
 */
@Data
@ToString
public class TestActions {
  /** Actions to apply if an entity is passing the test. */
  private final List<TestAction> passing;

  /** Actions to apply if an entity is failing the test. */
  private final List<TestAction> failing;
}
