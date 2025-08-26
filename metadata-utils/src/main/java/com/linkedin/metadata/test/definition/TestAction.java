package com.linkedin.metadata.test.definition;

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Definition of a DataHub Test action which determines what to do on success / failure of a
 * Metadata Test.
 */
@Data
@ToString
@EqualsAndHashCode
public class TestAction {

  /** The type of Action to apply. */
  private final ActionType type;

  /** Named parameters required for the Action of the given type. */
  private final Map<String, List<String>> params;
}
