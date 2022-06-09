package com.linkedin.metadata.test.definition;

import java.util.List;
import java.util.Optional;
import lombok.Value;


@Value
public class TestTargetingRule {
  /**
   * List of entity types to target
   */
  List<String> entityTypes;
  /**
   * Rules to further specify the entities being targeted by this test
   */
  Optional<TestPredicate> targetingRules;
}
