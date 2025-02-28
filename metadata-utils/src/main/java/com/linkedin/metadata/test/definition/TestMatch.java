package com.linkedin.metadata.test.definition;

import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.List;
import javax.annotation.Nullable;
import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class TestMatch {
  /** List of entity types to target */
  List<String> entityTypes;

  /** Rules to further specify the entities being targeted by this test */
  @Nullable Predicate conditions;
}
