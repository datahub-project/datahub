package com.linkedin.metadata.test.query;

import com.linkedin.metadata.test.definition.literal.Literal;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import java.util.Collections;
import java.util.List;
import lombok.Data;

@Data
public class QueryOperation {
  private final TestQuery query;
  private final OperatorType operatorType;
  private List<Literal> values = Collections.emptyList();
}
