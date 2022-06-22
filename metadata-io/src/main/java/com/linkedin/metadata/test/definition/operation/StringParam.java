package com.linkedin.metadata.test.definition.operation;

import java.util.List;
import lombok.Value;


/**
 * Parameter with a list of simple string values (no resolution required)
 */
@Value
public class StringParam implements OperationParam {
  List<String> values;

  @Override
  public Type getType() {
    return Type.STRING;
  }
}
