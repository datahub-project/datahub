package com.linkedin.metadata.test.definition.operation;

import lombok.Value;


@Value
public class StringParam implements OperationParam {
  String value;

  @Override
  public Type getType() {
    return Type.STRING;
  }
}
