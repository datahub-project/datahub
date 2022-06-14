package com.linkedin.metadata.test.definition.operation;

public interface OperationParam {

  Type getType();

  enum Type {
    STRING, STRING_LIST, PREDICATE, PREDICATE_LIST
  }
}
