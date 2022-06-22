package com.linkedin.metadata.test.definition.operation;

/**
 * Interface for defining the parameter inputted into an operator
 */
public interface OperationParam {

  Type getType();

  enum Type {
    STRING, PREDICATE, QUERY
  }
}
