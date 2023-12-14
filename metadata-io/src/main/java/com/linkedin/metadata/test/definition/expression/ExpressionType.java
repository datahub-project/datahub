package com.linkedin.metadata.test.definition.expression;

/**
 * The type of expression support inside the Metadata Tests framework
 *
 * <p>Currently, an expression type maps 1-1 with the expected return type. In the future, this may
 * not be the case.
 */
public enum ExpressionType {
  /** A query type (resolvable to a list of strings) */
  QUERY,
  /** A literal string list type (list of strings) */
  LITERAL,
  /** A predicate type (resolvable to a boolean) */
  PREDICATE,
}
