package io.datahub.test.eval;

import lombok.Getter;

/**
 * A resolved operand is an operand which has been resolved to a value.
 */
@Getter
public class ResolvedOperand {
  /**
   * The index of the operand
   */
  private final int index;
  /**
   * An optional name for the operand.
   */
  private final String name;
  /**
   * The expression associated with the operand.
   */
  private final ResolvedExpression expression;

  public ResolvedOperand(final int index, final ResolvedExpression expression) {
    this(index, null, expression);
  }

  public ResolvedOperand(final int index, final String name, final ResolvedExpression expression) {
    this.index = index;
    this.name = name;
    this.expression = expression;
  }
}
