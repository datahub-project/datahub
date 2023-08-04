package io.datahub.test.definition.operator;

import io.datahub.test.definition.expression.Expression;
import lombok.Getter;
import lombok.ToString;


/**
 * An operand serves as input to an {@link Operator}
 */
@Getter
@ToString
public class Operand {
  /**
   * The index of the operand -- TODO: Determine whether this is really necessary beyond convenience.
   */
  private final int index;
  /**
   * An optional name for the operand.
   */
  private final String name;
  /**
   * The expression associated with the operand.
   */
  private final Expression expression;

  public Operand(final int index, final Expression expression) {
    this(index, null, expression);
  }

  public Operand(final int index, final String name, final Expression expression) {
    this.index = index;
    this.name = name;
    this.expression = expression;
  }
}
