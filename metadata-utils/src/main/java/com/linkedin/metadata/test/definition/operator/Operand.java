package com.linkedin.metadata.test.definition.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.linkedin.metadata.test.definition.expression.Expression;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/** An operand serves as input to an {@link Operator} */
@Getter
@ToString
@JsonDeserialize(builder = Operand.OperandBuilder.class)
public class Operand {
  /**
   * The index of the operand -- TODO: Determine whether this is really necessary beyond
   * convenience.
   */
  @JsonProperty("index")
  private final int index;

  /** An optional name for the operand. */
  @JsonProperty("name")
  private final String name;

  /** The expression associated with the operand. */
  @JsonProperty("expression")
  private final Expression expression;

  public Operand(final int index, final Expression expression) {
    this(index, null, expression);
  }

  @Builder
  public Operand(final int index, final String name, final Expression expression) {
    this.index = index;
    this.name = name;
    this.expression = expression;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class OperandBuilder {}
}
