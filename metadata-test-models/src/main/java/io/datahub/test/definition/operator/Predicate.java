package io.datahub.test.definition.operator;

import io.datahub.test.definition.expression.Expression;
import io.datahub.test.definition.expression.ExpressionType;
import io.datahub.test.definition.value.BooleanType;
import io.datahub.test.definition.value.ValueType;
import java.util.ArrayList;
import java.util.List;
import lombok.ToString;
import lombok.Value;

/**
 * This represents an {@link Operator} which returns a boolean (true / false) value.
 */
@Value
@ToString
public class Predicate implements Operator {

  OperatorType operatorType;
  Operands operands;

  public static Predicate of(OperatorType operatorType, List<Expression> expressions) {
    return Predicate.of(operatorType, expressions, false);
  }

  public static Predicate of(OperatorType operatorType, List<Expression> expressions, boolean negated) {
    return new Predicate(operatorType, convertExpListToOperands(expressions), negated);
  }

  @Override
  public ExpressionType expressionType() {
    return ExpressionType.PREDICATE;
  }

  @Override
  public ValueType valueType() {
    return new BooleanType();
  }

  public Predicate(OperatorType operator, List<Operand> operands) {
    this(operator, operands, false);
  }

  public Predicate(OperatorType operatorType, List<Operand> operands, boolean negated) {
    this(operatorType, new Operands(operands), negated);
  }

  public Predicate(OperatorType operatorType, Operands operands, boolean negated) {
    // TODO: Clean this hack up.
    if (negated) {
      // Wrap the operands in a NOT predicate.
      this.operatorType = OperatorType.NOT;
      this.operands = injectNotPredicate(operatorType, operands);
    } else {
      this.operatorType = operatorType;
      this.operands = operands;
    }
  }

  @Override
  public OperatorType operatorType() {
    return this.operatorType;
  }

  @Override
  public Operands operands() {
    return this.operands;
  }

  private static List<Operand> convertExpListToOperands(List<Expression> expressions) {
    final List<Operand> result = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      result.add(new Operand(i, expressions.get(i)));
    }
    return result;
  }

  private static Operands injectNotPredicate(OperatorType operatorType, Operands operands) {
    final Predicate innerPredicate = new Predicate(operatorType, operands, false);
    return new Operands(createNotOperands(innerPredicate));
  }

  private static List<Operand> createNotOperands(Predicate base) {
    final List<Operand> result = new ArrayList<>();
    result.add(new Operand(0, base));
    return result;
  }
}
