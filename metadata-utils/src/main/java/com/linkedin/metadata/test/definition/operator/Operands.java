package com.linkedin.metadata.test.definition.operator;

import com.linkedin.metadata.test.definition.expression.ExpressionType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;

@Value
@Getter
/** A set of named {@link Expression}s. */
public class Operands {
  /** The list of operands. */
  List<Operand> operands;

  Map<String, Operand> nameToOperand;

  public Operands(final List<Operand> operands) {
    this.operands = operands;
    this.nameToOperand = initNameToOperands(operands);
  }

  /** Utility function to check whether there is a param of with input key of input type */
  public boolean hasKeyOfType(String key, ExpressionType type) {
    return nameToOperand.containsKey(key)
        && nameToOperand.get(key).getExpression().expressionType() == type;
  }

  public Operand get(String name) {
    return nameToOperand.get(name);
  }

  public Operand get(int index) {
    if (index >= this.operands.size()) {
      throw new IllegalArgumentException(
          String.format("Index out of bounds: Illegal operand index %s provided!", index));
    }
    return this.operands.get(index);
  }

  public List<Operand> get() {
    return this.operands;
  }

  public int size() {
    return this.operands.size();
  }

  /**
   * Utility function to get param with input key of input paramClass. Returns empty if there is
   * none
   */
  public <T> Optional<T> getOperandOfType(String key, Class<T> paramClass) {
    if (!nameToOperand.containsKey(key)
        || !paramClass.isAssignableFrom(nameToOperand.get(key).getExpression().getClass())) {
      return Optional.empty();
    }
    return Optional.of(paramClass.cast(nameToOperand.get(key).getExpression()));
  }

  /** Utility function to get all parameters with the input paramClass type */
  public <T> List<T> getOperandsOfType(Class<T> clazz) {
    return operands.stream()
        .map(Operand::getExpression)
        .filter(exp -> clazz.isAssignableFrom(exp.getClass()))
        .map(clazz::cast)
        .collect(Collectors.toList());
  }

  private static Map<String, Operand> initNameToOperands(List<Operand> operands) {
    return operands.stream()
        .filter(op -> op.getName() != null)
        .collect(Collectors.toMap(Operand::getName, op -> op));
  }

  @Override
  public String toString() {
    return this.operands.toString();
  }
}
