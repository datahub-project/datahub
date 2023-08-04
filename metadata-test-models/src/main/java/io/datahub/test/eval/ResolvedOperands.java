package io.datahub.test.eval;

import io.datahub.test.definition.expression.ExpressionType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * A wrapper around {@link ResolvedExpression} which contains named operands and their resolved value.
 */
public class ResolvedOperands {
  /**
   * The list of operands.
   */
  private final List<ResolvedOperand> operands;
  private final Map<String, ResolvedOperand> nameToOperand;

  public ResolvedOperands(final List<ResolvedOperand> operands) {
    this.operands = operands;
    this.nameToOperand = initNameToOperands(operands);
  }

  /**
   * Utility function to check whether there is a param of with input key of input type
   */
  public boolean hasKeyOfType(String key, ExpressionType type) {
    return nameToOperand.containsKey(key) && nameToOperand.get(key).getExpression().getExpressionType() == type;
  }

  public ResolvedOperand get(String name) {
    return nameToOperand.get(name);
  }

  public ResolvedOperand get(int index) {
    if (index >= this.operands.size()) {
      throw new IllegalArgumentException(String.format("Index out of bounds: Illegal operand index %s provided!", index));
    }
    return this.operands.get(index);
  }

  public List<ResolvedOperand> get() {
    return this.operands;
  }

  public int size() {
    return this.operands.size();
  }

  /**
   * Utility function to get param with input key of input paramClass. Returns empty if there is none
   */
  public <T> Optional<T> getOperandOfType(String key, Class<T> paramClass) {
    if (!nameToOperand.containsKey(key) || !paramClass.isAssignableFrom(nameToOperand.get(key).getExpression().getClass())) {
      return Optional.empty();
    }
    return Optional.of(paramClass.cast(nameToOperand.get(key).getExpression()));
  }

  /**
   * Utility function to get all parameters with the input paramClass type
   */
  public <T> List<T> getOperandsOfType(Class<T> clazz) {
    return operands
        .stream()
        .map(ResolvedOperand::getExpression)
        .filter(exp -> clazz.isAssignableFrom(exp.getClass()))
        .map(clazz::cast)
        .collect(Collectors.toList());
  }

  private static Map<String, ResolvedOperand> initNameToOperands(List<ResolvedOperand> operands) {
    return operands.stream()
        .filter(op -> op.getName() != null)
        .collect(Collectors.toMap(ResolvedOperand::getName, op -> op));
  }
}
