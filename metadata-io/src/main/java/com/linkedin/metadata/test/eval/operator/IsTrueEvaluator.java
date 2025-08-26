package com.linkedin.metadata.test.eval.operator;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.value.ListType;
import com.linkedin.metadata.test.definition.value.StringType;
import com.linkedin.metadata.test.definition.value.ValueType;
import com.linkedin.metadata.test.eval.ResolvedOperand;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Supports two operands, both list of string. Returns true if any string on the left hand side
 * matches "true" in case insensitive manner.
 */
public class IsTrueEvaluator extends BaseOperatorEvaluator {

  private static final String TRUE = "true";
  private static final Set<ValueType> SUPPORTED_OPERAND_TYPES =
      ImmutableSet.of(new ListType(new StringType()));

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.IS_TRUE;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (operands.size() != 1) {
      throw new InvalidOperandException(
          "Invalid params for the operation: Requires 1 input operands");
    }
    // Validate that both input types are string lists.
    if (!isSupportedOperandType(operands.get(0).getExpression())) {
      throw new InvalidOperandException(
          "Invalid params for the operation: Requires 1 string list operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {
    ResolvedOperand operand =
        resolvedOperands.get(0); // Query response -> This will be list of string.
    Set<String> operandValues = toStringSet(operand);
    // Non-empty and at least 1 non-empty / non-"false" string is considered "truthy"
    return (!operandValues.isEmpty()
        && !operandValues.stream()
            .allMatch(value -> value.equalsIgnoreCase("") || value.equalsIgnoreCase("false")));
  }

  private Set<String> toStringSet(ResolvedOperand operand) {
    if (operand.getExpression().getValue() instanceof String) {
      return Collections.singleton((String) operand.getExpression().getValue());
    } else if (operand.getExpression().getValue() instanceof List) {
      return new HashSet<>((List<String>) operand.getExpression().getValue());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Failed to evaluate ContainsStr operator against operand of type %s. Expected string or string list.",
              operand.getExpression().getValue().getClass()));
    }
  }

  private boolean isSupportedOperandType(Expression exp) {
    return SUPPORTED_OPERAND_TYPES.contains(exp.valueType());
  }
}
