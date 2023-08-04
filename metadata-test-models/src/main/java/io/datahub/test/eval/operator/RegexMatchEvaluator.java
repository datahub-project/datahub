package io.datahub.test.eval.operator;

import com.google.common.collect.ImmutableSet;
import io.datahub.test.definition.expression.Expression;
import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.definition.value.ListType;
import io.datahub.test.definition.value.StringType;
import io.datahub.test.definition.value.ValueType;
import io.datahub.test.eval.ResolvedOperand;
import io.datahub.test.eval.ResolvedOperands;
import io.datahub.test.exception.InvalidOperandException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Supports two operands, both list of string. Returns true
 * if any string on the left hand side matches any regex string on the right hand side.
 */
public class RegexMatchEvaluator extends BaseOperatorEvaluator {

  private static final Set<ValueType> SUPPORTED_OPERAND_TYPES = ImmutableSet.of(new ListType(new StringType()));

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.REGEX_MATCH;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (operands.size() != 2) {
      throw new InvalidOperandException("Invalid params for the operation: Requires 2 input operands");
    }
    // Validate that both input types are string lists.
    if (!isSupportedOperandType(operands.get(0).getExpression()) || !isSupportedOperandType(
        operands.get(1).getExpression())) {
      throw new InvalidOperandException("Invalid params for the operation: Requires 2 string list operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {

    ResolvedOperand operand1 = resolvedOperands.get(0); // Query response -> This will be list of string.
    ResolvedOperand operand2 = resolvedOperands.get(1); // -> This will be user provided or list.

    Set<String> operand1Values = toStringSet(operand1);
    Set<String> operand2Values = toStringSet(operand2);

    return operand2Values.stream().anyMatch(containee -> operand1Values.stream().anyMatch(container -> container.matches(containee)));
  }

  private Set<String> toStringSet(ResolvedOperand operand) {
    if (operand.getExpression().getValue() instanceof String) {
      return Collections.singleton((String) operand.getExpression().getValue());
    } else if (operand.getExpression().getValue() instanceof List) {
      return new HashSet<>((List<String>) operand.getExpression().getValue());
    } else {
      throw new IllegalArgumentException(String.format(
          "Failed to evaluate ContainsStr operator against operand of type %s. Expected string or string list.",
          operand.getExpression().getValue().getClass()));
    }
  }

  private boolean isSupportedOperandType(Expression exp) {
    return SUPPORTED_OPERAND_TYPES.contains(exp.valueType());
  }
}