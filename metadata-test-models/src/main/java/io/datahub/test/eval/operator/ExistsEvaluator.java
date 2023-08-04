package io.datahub.test.eval.operator;

import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.eval.ResolvedOperand;
import io.datahub.test.eval.ResolvedOperands;
import io.datahub.test.exception.InvalidOperandException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;


/**
 * Exists operation evaluator. Checks whether the query response exists
 */
@Slf4j
public class ExistsEvaluator extends BaseOperatorEvaluator {
  @Override
  public OperatorType getOperatorType() {
    return OperatorType.EXISTS;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (operands.size() != 1) {
      throw new InvalidOperandException(
          "Invalid params for the exists operation: Requires 1 input operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {
    ResolvedOperand operand = resolvedOperands.get(0); // Query response -> This will be list of string or string.

    log.debug(String.format("Invoking 'exists' operator with resolved operand %s", operand.getExpression().getValue()));

    return exists(operand.getExpression().getValue());
  }

  private boolean exists(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof List) {
      // Lists must be longer than 0 to "exist".
      List<?> list = (List<?>) value;
      // At least one of the list's objects must exist.
      return list.size() > 0 && list.stream().anyMatch(this::exists);
    }
    if (value instanceof String) {
      return ((String) value).length() > 0;
    }
    return true;
  }
}
