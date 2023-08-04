package io.datahub.test.eval.operator;

import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.definition.value.BooleanType;
import io.datahub.test.eval.ResolvedOperands;
import io.datahub.test.exception.InvalidOperandException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * and operation evaluator. Checks whether any of input predicates returns true
 */
@Slf4j
public class AndEvaluator extends BaseOperatorEvaluator {

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.AND;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (!operands.get()
        .stream()
        .allMatch(operand -> BooleanType.get().equals(operand.getExpression().valueType()))) {
      throw new InvalidOperandException(
          "and operator requires boolean input operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {

    log.debug(String.format("Invoking 'and' operator with operands %s", resolvedOperands.get()
        .stream()
        .map(op -> op.getExpression().getValue())
        .collect(Collectors.toList())));

    return resolvedOperands.get()
        .stream()
        .allMatch(operand -> Boolean.TRUE.equals(operand.getExpression().getValue()));
  }
}
