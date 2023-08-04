package io.datahub.test.eval.operator;

import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.definition.value.BooleanType;
import io.datahub.test.eval.ResolvedOperands;
import io.datahub.test.exception.InvalidOperandException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * Or operation evaluator. Checks whether any of input predicates returns true
 */
@Slf4j
public class OrEvaluator extends BaseOperatorEvaluator {

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.OR;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    // Only predicates can be considered truthy :(
    if (!operands
        .get()
        .stream()
        .allMatch(operand -> BooleanType.get().equals(operand.getExpression().valueType()))) {
      throw new InvalidOperandException(
          "or operator requires boolean input operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {

    log.debug(String.format("Invoking 'or' operator with operands %s", resolvedOperands.get()
        .stream()
        .map(op -> op.getExpression().getValue())
        .collect(Collectors.toList())));

    return resolvedOperands.get()
        .stream()
        .anyMatch(operand -> Boolean.TRUE.equals(operand.getExpression().getValue()));
  }
}
