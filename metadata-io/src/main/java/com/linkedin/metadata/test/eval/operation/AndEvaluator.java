package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.eval.ResolvedParams;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;

import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.PREDICATES;


/**
 * and operation evaluator. Checks whether any of input predicates returns true
 */
public class AndEvaluator extends BaseOperationEvaluator {

  @Override
  public String getOperation() {
    return "and";
  }

  @Override
  public void validate(OperationParams params) throws OperationParamsInvalidException {
    if (params.hasKeyOfType(PREDICATES, OperationParam.Type.PREDICATE)) {
      return;
    }
    throw new OperationParamsInvalidException(
        "and operation requires param \"predicates\" containing the list of predicates to compose");
  }

  @Override
  public boolean evaluate(ResolvedParams resolvedParams) throws OperationParamsInvalidException {
    if (resolvedParams.hasKeyOfType(PREDICATES, OperationParam.Type.PREDICATE)) {
      return resolvedParams.getResolvedParam(PREDICATES)
          .getResolvedPredicateParam()
          .stream()
          .allMatch(Boolean::valueOf);
    }
    throw new OperationParamsInvalidException(
        "and operation requires param \"predicates\" containing the list of predicates to compose");
  }
}
