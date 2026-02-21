package com.linkedin.metadata.test.action.assertion;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Placeholder action that upserts managed assertions for an assertion assignment rule on matching
 * entities. The actual assertion creation logic will be added in a later PR.
 */
@Slf4j
public class UpsertAssertionAssignmentRuleAction implements Action {

  private static final String RULE_URN_PARAM = "ruleUrn";

  @Override
  public ActionType getActionType() {
    return ActionType.UPSERT_ASSERTION_ASSIGNMENT_RULE;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(RULE_URN_PARAM)
        || params.getParams().get(RULE_URN_PARAM).size() != 1) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'ruleUrn' parameter.");
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    final String ruleUrn = params.getParams().get(RULE_URN_PARAM).get(0);
    log.info(
        "UpsertAssertionAssignmentRuleAction placeholder: would upsert assertions for rule {} on {} entities",
        ruleUrn,
        urns.size());
  }
}
