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
 * Placeholder action that removes managed assertions for an assertion assignment rule from
 * non-matching entities. The actual assertion removal logic will be added in a later PR.
 */
@Slf4j
public class RemoveAssertionAssignmentRuleAction implements Action {

  private static final String RULE_URN_PARAM = "ruleUrn";

  @Override
  public ActionType getActionType() {
    return ActionType.REMOVE_ASSERTION_ASSIGNMENT_RULE;
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
        "RemoveAssertionAssignmentRuleAction placeholder: would remove assertions for rule {} from {} entities",
        ruleUrn,
        urns.size());
  }
}
