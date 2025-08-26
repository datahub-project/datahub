package com.linkedin.metadata.test.action.form;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormServiceAsync;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This action is used to mark a particular requirements form prompt as incomplete when it has
 * become invalid for a given entity.
 */
@RequiredArgsConstructor
@Slf4j
public class SetFormPromptIncompleteAction implements Action {

  private static final String FORM_URN_PARAMETER = "formUrn";
  private static final String FORM_PROMPT_ID_PARAMETER = "formPromptId";

  private final FormServiceAsync formService;

  @Override
  public ActionType getActionType() {
    return ActionType.SET_FORM_PROMPT_INCOMPLETE;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(FORM_URN_PARAMETER)
        || !(params.getParams().get(FORM_URN_PARAMETER).size() == 1)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'formUrn' parameter.");
    }
    if (!params.getParams().containsKey(FORM_PROMPT_ID_PARAMETER)
        || !(params.getParams().get(FORM_PROMPT_ID_PARAMETER).size() == 1)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'formPromptId' parameter.");
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // 1. Extract Parameters
    final Urn formUrn = UrnUtils.getUrn(params.getParams().get(FORM_URN_PARAMETER).get(0));
    final String formPromptId = params.getParams().get(FORM_PROMPT_ID_PARAMETER).get(0);
    // 2. Unset the form prompt
    try {
      formService.batchSetFormPromptIncomplete(opContext, urns, formUrn, formPromptId);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to apply form unassignment action for urns %s, form urn %s", urns, formUrn),
          e);
    }
  }
}
