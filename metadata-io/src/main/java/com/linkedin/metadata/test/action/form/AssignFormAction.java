package com.linkedin.metadata.test.action.form;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This form assigns a requirements form to a particular entity if it is not already set. If the
 * form is
 */
@RequiredArgsConstructor
@Slf4j
public class AssignFormAction extends ValuesAction {

  private static final String FORM_URN_PARAMETER = "formUrn";

  private final FormService formService;

  @Override
  public ActionType getActionType() {
    return ActionType.ASSIGN_FORM;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(FORM_URN_PARAMETER)
        || !(params.getParams().get(FORM_URN_PARAMETER).size() == 1)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'formUrn' parameter.");
    }
  }

  @Override
  public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
    // 1. Extract Parameters
    final Urn formUrn = UrnUtils.getUrn(params.getParams().get(FORM_URN_PARAMETER).get(0));
    // 2. Apply the action
    try {
      formService.batchAssignFormToEntities(urns, formUrn);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to apply form assignment action for urns %s, form urn %s", urns, formUrn),
          e);
    }
  }
}
