package com.linkedin.metadata.test.action.form;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** This unassigns a requirements form using an action. */
@RequiredArgsConstructor
@Slf4j
public class UnassignFormAction extends ValuesAction {

  private static final String FORM_URN_PARAMETER = "formUrn";

  private final FormServiceAsync formService;

  @Override
  public ActionType getActionType() {
    return ActionType.UNASSIGN_FORM;
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
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // 1. Extract Parameters
    final Urn formUrn = UrnUtils.getUrn(params.getParams().get(FORM_URN_PARAMETER).get(0));
    // 2. Apply the action
    try {
      formService.batchUnassignFormForEntities(opContext, urns, formUrn);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to apply form unassignment action for urns %s, form urn %s", urns, formUrn),
          e);
    }
  }
}
