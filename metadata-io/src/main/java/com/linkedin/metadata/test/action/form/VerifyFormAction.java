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
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class VerifyFormAction implements Action {

  private static final String ACTOR_URN_PARAMETER = "actorUrn";
  private static final String FORM_URN_PARAMETER = "formUrn";

  private final FormServiceAsync formService;

  @Override
  public ActionType getActionType() {
    return ActionType.VERIFY_FORM;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!paramsContainsKey(params, ACTOR_URN_PARAMETER)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'actorUrn' parameter.");
    }
    if (!paramsContainsKey(params, FORM_URN_PARAMETER)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'formUrn' parameter.");
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    final List<String> urnStrings = urns.stream().map(Urn::toString).collect(Collectors.toList());
    final Urn actorUrn = UrnUtils.getUrn(params.getParams().get(ACTOR_URN_PARAMETER).get(0));
    final Urn formUrn = UrnUtils.getUrn(params.getParams().get(FORM_URN_PARAMETER).get(0));

    try {
      formService.batchVerifyForm(opContext, urnStrings, formUrn, actorUrn);
    } catch (Exception e) {
      log.error(
          String.format("Failed to verify form action for urns %s, form urn %s", urns, formUrn), e);
    }
  }

  private boolean paramsContainsKey(ActionParameters params, String key) {
    return params.getParams().containsKey(key) && params.getParams().get(key).size() >= 1;
  }
}
