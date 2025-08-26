package com.linkedin.metadata.test.action.form;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.form.FormPromptType;
import com.linkedin.metadata.service.FormServiceAsync;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SubmitFormPromptAction implements Action {

  private static final String ACTOR_URN_PARAMETER = "actorUrn";
  private static final String FORM_URN_PARAMETER = "formUrn";
  private static final String PROMPT_ID_PARAMETER = "promptId";
  private static final String PROMPT_TYPE_PARAMETER = "promptType";
  private static final String STRUCTURED_PROPERTY_URN_PARAMETER = "structuredPropertyUrn";
  private static final String STRING_VALUES_PARAMETER = "stringValues";
  private static final String NUMBER_VALUES_PARAMETER = "numberValues";
  private static final String OWNERS_PARAMETER = "owners";
  private static final String OWNERSHIP_TYPE_URN_PARAMETER = "ownershipTypeUrn";
  private static final String DOCUMENTATION_PARAMETER = "documentation";
  private static final String GLOSSARY_TERMS_PARAMETER = "glossaryTerms";
  private static final String DOMAIN_PARAMETER = "domain";

  private final FormServiceAsync formService;

  @Override
  public ActionType getActionType() {
    return ActionType.SUBMIT_FORM_PROMPT;
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
    if (!paramsContainsKey(params, PROMPT_ID_PARAMETER)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'promptId' parameter.");
    }
    if (!paramsContainsKey(params, PROMPT_TYPE_PARAMETER)) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'promptType' parameter.");
    }
    try {
      FormPromptType.valueOf(params.getParams().get(PROMPT_TYPE_PARAMETER).get(0));
    } catch (Exception e) {
      throw new InvalidActionParamsException("Action parameter 'promptType' is invalid");
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    final List<String> urnStrings = urns.stream().map(Urn::toString).collect(Collectors.toList());
    final Urn actorUrn = UrnUtils.getUrn(params.getParams().get(ACTOR_URN_PARAMETER).get(0));
    final Urn formUrn = UrnUtils.getUrn(params.getParams().get(FORM_URN_PARAMETER).get(0));
    final String promptId = params.getParams().get(PROMPT_ID_PARAMETER).get(0);
    final FormPromptType promptType =
        FormPromptType.valueOf(params.getParams().get(PROMPT_TYPE_PARAMETER).get(0));

    try {
      if (promptType.equals(FormPromptType.STRUCTURED_PROPERTY)) {
        submitStructuredPropertyResponse(
            opContext, params, urnStrings, formUrn, promptId, actorUrn);
      } else if (promptType.equals(FormPromptType.OWNERSHIP)) {
        submitOwnershipResponse(opContext, params, urnStrings, formUrn, promptId, actorUrn);
      } else if (promptType.equals(FormPromptType.DOCUMENTATION)) {
        submitDocumentationResponse(opContext, params, urnStrings, formUrn, promptId, actorUrn);
      } else if (promptType.equals(FormPromptType.GLOSSARY_TERMS)) {
        submitGlossaryTermsResponse(opContext, params, urnStrings, formUrn, promptId, actorUrn);
      } else if (promptType.equals(FormPromptType.DOMAIN)) {
        submitDomainResponse(opContext, params, urnStrings, formUrn, promptId, actorUrn);
      } else {
        log.error(
            String.format(
                "Failed to submit form prompt action for urns %s, form urn %s as no valid prompt type was provided",
                urns, formUrn));
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit form prompt action for urns %s, form urn %s", urns, formUrn),
          e);
    }
  }

  private boolean paramsContainsKey(ActionParameters params, String key) {
    return params.getParams().containsKey(key) && params.getParams().get(key).size() >= 1;
  }

  private void submitStructuredPropertyResponse(
      @Nonnull OperationContext opContext,
      ActionParameters params,
      List<String> urnStrings,
      Urn formUrn,
      String promptId,
      Urn actorUrn) {
    try {
      // Grab params specific to submitting structured property response
      if (!paramsContainsKey(params, STRUCTURED_PROPERTY_URN_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'structuredPropertyUrn' parameter.");
      }
      final Urn structuredPropertyUrn =
          UrnUtils.getUrn(params.getParams().get(STRUCTURED_PROPERTY_URN_PARAMETER).get(0));
      List<String> stringValues = params.getParams().get(STRING_VALUES_PARAMETER);
      List<String> numberValues = params.getParams().get(NUMBER_VALUES_PARAMETER);
      PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
      if (stringValues != null) {
        values.addAll(
            stringValues.stream().map(PrimitivePropertyValue::create).collect(Collectors.toList()));
      } else if (numberValues != null) {
        values.addAll(
            numberValues.stream()
                .map(Double::parseDouble)
                .map(PrimitivePropertyValue::create)
                .collect(Collectors.toList()));
      } else {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'stringValues' or 'numberValues' parameter.");
      }

      formService.batchSubmitStructuredPropertyPromptResponse(
          opContext, urnStrings, structuredPropertyUrn, values, formUrn, promptId, actorUrn, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit structured property response for urns %s, form urn %s",
              urnStrings, formUrn),
          e);
    }
  }

  private void submitOwnershipResponse(
      @Nonnull OperationContext opContext,
      ActionParameters params,
      List<String> urnStrings,
      Urn formUrn,
      String promptId,
      Urn actorUrn) {
    try {
      // Grab params specific to submitting ownership response
      if (!paramsContainsKey(params, OWNERS_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'owners' parameter.");
      }
      if (!paramsContainsKey(params, OWNERSHIP_TYPE_URN_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'ownershipTypeUrn' parameter.");
      }

      List<Urn> owners =
          params.getParams().get(OWNERS_PARAMETER).stream()
              .map(UrnUtils::getUrn)
              .collect(Collectors.toList());
      final Urn ownershipTypeUrn =
          UrnUtils.getUrn(params.getParams().get(OWNERSHIP_TYPE_URN_PARAMETER).get(0));

      formService.batchSubmitOwnershipPromptResponse(
          opContext, urnStrings, owners, ownershipTypeUrn, formUrn, promptId, actorUrn, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit ownership response for urns %s, form urn %s", urnStrings, formUrn),
          e);
    }
  }

  private void submitDocumentationResponse(
      @Nonnull OperationContext opContext,
      ActionParameters params,
      List<String> urnStrings,
      Urn formUrn,
      String promptId,
      Urn actorUrn) {
    try {
      // Grab params specific to submitting documentation response
      if (!paramsContainsKey(params, DOCUMENTATION_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'documentation' parameter.");
      }
      final String documentation = params.getParams().get(DOCUMENTATION_PARAMETER).get(0);

      formService.batchSubmitDocumentationPromptResponse(
          opContext, urnStrings, documentation, formUrn, promptId, actorUrn, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit ownership response for urns %s, form urn %s", urnStrings, formUrn),
          e);
    }
  }

  private void submitGlossaryTermsResponse(
      @Nonnull OperationContext opContext,
      ActionParameters params,
      List<String> urnStrings,
      Urn formUrn,
      String promptId,
      Urn actorUrn) {
    try {
      // Grab params specific to submitting documentation response
      if (!paramsContainsKey(params, GLOSSARY_TERMS_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'glossaryTerms' parameter.");
      }
      List<Urn> termUrns =
          params.getParams().get(GLOSSARY_TERMS_PARAMETER).stream()
              .map(UrnUtils::getUrn)
              .collect(Collectors.toList());

      formService.batchSubmitGlossaryTermsPromptResponse(
          opContext, urnStrings, termUrns, formUrn, promptId, actorUrn, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit ownership response for urns %s, form urn %s", urnStrings, formUrn),
          e);
    }
  }

  private void submitDomainResponse(
      @Nonnull OperationContext opContext,
      ActionParameters params,
      List<String> urnStrings,
      Urn formUrn,
      String promptId,
      Urn actorUrn) {
    try {
      // Grab params specific to submitting documentation response
      if (!paramsContainsKey(params, DOMAIN_PARAMETER)) {
        throw new InvalidActionParamsException(
            "Action parameters are missing the required 'domain' parameter.");
      }
      Urn domainUrn =
          params.getParams().get(DOMAIN_PARAMETER).stream()
              .map(UrnUtils::getUrn)
              .collect(Collectors.toList())
              .get(0);

      formService.batchSubmitDomainPromptResponse(
          opContext, urnStrings, domainUrn, formUrn, promptId, actorUrn, false);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to submit domain response for urns %s, form urn %s", urnStrings, formUrn),
          e);
    }
  }
}
