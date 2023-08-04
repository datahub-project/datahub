package io.datahub.test.action.term;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.GlossaryTermService;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;


@Slf4j
@RequiredArgsConstructor
public class RemoveGlossaryTermsAction implements Action {

  private static final String VALUES_PARAM = "values";

  private final GlossaryTermService glossaryTermService;

  @Override
  public ActionType getActionType() {
    return ActionType.REMOVE_GLOSSARY_TERMS;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(VALUES_PARAM)) {
      throw new InvalidActionParamsException("Action parameters are missing the required 'values' parameter.");
    }
  }

  @Override
  public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
    List<String> glossaryTermUrnStrs = params.getParams().get(VALUES_PARAM);
    List<Urn> glossaryTermUrns = glossaryTermUrnStrs.stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(glossaryTermUrns, entityTypeToUrns.getValue());
    }
  }

  private void applyInternal(List<Urn> tagUrns, List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.glossaryTermService.batchRemoveGlossaryTerms(tagUrns, urns.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList()), Constants.METADATA_TESTS_SOURCE);
    }
  }
}