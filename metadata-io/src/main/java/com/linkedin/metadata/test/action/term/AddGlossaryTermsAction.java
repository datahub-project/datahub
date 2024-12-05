package com.linkedin.metadata.test.action.term;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.GlossaryTermServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddGlossaryTermsAction extends UrnValuesAction {

  private final GlossaryTermServiceAsync glossaryTermService;

  @Override
  public ActionType getActionType() {
    return ActionType.ADD_GLOSSARY_TERMS;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    List<String> glossaryTermUrnStrs = params.getParams().get(VALUES_PARAM);
    List<Urn> glossaryTermUrns =
        glossaryTermUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(opContext, glossaryTermUrns, entityTypeToUrns.getValue());
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(GLOSSARY_TERM_ENTITY_NAME);
  }

  private void applyInternal(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.glossaryTermService.batchAddGlossaryTerms(
          opContext, tagUrns, getResourceReferences(urns), METADATA_TESTS_SOURCE, null);
    }
  }
}
