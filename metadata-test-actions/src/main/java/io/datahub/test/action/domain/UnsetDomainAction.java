package io.datahub.test.action.domain;

import com.linkedin.common.urn.Urn;
import io.datahub.test.action.Action;
import io.datahub.test.action.ActionParameters;
import io.datahub.test.definition.value.ActionType;
import io.datahub.test.exception.InvalidActionParamsException;
import io.datahub.test.exception.InvalidOperandException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class UnsetDomainAction implements Action {

  private final DomainService domainService;

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_DOMAIN;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
  }

  @Override
  public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
    // For each entity type, group then apply the action.
    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(entityTypeToUrns.getValue());
    }
  }

  private void applyInternal(List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.domainService.batchUnsetDomain(urns.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList()), Constants.METADATA_TESTS_SOURCE);
    }
  }
}