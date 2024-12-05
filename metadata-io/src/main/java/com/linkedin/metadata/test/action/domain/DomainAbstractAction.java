package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.test.action.ActionUtils.getEntityTypeToUrns;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DomainServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class DomainAbstractAction extends UrnValuesAction {

  protected final DomainServiceAsync domainService;

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // For each entity type, group then apply the action.
    final List<String> domainUrnStrs = params.getParams().get(VALUES_PARAM);
    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrn : entityTypesToUrns.entrySet()) {
      List<Urn> entityUrns = entityTypeToUrn.getValue();
      Urn domainUrn = UrnUtils.getUrn(domainUrnStrs.get(0));
      if (entityUrns.isEmpty()) {
        continue;
      }
      applyInternal(opContext, domainUrn, entityUrns);
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(DOMAIN_ENTITY_NAME);
  }

  abstract void applyInternal(@Nonnull OperationContext opContext, Urn domainUrn, List<Urn> urns);
}
